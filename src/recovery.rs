use crate::content::Bytes;
use crate::pager::{DbState, LogContext, PageId, PageIdExt, PageWrite, Pager};
use crate::wal::{
    build_wal_header, load_wal_header, Lsn, LsnExt, TxId, TxState, Wal, WalDecodeResult, WalEntry,
    WalHeader, WalRecord, WAL_HEADER_SIZE,
};
use anyhow::anyhow;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::os::unix::fs::MetadataExt;
use std::slice::SliceIndex;

pub(crate) fn recover(mut f: File, pager: &Pager, page_size: usize) -> anyhow::Result<Wal> {
    let wal_header = get_wal_header(&mut f, page_size)?;

    let analyze_result = analyze(&mut f, &wal_header, page_size)?;
    redo(&mut f, pager, &wal_header, &analyze_result, page_size)?;

    let wal = Wal::new(
        f,
        wal_header.relative_lsn_offset,
        analyze_result.next_lsn,
        page_size,
    )?;

    undo(&analyze_result, pager, &wal)?;

    Ok(wal)
}

fn get_wal_header(f: &mut File, page_size: usize) -> anyhow::Result<WalHeader> {
    if f.metadata()?.size() < 2 * WAL_HEADER_SIZE as u64 {
        build_wal_header(f)
    } else {
        load_wal_header(f)
    }
}

fn iterate_wal_forward(
    f: &mut File,
    relative_lsn_offset: usize,
    page_size: usize,
    lsn: Lsn,
) -> anyhow::Result<WalIterator> {
    let file_len = f.metadata()?.len();
    let f_offset = lsn.add(relative_lsn_offset).get();
    if f_offset >= file_len {
        f.seek(SeekFrom::Start(file_len))?;
    } else {
        f.seek(SeekFrom::Start(f_offset))?;
    }

    Ok(WalIterator {
        f,
        lsn,
        buffer: vec![0u8; 4 * page_size],
        start_offset: 0,
        end_offset: 0,
    })
}

struct WalIterator<'a> {
    f: &'a mut File,
    lsn: Lsn,
    buffer: Vec<u8>,
    start_offset: usize,
    end_offset: usize,
}

impl WalIterator<'_> {
    pub(crate) fn next(&mut self) -> anyhow::Result<Option<(Lsn, WalEntry)>> {
        loop {
            let buff = &self.buffer[self.start_offset..self.end_offset];
            // TODO: check whether this is safe.
            let buff = unsafe { std::mem::transmute::<&[u8], &[u8]>(buff) };

            let entry = WalEntry::decode(buff);
            match entry {
                WalDecodeResult::Ok(entry) => {
                    self.start_offset += entry.size();
                    self.lsn = self.lsn.add(entry.size());
                    return Ok(Some((self.lsn, entry)));
                }
                WalDecodeResult::NeedMoreBytes => {
                    let len = self.end_offset - self.start_offset;
                    for i in 0..len {
                        self.buffer[i] = self.buffer[self.start_offset + i];
                    }
                    self.start_offset = 0;
                    self.end_offset = len;

                    let n = self.f.read(&mut self.buffer[self.end_offset..])?;
                    if n == 0 {
                        return Ok(None);
                    }
                    self.end_offset += n;
                }
                WalDecodeResult::Incomplete => {
                    return Ok(None);
                }
                WalDecodeResult::Err(err) => return Err(err),
            }
        }
    }
}

struct AriesAnalyzeResult {
    lsn_to_redo: Lsn,
    dirty_pages: HashMap<PageId, Lsn>,
    active_tx: TxState,
    next_lsn: Lsn,
}

fn analyze(
    f: &mut File,
    wal_header: &WalHeader,
    page_size: usize,
) -> anyhow::Result<AriesAnalyzeResult> {
    // TODO: perform aries recovery here, and get the `next_lsn`.
    let analyze_start = wal_header
        .checkpoint
        .unwrap_or(Lsn::new(WAL_HEADER_SIZE as u64 * 2).unwrap());

    let mut iter = iterate_wal_forward(
        f,
        wal_header.relative_lsn_offset as usize,
        page_size,
        analyze_start,
    )?;

    let mut next_lsn = wal_header.checkpoint;
    let mut tx_state = TxState::None;
    let mut last_txn: Option<TxId> = None;
    let mut dirty_pages = HashMap::default();

    log::debug!("aries analysis started wal_header={wal_header:?}");

    // TODO: change assert into return error
    while let Some((lsn, entry)) = iter.next()? {
        log::debug!("recovery item lsn={lsn:?} entry={entry:?}");

        match entry.record {
            WalRecord::Begin => {
                assert_eq!(
                    TxState::None,
                    tx_state,
                    "when a transaction begin, there should be no active tx previously"
                );
                assert!(last_txn.map(TxId::get).unwrap_or(0) < entry.txid.get());
                tx_state = TxState::Active(entry.txid);
                last_txn = Some(entry.txid);
            }
            WalRecord::Commit => {
                assert_eq!(TxState::Active(entry.txid), tx_state, "when a transaction committed, there should be exactly one active tx previously");
                tx_state = TxState::Committing(entry.txid);
            }
            WalRecord::Rollback => {
                assert_eq!(
                    TxState::Active(entry.txid),
                    tx_state,
                    "when a transaction aborted, there should be exactly one active tx previously"
                );
                tx_state = TxState::Aborting {
                    txid: entry.txid,
                    rollback: lsn,
                    last_undone: lsn,
                };
            }
            WalRecord::End => {
                assert!(
                    TxState::Committing(entry.txid) == tx_state ||
                    if let TxState::Aborting{txid, ..} = tx_state {
                        txid == entry.txid
                    } else { false },
                    "when a transaction ended, there should be exactly one committing or aborting tx previously",
                );
                tx_state = TxState::None;
            }

            WalRecord::CheckpointBegin { active_tx, .. } => {
                tx_state = active_tx;
            }

            WalRecord::InteriorInit { pgid, .. }
            | WalRecord::InteriorInsert { pgid, .. }
            | WalRecord::InteriorDelete { pgid, .. }
            | WalRecord::LeafInit { pgid, .. }
            | WalRecord::LeafInsert { pgid, .. } => {
                dirty_pages.entry(pgid).or_insert(lsn);
            }

            _ => (),
        }

        next_lsn = Some(lsn);
    }

    log::debug!("aries analysis finished next_lsn={next_lsn:?} dirty_pages={dirty_pages:?} tx_state={tx_state:?}");

    let mut min_rec_lsn = next_lsn;
    for rec_lsn in dirty_pages.values() {
        if let Some(min_lsn) = min_rec_lsn {
            min_rec_lsn = Some(std::cmp::min(min_lsn, *rec_lsn));
        } else {
            min_rec_lsn = Some(*rec_lsn);
        }
    }

    let next_lsn = next_lsn.unwrap_or(Lsn::new(WAL_HEADER_SIZE as u64 * 2).unwrap());

    Ok(AriesAnalyzeResult {
        lsn_to_redo: Lsn::new(wal_header.relative_lsn_offset + 2 * WAL_HEADER_SIZE as u64).unwrap(),
        dirty_pages,
        active_tx: tx_state,
        next_lsn,
    })
}

fn redo(
    f: &mut File,
    pager: &Pager,
    wal_header: &WalHeader,
    analyze_result: &AriesAnalyzeResult,
    page_size: usize,
) -> anyhow::Result<()> {
    let mut iter = iterate_wal_forward(
        f,
        wal_header.relative_lsn_offset as usize,
        page_size,
        analyze_result.lsn_to_redo,
    )?;

    log::debug!("aries redo started");

    while let Some((lsn, entry)) = iter.next()? {
        log::debug!("aries redo item lsn={lsn:?} entry={entry:?}");
        match entry.record {
            WalRecord::Begin | WalRecord::Commit | WalRecord::Rollback | WalRecord::End => (),

            WalRecord::CheckpointBegin { root, freelist, .. }
            | WalRecord::HeaderSet { root, freelist, .. }
            | WalRecord::HeaderUndoSet { root, freelist, .. } => {
                pager.set_db_state(DbState { root, freelist });
            }

            WalRecord::InteriorReset { pgid, .. }
            | WalRecord::InteriorUndoReset { pgid }
            | WalRecord::InteriorInit { pgid, .. }
            | WalRecord::InteriorInsert { pgid, .. }
            | WalRecord::InteriorDelete { pgid, .. }
            | WalRecord::InteriorUndoDelete { pgid, .. }
            | WalRecord::LeafReset { pgid, .. }
            | WalRecord::LeafUndoReset { pgid }
            | WalRecord::LeafInit { pgid, .. }
            | WalRecord::LeafInsert { pgid, .. }
            | WalRecord::LeafDelete { pgid, .. }
            | WalRecord::LeafUndoDelete { pgid, .. }
            | WalRecord::LeafSetOverflow { pgid, .. }
            | WalRecord::OverflowReset { pgid, .. }
            | WalRecord::OverflowUndoReset { pgid }
            | WalRecord::OverflowInit { pgid, .. }
            | WalRecord::OverflowInsert { pgid, .. } => {
                redo_page(pager, analyze_result, lsn, &entry, pgid)?;
            }
        };
    }

    log::debug!("aries redo finished");

    Ok(())
}

fn redo_page(
    pager: &Pager,
    analyze_result: &AriesAnalyzeResult,
    lsn: Lsn,
    entry: &WalEntry,
    pgid: PageId,
) -> anyhow::Result<()> {
    let Some(rec_lsn) = analyze_result.dirty_pages.get(&pgid) else {
        return Ok(());
    };
    if &lsn < rec_lsn {
        return Ok(());
    }

    let page = pager.write(entry.txid, pgid)?;
    if let Some(page_lsn) = page.page_lsn() {
        if page_lsn >= lsn {
            return Ok(());
        }
    }

    let ctx = LogContext::Redo(lsn);

    match entry.record {
        WalRecord::Begin
        | WalRecord::Commit
        | WalRecord::Rollback
        | WalRecord::End
        | WalRecord::CheckpointBegin { .. }
        | WalRecord::HeaderSet { .. }
        | WalRecord::HeaderUndoSet { .. } => {
            unreachable!("this case should be filtered out by the caller")
        }

        WalRecord::InteriorReset { pgid, .. } | WalRecord::InteriorUndoReset { pgid } => {
            let Some(mut page) = page.into_interior() else {
                return Err(anyhow!(
                    "redo failed on interior reset because page is not an interior"
                ));
            };
            page.reset(ctx)?;
        }
        WalRecord::InteriorInit { pgid, last } => {
            if page.init_interior(ctx, last)?.is_none() {
                return Err(anyhow!("redo failed on interior init"));
            }
        }
        WalRecord::InteriorInsert {
            index,
            raw,
            ptr,
            key_size,
            overflow,
            ..
        } => {
            let Some(mut page) = page.into_interior() else {
                return Err(anyhow!(
                    "redo failed on interior insert because page is not an interior"
                ));
            };
            let ok =
                page.insert_content(ctx, index, &mut Bytes::new(raw), key_size, ptr, overflow)?;
            if !ok {
                return Err(anyhow!(
                    "redo failed on interior insert because the content can't be inserted"
                ));
            }
        }
        WalRecord::InteriorDelete { index, .. } | WalRecord::InteriorUndoDelete { index, .. } => {
            let Some(mut page) = page.into_interior() else {
                return Err(anyhow!(
                    "redo failed on interior delete because page is not an interior"
                ));
            };
            page.delete(ctx, index)?;
        }

        WalRecord::LeafReset { pgid, .. } | WalRecord::LeafUndoReset { pgid } => {
            let Some(mut page) = page.into_leaf() else {
                return Err(anyhow!(
                    "redo failed on leaf reset because page is not a leaf"
                ));
            };
            page.reset(ctx)?;
        }
        WalRecord::LeafInit { pgid } => {
            if page.init_leaf(ctx)?.is_none() {
                return Err(anyhow!("redo failed on leaf init"));
            };
        }
        WalRecord::LeafInsert {
            index,
            raw,
            overflow,
            key_size,
            value_size,
            ..
        } => {
            let Some(mut page) = page.into_leaf() else {
                return Err(anyhow!(
                    "redo failed on leaf insert because page is not a leaf"
                ));
            };
            let ok = page.insert_content(
                ctx,
                index,
                &mut Bytes::new(raw),
                key_size,
                value_size,
                overflow,
            )?;
            if !ok {
                return Err(anyhow!(
                    "redo failed on leaf insert because the content can't be inserted"
                ));
            }
        }
        WalRecord::LeafDelete { index, .. } | WalRecord::LeafUndoDelete { index, .. } => {
            let Some(mut page) = page.into_leaf() else {
                return Err(anyhow!(
                    "redo failed on leaf delete because page is not a leaf"
                ));
            };
            page.delete(ctx, index)?;
        }
        WalRecord::LeafSetOverflow {
            pgid,
            index,
            overflow,
            ..
        } => {
            let Some(mut page) = page.into_leaf() else {
                return Err(anyhow!(
                    "redo failed on leaf set overflow because page is not a leaf"
                ));
            };
            page.set_cell_overflow(ctx, index, overflow)?;
        }

        WalRecord::OverflowReset { pgid, .. } | WalRecord::OverflowUndoReset { pgid } => {
            let Some(mut page) = page.into_overflow() else {
                return Err(anyhow!(
                    "redo failed on overflow reset because page is not an overflow"
                ));
            };
            page.reset(ctx)?;
        }
        WalRecord::OverflowInit { pgid } => {
            if page.init_overflow(ctx)?.is_none() {
                return Err(anyhow!("redo failed on overflow init"));
            };
        }
        WalRecord::OverflowInsert { pgid, next, raw } => {
            let Some(mut page) = page.into_overflow() else {
                return Err(anyhow!(
                    "redo failed on overflow reset because page is not an overflow"
                ));
            };
            page.insert_content(ctx, &mut Bytes::new(raw), next)?;
        }
    }

    Ok(())
}

fn undo(analyze_result: &AriesAnalyzeResult, pager: &Pager, wal: &Wal) -> anyhow::Result<()> {
    log::debug!("aries undo started");

    let mut active_tx = analyze_result.active_tx;
    match &mut active_tx {
        TxState::None => {}

        TxState::Active(txid) => {
            let lsn = wal.append(*txid, None, WalRecord::Rollback)?;
            let mut last_undone = lsn;
            undo_txn(pager, wal, *txid, lsn, &mut last_undone)?;
            wal.append(*txid, None, WalRecord::End)?;
        }

        TxState::Committing(txid) => {
            wal.append(*txid, None, WalRecord::End)?;
        }

        TxState::Aborting {
            txid,
            rollback,
            ref mut last_undone,
        } => {
            undo_txn(pager, wal, *txid, *rollback, last_undone)?;
        }
    }

    log::debug!("aries undo finished");

    Ok(())
}

// TODO: maybe it's better to move this function somewhere else since undo is not only used for recovery
// but during runtime as well.
pub(crate) fn undo_txn(
    pager: &Pager,
    wal: &Wal,
    txid: TxId,
    lsn: Lsn,
    last_undone_clr: &mut Lsn,
) -> anyhow::Result<()> {
    log::debug!("undo txn started from={lsn:?} last_undone_clr={last_undone_clr:?}");

    let mut iterator = wal.iterate_back(lsn);
    let mut is_ended = false;

    let ctx = LogContext::Undo(wal, lsn);

    while let Some((lsn, entry)) = iterator.next()? {
        log::debug!("undo txn item lsn={lsn:?} entry={entry:?}");
        if entry.txid != txid {
            continue;
        }
        if lsn >= *last_undone_clr {
            continue;
        }
        assert!(
            entry.clr.is_none(),
            "when iterating back from rollback, there shouldn't be any CLR logs"
        );

        *last_undone_clr = lsn;
        match entry.record {
            WalRecord::Begin => break,
            WalRecord::Commit => {
                return Err(anyhow!("found a commit log during transaction rollback"))
            }
            WalRecord::Rollback => (),
            WalRecord::End => is_ended = true,

            WalRecord::HeaderSet {
                old_root,
                old_freelist,
                ..
            } => {
                pager.set_db_state(DbState {
                    root: old_root,
                    freelist: old_freelist,
                });
            }
            WalRecord::HeaderUndoSet { .. } => {
                unreachable!("HeaderUndoSet only used for CLR which shouldn't be undone");
            }

            WalRecord::InteriorReset {
                pgid,
                page_version,
                payload,
            } => {
                if page_version != 0 {
                    return Err(anyhow!("page version {page_version} is not supported"));
                }
                let page = pager.write(txid, pgid)?;
                page.set_interior(ctx, payload)?;
            }
            WalRecord::InteriorUndoReset { pgid } => {
                unreachable!("InteriorUndoReset only used for CLR which shouldn't be undone");
            }
            WalRecord::InteriorInit { pgid, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.reset(ctx)?;
            }
            WalRecord::InteriorInsert { pgid, index, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.delete(ctx, index)?;
            }
            WalRecord::InteriorDelete {
                pgid,
                index,
                old_raw,
                old_ptr,
                old_overflow,
                old_key_size,
            } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.insert_content(
                    ctx,
                    index,
                    &mut Bytes::new(old_raw),
                    old_key_size,
                    old_ptr,
                    old_overflow,
                )?;
            }
            WalRecord::InteriorUndoDelete { pgid, index } => {
                unreachable!("InteriorUndoDelete only used for CLR which shouldn't be undone");
            }

            WalRecord::LeafReset {
                pgid,
                page_version,
                payload,
            } => {
                if page_version != 0 {
                    return Err(anyhow!("page version {page_version} is not supported"));
                }
                let page = pager.write(txid, pgid)?;
                page.set_leaf(ctx, payload)?;
            }
            WalRecord::LeafUndoReset { pgid } => {
                unreachable!("LeafUndoReset only used for CLR which shouldn't be undone");
            }
            WalRecord::LeafInit { pgid } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_leaf() else {
                    return Err(anyhow!("expected a leaf page for undo"));
                };
                page.reset(ctx)?;
            }
            WalRecord::LeafInsert { pgid, index, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_leaf() else {
                    return Err(anyhow!("expected an leaf page for undo"));
                };
                page.delete(ctx, index)?;
            }
            WalRecord::LeafDelete {
                pgid,
                index,
                old_raw,
                old_overflow,
                old_key_size,
                old_val_size,
            } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_leaf() else {
                    return Err(anyhow!("expected a leaf page for undo"));
                };
                page.insert_content(
                    ctx,
                    index,
                    &mut Bytes::new(old_raw),
                    old_key_size,
                    old_val_size,
                    old_overflow,
                )?;
            }
            WalRecord::LeafUndoDelete { pgid, index } => {
                unreachable!("LeafUndoDelete only used for CLR which shouldn't be undone");
            }
            WalRecord::LeafSetOverflow {
                pgid,
                index,
                old_overflow,
                ..
            } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_leaf() else {
                    return Err(anyhow!("expected a leaf page for undo"));
                };
                page.set_cell_overflow(ctx, index, old_overflow)?;
            }

            WalRecord::OverflowReset {
                pgid,
                page_version,
                payload,
            } => {
                if page_version != 0 {
                    return Err(anyhow!("page version {page_version} is not supported"));
                }
                let page = pager.write(txid, pgid)?;
                page.set_overflow(ctx, payload)?;
            }
            WalRecord::OverflowUndoReset { pgid } => {
                unreachable!("OverflowUndoReset only used for CLR which shouldn't be undone");
            }
            WalRecord::OverflowInit { pgid } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_overflow() else {
                    return Err(anyhow!("expected a overflow page for undo"));
                };
                page.reset(ctx)?;
            }
            WalRecord::OverflowInsert { pgid, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_overflow() else {
                    return Err(anyhow!("expected a overflow page for undo"));
                };
                page.clear(ctx);
            }

            WalRecord::CheckpointBegin { .. } => (),
        }
    }

    if !is_ended {
        log::debug!("appending txn-end txid={txid:?}");
        wal.append(txid, None, WalRecord::End)?;
    }

    log::debug!("undo txn finished");
    Ok(())
}
