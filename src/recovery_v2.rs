use crate::content::Bytes;
use crate::id::{Lsn, PageId, TxId};
use crate::log::{TxState, WalEntry, WalKind};
use crate::pager_v2::{DbState, LogContext, PageOps, PageWriteOps, Pager};
use crate::wal::Wal;
use anyhow::anyhow;
use std::path::Path;

pub(crate) struct RecoveryResult {
    pub(crate) wal: Wal,
    pub(crate) next_txid: TxId,
}

pub(crate) fn recover(path: &Path, pager: &Pager) -> anyhow::Result<RecoveryResult> {
    let mut analyzer = Analyzer::new();
    let mut redoer = Redoer::new(pager);

    log::debug!("aries_recover start");
    let wal = crate::wal::recover(path, |lsn, entry| {
        log::debug!("aries_recover {lsn:?} entry={entry:?}");
        analyzer.analyze(lsn, &entry);
        redoer.redo(lsn, &entry)?;
        Ok(())
    })?;
    log::debug!("aries_recover analyze and redo finish");

    let analyze_result = analyzer.take_result();

    undo(pager, &wal, &analyze_result)?;

    let next_txid = if let Some(txid) = analyze_result.last_txid {
        txid.next()
    } else {
        TxId::new(1).unwrap()
    };

    Ok(RecoveryResult { wal, next_txid })
}

struct Analyzer {
    tx_state: TxState,
    last_txn: Option<TxId>,
}

impl Analyzer {
    fn new() -> Self {
        Self {
            tx_state: TxState::None,
            last_txn: None,
        }
    }

    fn analyze(&mut self, lsn: Lsn, entry: &WalEntry) {
        if let Some(clr) = entry.clr {
            let TxState::Aborting {
                ref mut last_undone,
                ..
            } = self.tx_state
            else {
                panic!(
                    "when found a clr entry, we should be in the middle of aborting a transaction"
                );
            };
            *last_undone = clr;
        }

        match entry.kind {
            WalKind::Begin { txid } => {
                assert_eq!(
                    TxState::None,
                    self.tx_state,
                    "when a transaction begin, there should be no active tx previously, but got {:?}", self.tx_state
                );
                assert!(
                    self.last_txn < Some(txid),
                    "last txn is {:?}, but newer txid is {txid:?}",
                    self.last_txn,
                );
                self.tx_state = TxState::Active(txid);
                self.last_txn = Some(txid);
            }

            WalKind::Commit { txid } => {
                assert_eq!(TxState::Active(txid), self.tx_state, "when a transaction committed, there should be exactly one active tx previously");
                self.tx_state = TxState::Committing(txid);
            }

            WalKind::Rollback { txid } => {
                assert_eq!(
                    TxState::Active(txid),
                    self.tx_state,
                    "when a transaction aborted, there should be exactly one active tx previously"
                );
                self.tx_state = TxState::Aborting {
                    txid,
                    last_undone: lsn,
                };
            }

            WalKind::End { txid } => {
                assert_eq!(
                    Some(txid),
                    self.last_txn,
                    "when a transaction ended, the last transaction should have the same id. last_txn={:?} txid={txid:?}",
                    self.last_txn,
                );
                self.tx_state = TxState::None;
            }

            WalKind::Checkpoint { active_tx, .. } => {
                self.last_txn = match active_tx {
                    TxState::Active(txid)
                    | TxState::Aborting { txid, .. }
                    | TxState::Committing(txid) => Some(txid),
                    _ => None,
                };
                self.tx_state = active_tx;
            }

            WalKind::InteriorReset { .. }
            | WalKind::InteriorResetForUndo { .. }
            | WalKind::InteriorSet { .. }
            | WalKind::InteriorInit { .. }
            | WalKind::InteriorInsert { .. }
            | WalKind::InteriorDelete { .. }
            | WalKind::InteriorDeleteForUndo { .. }
            | WalKind::InteriorSetCellOverflow { .. }
            | WalKind::InteriorSetCellPtr { .. }
            | WalKind::InteriorSetLast { .. }
            | WalKind::LeafReset { .. }
            | WalKind::LeafResetForUndo { .. }
            | WalKind::LeafSet { .. }
            | WalKind::LeafInit { .. }
            | WalKind::LeafInsert { .. }
            | WalKind::LeafDelete { .. }
            | WalKind::LeafDeleteForUndo { .. }
            | WalKind::LeafSetOverflow { .. }
            | WalKind::LeafSetNext { .. }
            | WalKind::OverflowReset { .. }
            | WalKind::OverflowResetForUndo { .. }
            | WalKind::OverflowInit { .. }
            | WalKind::OverflowSetContent { .. }
            | WalKind::OverflowSetContentForUndo { .. }
            | WalKind::OverflowSetNext { .. }
            | WalKind::HeaderSet { .. }
            | WalKind::HeaderSetForUndo { .. }
            | WalKind::AllocPage { .. }
            | WalKind::DeallocPage { .. } => (),
        }
    }

    fn take_result(self) -> AnalyzeResult {
        AnalyzeResult {
            active_tx: self.tx_state,
            last_txid: self.last_txn,
        }
    }
}

#[derive(Debug)]
struct AnalyzeResult {
    active_tx: TxState,
    last_txid: Option<TxId>,
}

struct Redoer<'a> {
    pager: &'a Pager,
}

impl<'a> Redoer<'a> {
    fn new(pager: &'a Pager) -> Self {
        Self { pager }
    }

    fn redo(&mut self, lsn: Lsn, entry: &WalEntry) -> anyhow::Result<()> {
        match entry.kind {
            WalKind::Begin { .. }
            | WalKind::Commit { .. }
            | WalKind::Rollback { .. }
            | WalKind::End { .. } => (),

            WalKind::Checkpoint {
                root,
                freelist,
                page_count,
                ..
            }
            | WalKind::HeaderSet {
                root,
                freelist,
                page_count,
                ..
            }
            | WalKind::HeaderSetForUndo {
                root,
                freelist,
                page_count,
                ..
            } => {
                self.pager.set_state(
                    LogContext::Redo(lsn),
                    DbState {
                        root,
                        freelist,
                        page_count,
                    },
                )?;
            }

            WalKind::AllocPage { txid, pgid } => {
                let page = self.pager.alloc(LogContext::Redo(lsn), txid)?;
                assert_eq!(page.id(), pgid);
            }
            WalKind::DeallocPage { txid, pgid } => {
                self.pager.dealloc(LogContext::Redo(lsn), txid, pgid)?;
            }

            WalKind::InteriorReset { txid, pgid, .. }
            | WalKind::InteriorResetForUndo { txid, pgid }
            | WalKind::InteriorSet { txid, pgid, .. }
            | WalKind::InteriorInit { txid, pgid, .. }
            | WalKind::InteriorInsert { txid, pgid, .. }
            | WalKind::InteriorDelete { txid, pgid, .. }
            | WalKind::InteriorDeleteForUndo { txid, pgid, .. }
            | WalKind::InteriorSetCellOverflow { txid, pgid, .. }
            | WalKind::InteriorSetCellPtr { txid, pgid, .. }
            | WalKind::InteriorSetLast { txid, pgid, .. }
            | WalKind::LeafReset { txid, pgid, .. }
            | WalKind::LeafResetForUndo { txid, pgid, .. }
            | WalKind::LeafSet { txid, pgid, .. }
            | WalKind::LeafInit { txid, pgid, .. }
            | WalKind::LeafInsert { txid, pgid, .. }
            | WalKind::LeafDelete { txid, pgid, .. }
            | WalKind::LeafDeleteForUndo { txid, pgid, .. }
            | WalKind::LeafSetOverflow { txid, pgid, .. }
            | WalKind::LeafSetNext { txid, pgid, .. }
            | WalKind::OverflowReset { txid, pgid, .. }
            | WalKind::OverflowResetForUndo { txid, pgid, .. }
            | WalKind::OverflowInit { txid, pgid, .. }
            | WalKind::OverflowSetContent { txid, pgid, .. }
            | WalKind::OverflowSetContentForUndo { txid, pgid, .. }
            | WalKind::OverflowSetNext { txid, pgid, .. } => {
                self.redo_page(lsn, entry, txid, pgid)?;
            }
        }

        Ok(())
    }

    fn redo_page(
        &mut self,
        lsn: Lsn,
        entry: &WalEntry,
        txid: TxId,
        pgid: PageId,
    ) -> anyhow::Result<()> {
        let page = self.pager.write(txid, pgid)?;
        if page.lsn() >= lsn {
            log::debug!(
                "redo skipped because page_lsn={:?} >= {lsn:?}",
                page.page_lsn()
            );
            return Ok(());
        }

        let ctx = LogContext::Redo(lsn);

        match entry.kind {
            WalKind::Begin { .. }
            | WalKind::Commit { .. }
            | WalKind::Rollback { .. }
            | WalKind::End { .. }
            | WalKind::Checkpoint { .. }
            | WalKind::HeaderSet { .. }
            | WalKind::HeaderSetForUndo { .. }
            | WalKind::AllocPage { .. }
            | WalKind::DeallocPage { .. } => {
                unreachable!("this case should be filtered out by the caller")
            }

            WalKind::InteriorReset { .. } | WalKind::InteriorResetForUndo { .. } => {
                let Some(page) = page.into_write_interior() else {
                    return Err(anyhow!(
                        "redo failed on interior reset because page {pgid:?} is not an interior"
                    ));
                };
                page.reset(ctx)?;
            }
            WalKind::InteriorSet { payload, .. } => {
                page.set_interior(ctx, payload.slice())?;
            }
            WalKind::InteriorInit { last, .. } => {
                if page.init_interior(ctx, last)?.is_none() {
                    return Err(anyhow!("redo failed on interior init on page {pgid:?}"));
                }
            }
            WalKind::InteriorInsert {
                index,
                mut raw,
                ptr,
                key_size,
                overflow,
                ..
            } => {
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!(
                        "redo failed on interior insert because page {pgid:?} is not an interior"
                    ));
                };
                let ok = page.insert_content(ctx, index, &mut raw, key_size, ptr, overflow)?;
                if !ok {
                    return Err(anyhow!(
                    "redo failed on interior insert because the content can't be inserted into page {pgid:?}"
                ));
                }
            }
            WalKind::InteriorDelete { index, .. }
            | WalKind::InteriorDeleteForUndo { index, .. } => {
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!(
                        "redo failed on interior delete because page {pgid:?} is not an interior"
                    ));
                };
                page.delete(ctx, index)?;
            }
            WalKind::InteriorSetCellOverflow {
                index, overflow, ..
            } => {
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!(
                    "redo failed on interior set overflow because page {pgid:?} is not an interior"
                ));
                };
                page.set_cell_overflow(ctx, index, overflow)?;
            }
            WalKind::InteriorSetCellPtr { index, ptr, .. } => {
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!(
                        "redo failed on interior set ptr because page {pgid:?} is not an interior"
                    ));
                };
                page.set_cell_ptr(ctx, index, ptr)?;
            }
            WalKind::InteriorSetLast { last, .. } => {
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!(
                        "redo failed on interior set ptr because page {pgid:?} is not an interior"
                    ));
                };
                page.set_last(ctx, last)?;
            }

            WalKind::LeafReset { .. } | WalKind::LeafResetForUndo { .. } => {
                let Some(page) = page.into_write_leaf() else {
                    return Err(anyhow!(
                        "redo failed on leaf reset because page {pgid:?} is not a leaf"
                    ));
                };
                page.reset(ctx)?;
            }
            WalKind::LeafSet { payload, .. } => {
                page.set_leaf(ctx, payload.slice())?;
            }
            WalKind::LeafInit { .. } => {
                page.init_leaf(ctx)?;
            }
            WalKind::LeafInsert {
                index,
                mut raw,
                overflow,
                key_size,
                value_size,
                ..
            } => {
                let Some(mut page) = page.into_write_leaf() else {
                    return Err(anyhow!(
                        "redo failed on leaf insert because page {pgid:?} is not a leaf"
                    ));
                };
                let ok =
                    page.insert_content(ctx, index, &mut raw, key_size, value_size, overflow)?;
                if !ok {
                    return Err(anyhow!(
                    "redo failed on leaf insert because the content can't be inserted into page {pgid:?}"
                ));
                }
            }
            WalKind::LeafDelete { index, .. } | WalKind::LeafDeleteForUndo { index, .. } => {
                let Some(mut page) = page.into_write_leaf() else {
                    return Err(anyhow!(
                        "redo failed on leaf delete because page {pgid:?} is not a leaf"
                    ));
                };
                page.delete(ctx, index)?;
            }
            WalKind::LeafSetOverflow {
                index, overflow, ..
            } => {
                let Some(mut page) = page.into_write_leaf() else {
                    return Err(anyhow!(
                        "redo failed on leaf set overflow because page {pgid:?} is not a leaf",
                    ));
                };
                page.set_cell_overflow(ctx, index, overflow)?;
            }
            WalKind::LeafSetNext { next, .. } => {
                let Some(mut page) = page.into_write_leaf() else {
                    return Err(anyhow!(
                        "redo failed on leaf set overflow because page {pgid:?} is not a leaf",
                    ));
                };
                page.set_next(ctx, next)?;
            }

            WalKind::OverflowReset { .. } | WalKind::OverflowResetForUndo { .. } => {
                let Some(page) = page.into_write_overflow() else {
                    return Err(anyhow!(
                        "redo failed on overflow reset because page {pgid:?} is not an overflow"
                    ));
                };
                page.reset(ctx)?;
            }
            WalKind::OverflowInit { .. } => {
                if page.init_overflow(ctx)?.is_none() {
                    return Err(anyhow!("redo failed on overflow init"));
                };
            }
            WalKind::OverflowSetContent { next, mut raw, .. } => {
                let Some(mut page) = page.into_write_overflow() else {
                    return Err(anyhow!(
                        "redo failed on overflow reset because page {pgid:?} is not an overflow"
                    ));
                };
                page.set_content(ctx, &mut raw, next)?;
            }
            WalKind::OverflowSetContentForUndo { .. } => {
                let Some(mut page) = page.into_write_overflow() else {
                    return Err(anyhow!(
                        "redo failed on overflow reset because page {pgid:?} is not an overflow"
                    ));
                };
                page.unset_content(ctx)?;
            }
            WalKind::OverflowSetNext { next, .. } => {
                let Some(mut page) = page.into_write_overflow() else {
                    return Err(anyhow!(
                        "redo failed on overflow reset because page {pgid:?} is not an overflow"
                    ));
                };
                page.set_next(ctx, next)?;
            }
        }

        Ok(())
    }
}

fn undo(pager: &Pager, wal: &Wal, analyze_result: &AnalyzeResult) -> anyhow::Result<()> {
    log::debug!("undo_started analyze_result={analyze_result:?}");

    let mut active_tx = analyze_result.active_tx;
    match &mut active_tx {
        TxState::None => {}

        TxState::Active(txid) => {
            let lsn = wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::Rollback { txid: *txid },
            })?;
            let mut last_undone = lsn;
            undo_txn(pager, wal, *txid, &mut last_undone)?;
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::End { txid: *txid },
            })?;
        }

        TxState::Committing(txid) => {
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::End { txid: *txid },
            })?;
        }

        TxState::Aborting {
            txid,
            ref mut last_undone,
        } => {
            undo_txn(pager, wal, *txid, last_undone)?;
        }
    }

    log::debug!("undo_finished");

    Ok(())
}

pub(crate) fn undo_txn(
    pager: &Pager,
    wal: &Wal,
    txid: TxId,
    last_undone: &mut Lsn,
) -> anyhow::Result<()> {
    log::debug!("undo_txn {txid:?} last_undone={last_undone:?}");

    let mut last_clr = None;
    wal.iter_back(*last_undone, |lsn, entry| {
        log::debug!("undo_item {lsn:?} {entry:?}");
        assert!(lsn < *last_undone);

        if let Some(clr) = entry.clr {
            last_clr = last_clr.map(|last_clr| std::cmp::min(last_clr, clr));
            log::debug!("undo_item skipped since clr={:?}", entry.clr);
            return Ok(false);
        }
        if let Some(last_clr) = last_clr {
            if lsn >= last_clr {
                log::debug!("undo_item skipped since lsn >= last_clr. lsn={lsn:?} last_clr={last_clr:?}");
                // means this log entry is already undone and written to the WAL, so
                // we shouldn't undo it again
                return Ok(false);
            }
        }

        let ctx = LogContext::Undo(wal, lsn);

        match entry.kind {
            WalKind::Begin { txid: log_txid } => {
                assert_eq!(txid, log_txid);
                return Ok(true);
            }
            WalKind::Commit { txid } => {
                return Err(anyhow!(
                    "found a commit log during transaction rollback {txid:?}"
                ))
            }
            WalKind::Rollback { .. } => (),
            WalKind::End { .. } => (),

            WalKind::HeaderSet {
                old_root,
                old_freelist,
                old_page_count,
                ..
            } => {
                pager.set_state(
                    ctx,
                    DbState {
                        root: old_root,
                        freelist: old_freelist,
                        page_count: old_page_count,
                    },
                )?;
            }
            WalKind::HeaderSetForUndo { .. } => {
                unreachable!("HeaderUndoSet only used for CLR which shouldn't be undone");
            }

            WalKind::AllocPage { txid, pgid } => {
                pager.dealloc(ctx, txid, pgid)?;
            }
            WalKind::DeallocPage { .. } => {
                unreachable!("DeallocPage only used for CLR which shouldn't be undone");
            }

            WalKind::InteriorReset {
                txid,
                pgid,
                page_version,
                payload,
            } => {
                if page_version != 0 {
                    return Err(anyhow!("page version {page_version} is not supported"));
                }
                let page = pager.write(txid, pgid)?;
                page.set_interior(ctx, payload.slice())?;
            }
            WalKind::InteriorResetForUndo { .. } => {
                unreachable!("InteriorUndoReset only used for CLR which shouldn't be undone");
            }
            WalKind::InteriorSet {
                pgid, page_version, ..
            } => {
                if page_version != 0 {
                    return Err(anyhow!("page version {page_version} is not supported"));
                }
                let page = pager.write(txid, pgid)?;
                let Some(page) = page.into_write_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.reset(ctx)?;
            }
            WalKind::InteriorInit { pgid, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(page) = page.into_write_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.reset(ctx)?;
            }
            WalKind::InteriorInsert { pgid, index, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.delete(ctx, index)?;
            }
            WalKind::InteriorDelete {
                txid,
                pgid,
                index,
                mut old_raw,
                old_ptr,
                old_overflow,
                old_key_size,
            } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                let ok = page.insert_content(
                    ctx,
                    index,
                    &mut old_raw,
                    old_key_size,
                    old_ptr,
                    old_overflow,
                )?;
                assert!(ok, "if it can be deleted, then it must be ok to insert, pgid={pgid:?} index={index} old_raw_len={}", old_raw.len());
            }
            WalKind::InteriorDeleteForUndo { .. } => {
                unreachable!("InteriorUndoDelete only used for CLR which shouldn't be undone");
            }
            WalKind::InteriorSetCellOverflow {
                pgid,
                index,
                old_overflow,
                ..
            } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.set_cell_overflow(ctx, index, old_overflow)?;
            }
            WalKind::InteriorSetCellPtr {
                pgid,
                index,
                old_ptr,
                ..
            } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.set_cell_ptr(ctx, index, old_ptr)?;
            }
            WalKind::InteriorSetLast { pgid, old_last, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_interior() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.set_last(ctx, old_last)?;
            }

            WalKind::LeafReset {
                txid,
                pgid,
                page_version,
                payload,
            } => {
                if page_version != 0 {
                    return Err(anyhow!("page version {page_version} is not supported"));
                }
                let page = pager.write(txid, pgid)?;
                page.set_leaf(ctx, payload.slice())?;
            }
            WalKind::LeafResetForUndo { .. } => {
                unreachable!("LeafUndoReset only used for CLR which shouldn't be undone");
            }
            WalKind::LeafSet {
                pgid, page_version, ..
            } => {
                if page_version != 0 {
                    return Err(anyhow!("page version {page_version} is not supported"));
                }
                let page = pager.write(txid, pgid)?;
                let Some(page) = page.into_write_leaf() else {
                    return Err(anyhow!("expected an interior page for undo"));
                };
                page.reset(ctx)?;
            }
            WalKind::LeafInit { txid, pgid } => {
                let page = pager.write(txid, pgid)?;
                let Some(page) = page.into_write_leaf() else {
                    return Err(anyhow!("expected a leaf page for undo {pgid:?}"));
                };
                page.reset(ctx)?;
            }
            WalKind::LeafInsert { pgid, index, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_leaf() else {
                    return Err(anyhow!("expected a leaf page for undo {pgid:?}"));
                };
                page.delete(ctx, index)?;
            }
            WalKind::LeafDelete {
                txid,
                pgid,
                index,
                old_raw,
                old_overflow,
                old_key_size,
                old_val_size,
            } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_leaf() else {
                    return Err(anyhow!("expected a leaf page for undo {pgid:?}"));
                };
                let ok = page.insert_content(
                    ctx,
                    index,
                    &mut Bytes::new(old_raw.slice()),
                    old_key_size,
                    old_val_size,
                    old_overflow,
                )?;
                assert!(ok, "if it can be deleted, then it must be ok to insert, pgid={pgid:?} index={index} old_raw_len={}", old_raw.len());
            }
            WalKind::LeafDeleteForUndo { .. } => {
                unreachable!("LeafUndoDelete only used for CLR which shouldn't be undone");
            }
            WalKind::LeafSetOverflow {
                pgid,
                index,
                old_overflow,
                ..
            } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_leaf() else {
                    return Err(anyhow!("expected a leaf page for undo {pgid:?}"));
                };
                page.set_cell_overflow(ctx, index, old_overflow)?;
            }
            WalKind::LeafSetNext { pgid, old_next, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_leaf() else {
                    return Err(anyhow!("expected a leaf page for undo {pgid:?}"));
                };
                page.set_next(ctx, old_next)?;
            }

            WalKind::OverflowReset {
                txid,
                pgid,
                page_version,
                payload,
            } => {
                if page_version != 0 {
                    return Err(anyhow!("page version {page_version} is not supported"));
                }
                let page = pager.write(txid, pgid)?;
                page.set_overflow(ctx, payload.slice())?;
            }
            WalKind::OverflowResetForUndo { .. } => {
                unreachable!("OverflowUndoReset only used for CLR which shouldn't be undone");
            }
            WalKind::OverflowInit { txid, pgid } => {
                let page = pager.write(txid, pgid)?;
                let Some(page) = page.into_write_overflow() else {
                    return Err(anyhow!("expected a overflow page for undo"));
                };
                page.reset(ctx)?;
            }
            WalKind::OverflowSetContent { pgid, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_overflow() else {
                    return Err(anyhow!("expected a overflow page for undo"));
                };
                page.unset_content(ctx)?;
            }
            WalKind::OverflowSetContentForUndo { .. } => {
                unreachable!("OverflowUndoSetContent only used for CLR which shouldn't be undone");
            }
            WalKind::OverflowSetNext { pgid, old_next, .. } => {
                let page = pager.write(txid, pgid)?;
                let Some(mut page) = page.into_write_overflow() else {
                    return Err(anyhow!("expected a overflow page for undo"));
                };
                page.set_next(ctx, old_next)?;
            }

            WalKind::Checkpoint { .. } => (),
        }

        Ok(false)
    })?;

    wal.append_log(WalEntry {
        clr: None,
        kind: WalKind::End { txid },
    })?;
    log::debug!("undo_txn_finished");

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_recovering_failing_abort() -> anyhow::Result<()> {
        // TODO:
        // 1. Create a transaction
        // 2. Do some operations that triggers the wal
        // 3. Abort it
        // 4. Wait until some of the logs are undone
        // 5. But, don't undo all of them
        // 6. Fail in the middle, but make sure some of the undo logs are written.
        // 7. Recover
        Ok(())
    }
}
