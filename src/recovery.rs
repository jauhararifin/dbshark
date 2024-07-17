use crate::content::Bytes;
use crate::pager::{PageId, PageIdExt, PageWrite, Pager};
use crate::wal::{Lsn, TxId, TxState, Wal, WalDecodeResult, WalEntry, WalRecord};
use anyhow::anyhow;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::os::unix::fs::MetadataExt;

pub(crate) fn recover(mut f: File, pager: &Pager, page_size: usize) -> anyhow::Result<Wal> {
    let wal_header = get_wal_header(&mut f, page_size)?;

    let analyze_result = analyze(&mut f, &wal_header, page_size)?;
    let redo_result = redo(&mut f, pager, &wal_header, &analyze_result, page_size)?;
    undo(&analyze_result)?;

    Wal::new(
        f,
        wal_header.relative_lsn_offset,
        analyze_result.next_lsn,
        page_size,
    )
}

fn get_wal_header(f: &mut File, page_size: usize) -> anyhow::Result<WalHeader> {
    if f.metadata()?.size() < 2 * WAL_HEADER_SIZE as u64 {
        build_wal_header(f)
    } else {
        load_wal_header(f)
    }
}

fn build_wal_header(f: &mut File) -> anyhow::Result<WalHeader> {
    let header = WalHeader {
        version: 0,
        relative_lsn_offset: 0,
        checkpoint: None,
    };

    f.seek(SeekFrom::Start(0))?;
    let mut header_buff = vec![0u8; 2 * WAL_HEADER_SIZE];
    header.encode(&mut header_buff[..WAL_HEADER_SIZE]);
    header.encode(&mut header_buff[WAL_HEADER_SIZE..]);
    f.write_all(&header_buff)?;

    Ok(header)
}

fn load_wal_header(f: &mut File) -> anyhow::Result<WalHeader> {
    f.seek(SeekFrom::Start(0))?;
    let mut header_buff = vec![0u8; 2 * WAL_HEADER_SIZE];
    f.read_exact(&mut header_buff)?;
    if header_buff[0..6].cmp(b"db_wal").is_ne() {
        return Err(anyhow!("the file is not a wal file"));
    }

    let wal_header = WalHeader::decode(&header_buff[..WAL_HEADER_SIZE])
        .or_else(|| WalHeader::decode(&header_buff[WAL_HEADER_SIZE..]))
        .ok_or_else(|| anyhow!("corrupted wal file"))?;

    if wal_header.version != 0 {
        return Err(anyhow!("unsupported WAL version: {}", wal_header.version));
    }

    Ok(wal_header)
}

const WAL_HEADER_SIZE: usize = 32;

struct WalHeader {
    version: u16,
    checkpoint: Option<Lsn>,
    relative_lsn_offset: u64,
}

impl WalHeader {
    fn decode(buff: &[u8]) -> Option<Self> {
        let version = u16::from_be_bytes(buff[6..8].try_into().unwrap());
        let relative_lsn_offset = u64::from_be_bytes(buff[8..16].try_into().unwrap());
        let checkpoint = Lsn::from_be_bytes(buff[16..24].try_into().unwrap());

        let stored_checksum = u64::from_be_bytes(buff[24..32].try_into().unwrap());
        let calculated_checksum = crc64::crc64(0, &buff[0..24]);
        if stored_checksum != calculated_checksum {
            return None;
        }

        Some(WalHeader {
            version,
            checkpoint,
            relative_lsn_offset,
        })
    }

    fn encode(&self, buff: &mut [u8]) {
        buff[0..6].copy_from_slice(b"db_wal");
        buff[6..8].copy_from_slice(&self.version.to_be_bytes());
        buff[8..16].copy_from_slice(&self.relative_lsn_offset.to_be_bytes());
        let checksum = crc64::crc64(0, &buff[0..16]);
        buff[16..24].copy_from_slice(&checksum.to_be_bytes());
    }
}

fn iterate_wal_forward(
    f: &mut File,
    relative_lsn_offset: usize,
    page_size: usize,
    lsn: Lsn,
) -> WalIterator {
    WalIterator {
        f,
        f_offset: lsn.add(relative_lsn_offset).get(),

        lsn,

        buffer: vec![0u8; 4 * page_size],
        start_offset: 0,
        end_offset: 0,
    }
}

struct WalIterator<'a> {
    f: &'a mut File,
    f_offset: u64,

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

                    if self.f_offset >= self.f.metadata()?.size() {
                        return Ok(None);
                    }
                    self.f.seek(SeekFrom::Start(self.f_offset))?;
                    let n = self.f.read(&mut self.buffer[self.end_offset..])?;
                    self.f_offset += n as u64;
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
    );

    let mut next_lsn = wal_header.checkpoint;
    let mut tx_state = TxState::None;
    let mut checkpoint_begin_found = false;
    let mut checkpoint_end_found = false;
    let mut last_txn: Option<TxId> = None;
    let mut dirty_pages = HashMap::default();

    // TODO: change assert into return error
    while let Some((lsn, entry)) = iter.next()? {
        match entry.record {
            WalRecord::Begin => {
                assert_eq!(
                    TxState::None,
                    tx_state,
                    "when a transaction begin, there should be no active tx previously"
                );
                assert!(last_txn.map(TxId::get).unwrap_or(0) < entry.txid.get());
                checkpoint_begin_found = true;
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
                tx_state = TxState::Aborting(entry.txid);
            }
            WalRecord::End => {
                assert!(TxState::Committing(entry.txid) == tx_state || TxState::Aborting(entry.txid) == tx_state, "when a transaction ended, there should be exactly one committing or aborting tx previously");
                tx_state = TxState::None;
            }

            WalRecord::CheckpointBegin => (),
            WalRecord::CheckpointEnd {
                active_tx,
                dirty_pages: dp,
            } => {
                if checkpoint_end_found {
                    continue;
                }
                checkpoint_end_found = true;

                match active_tx {
                    TxState::None => (),
                    TxState::Committing(txid) | TxState::Active(txid) | TxState::Aborting(txid) => {
                        if let Some(last_txid) = last_txn {
                            assert!(txid.get() <= last_txid.get());
                        } else {
                            assert_eq!(TxState::None, tx_state);
                            tx_state = active_tx;
                        }
                    }
                }

                for dirty_page in dp {
                    dirty_pages.insert(dirty_page.id, dirty_page.rec_lsn);
                }
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

    if checkpoint_begin_found && !checkpoint_end_found {
        return Err(anyhow!(
            "wal file is corrupted, checkpoint begin found but checkpoint end not found"
        ));
    }

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

struct RedoResult {
    db_state: Option<DbState>,
}

struct DbState {
    root: Option<PageId>,
    freelist: Option<PageId>,
}

fn redo(
    f: &mut File,
    pager: &Pager,
    wal_header: &WalHeader,
    analyze_result: &AriesAnalyzeResult,
    page_size: usize,
) -> anyhow::Result<RedoResult> {
    let mut iter = iterate_wal_forward(
        f,
        wal_header.relative_lsn_offset as usize,
        page_size,
        analyze_result.lsn_to_redo,
    );

    let mut result = RedoResult { db_state: None };

    while let Some((lsn, entry)) = iter.next()? {
        match entry.record {
            WalRecord::Begin
            | WalRecord::Commit
            | WalRecord::Rollback
            | WalRecord::End
            | WalRecord::CheckpointBegin
            | WalRecord::CheckpointEnd { .. } => (),

            WalRecord::HeaderSet { root, freelist } => {
                result.db_state = Some(DbState { root, freelist });
            }

            WalRecord::InteriorInit { pgid, .. }
            | WalRecord::InteriorInsert { pgid, .. }
            | WalRecord::InteriorDelete { pgid, .. }
            | WalRecord::LeafInit { pgid, .. }
            | WalRecord::LeafInsert { pgid, .. } => {
                redo_page(pager, analyze_result, lsn, &entry, pgid)?;
            }
        };
    }

    Ok(result)
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

    match entry.record {
        WalRecord::Begin
        | WalRecord::Commit
        | WalRecord::Rollback
        | WalRecord::End
        | WalRecord::CheckpointBegin
        | WalRecord::CheckpointEnd { .. }
        | WalRecord::HeaderSet { .. } => unreachable!(),

        WalRecord::InteriorInit { pgid, last } => {
            if page.init_interior(Some(lsn), last)?.is_none() {
                return Err(anyhow!("redo failed on interior init"));
            }
        }
        WalRecord::InteriorInsert {
            index,
            raw,
            ptr,
            key_size,
            ..
        } => {
            let Some(mut page) = page.into_interior() else {
                return Err(anyhow!(
                    "redo failed on interior insert because page is not an interior"
                ));
            };
            let ok = page.insert_content(Some(lsn), index, &mut Bytes::new(raw), key_size, ptr)?;
            if !ok {
                return Err(anyhow!(
                    "redo failed on interior insert because the content can't be inserted"
                ));
            }
        }
        WalRecord::InteriorDelete { index, .. } => {
            let Some(mut page) = page.into_interior() else {
                return Err(anyhow!(
                    "redo failed on interior delete because page is not an interior"
                ));
            };
            page.delete(Some(lsn), index)?;
        }

        WalRecord::LeafInit { pgid } => {
            if page.init_leaf(Some(lsn))?.is_none() {
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
            let ok =
                page.insert_content(Some(lsn), index, &mut Bytes::new(raw), key_size, value_size)?;
            if !ok {
                return Err(anyhow!(
                    "redo failed on leaf insert because the content can't be inserted"
                ));
            }
        }
    }

    Ok(())
}

fn undo(analyze_result: &AriesAnalyzeResult) -> anyhow::Result<()> {
    match analyze_result.active_tx {
        TxState::None => Ok(()),

        // TODO: maybe just create the DB, and let the DB handle the rollback
        TxState::Active(..) => todo!("need to abort"),

        // TODO: maybe just create the DB, and let the DB handle the rollback
        TxState::Committing(..) => todo!("just need to create txn-end record"),

        // TODO: maybe just create the DB, and let the DB handle the rollback
        TxState::Aborting(..) => todo!(
            "continue aborting, find the first non-CLR record and continue undo it from there"
        ),
    }
}
