use crate::pager::{PageId, PageIdExt};
use crate::recovery::WAL_HEADER_SIZE;
use anyhow::anyhow;
use parking_lot::{Mutex, RwLock};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::os::unix::fs::MetadataExt;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TxId(NonZeroU64);

impl TxId {
    pub(crate) fn new(id: u64) -> Option<Self> {
        if id >= 1 << 56 {
            None
        } else {
            NonZeroU64::new(id).map(Self)
        }
    }

    pub(crate) fn get(self) -> u64 {
        self.0.get()
    }

    pub(crate) fn from_be_bytes(lsn: [u8; 8]) -> Option<Self> {
        Self::new(u64::from_be_bytes(lsn))
    }

    pub(crate) fn next(&self) -> TxId {
        Self(self.0.checked_add(1).unwrap())
    }
}

pub(crate) trait TxIdExt {
    fn to_be_bytes(&self) -> [u8; 8];
}

impl TxIdExt for TxId {
    fn to_be_bytes(&self) -> [u8; 8] {
        self.0.get().to_be_bytes()
    }
}
impl TxIdExt for Option<TxId> {
    fn to_be_bytes(&self) -> [u8; 8] {
        if let Some(pgid) = self {
            pgid.to_be_bytes()
        } else {
            0u64.to_be_bytes()
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Lsn(NonZeroU64);

impl Lsn {
    pub(crate) fn new(lsn: u64) -> Option<Self> {
        NonZeroU64::new(lsn).map(Self)
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.get()
    }

    pub(crate) fn add(&self, offset: usize) -> Self {
        let v = self.0.get() + offset as u64;
        Self::new(v).expect("should not overflow")
    }

    pub(crate) fn from_be_bytes(lsn: [u8; 8]) -> Option<Self> {
        Self::new(u64::from_be_bytes(lsn))
    }
}

pub(crate) trait LsnExt {
    fn to_be_bytes(&self) -> [u8; 8];
    fn add(&self, offset: usize) -> Lsn;
}

impl LsnExt for Lsn {
    fn to_be_bytes(&self) -> [u8; 8] {
        self.0.get().to_be_bytes()
    }

    fn add(&self, offset: usize) -> Lsn {
        let v = self.0.get() + offset as u64;
        Self::new(v).expect("should not overflow")
    }
}

impl LsnExt for Option<Lsn> {
    fn to_be_bytes(&self) -> [u8; 8] {
        if let Some(pgid) = self {
            pgid.to_be_bytes()
        } else {
            0u64.to_be_bytes()
        }
    }

    fn add(&self, offset: usize) -> Lsn {
        let v = self.as_ref().map(Lsn::get).unwrap_or(0) + offset as u64;
        Lsn::new(v).unwrap()
    }
}

// TODO: when a txn is committed, we need to flush their commit log record to disk. But,
// we can actually start the next transaction right away without waiting for the flush to complete.
// We just need to block the `commit()` call until the flush is done. This way, we can improve
// the throughput of the system. Also flushing the log to disk can be done in a separate thread
// making the flushing process non-blocking.
pub(crate) struct Wal {
    f: Mutex<File>,

    relative_lsn_offset: u64,

    internal: RwLock<WalInternal>,
}

struct WalInternal {
    buffer: Vec<u8>,
    offset_end: usize,
    first_lsn_in_buffer: Option<Lsn>,
    next_lsn: Lsn,
}

impl Wal {
    pub(crate) fn new(
        mut f: File,
        relative_lsn_offset: u64,
        next_lsn: Lsn,
        page_size: usize,
    ) -> anyhow::Result<Self> {
        let buff_size = page_size * 100;

        let f = Mutex::new(f);

        Ok(Wal {
            f,

            relative_lsn_offset,

            internal: RwLock::new(WalInternal {
                buffer: vec![0u8; buff_size],
                offset_end: 0,
                first_lsn_in_buffer: None,
                next_lsn,
            }),
        })
    }

    pub(crate) fn append(
        &self,
        txid: TxId,
        clr: Option<Lsn>,
        record: WalRecord,
    ) -> anyhow::Result<Lsn> {
        let entry = WalEntry { txid, clr, record };
        let size = entry.size();

        let mut internal = self.internal.write();
        if internal.offset_end + size > internal.buffer.len() {
            Self::flush(&mut self.f.lock(), &mut internal)?;
        }

        let offset_end = internal.offset_end;
        let lsn = internal.next_lsn;
        entry.encode(&mut internal.buffer[offset_end..offset_end + size]);
        internal.offset_end += size;
        internal.next_lsn = internal.next_lsn.add(size);

        if internal.first_lsn_in_buffer.is_none() {
            internal.first_lsn_in_buffer = Some(lsn);
        }

        log::debug!("appended wal entry={entry:?} lsn={lsn:?}");
        Ok(lsn)
    }

    pub(crate) fn sync(&self, lsn: Lsn) -> anyhow::Result<()> {
        let mut internal = self.internal.write();
        Self::flush(&mut self.f.lock(), &mut internal)
    }

    fn flush(f: &mut File, internal: &mut WalInternal) -> anyhow::Result<()> {
        f.seek(SeekFrom::End(0))?;
        f.write_all(&internal.buffer[..internal.offset_end])?;
        f.sync_all()?;
        internal.offset_end = 0;
        internal.first_lsn_in_buffer = None;
        Ok(())
    }

    pub(crate) fn iterate_back(&self, upper_bound: Lsn) -> WalBackwardIterator {
        let internal = self.internal.read();
        let mut buffer = vec![0u8; internal.buffer.len()];
        let buffer_len = buffer.len();
        if let Some(first_lsn_in_buffer) = internal.first_lsn_in_buffer {
            if upper_bound > first_lsn_in_buffer {
                let offset = upper_bound.get() - first_lsn_in_buffer.get();
                let to_copy = &internal.buffer[..offset as usize];
                buffer[buffer_len - to_copy.len()..].copy_from_slice(to_copy);
                return WalBackwardIterator {
                    wal: self,
                    lsn: upper_bound,
                    buffer,
                    start_offset: buffer_len - to_copy.len(),
                    end_offset: buffer_len,
                };
            }
        }

        WalBackwardIterator {
            wal: self,
            lsn: upper_bound,
            buffer,
            start_offset: buffer_len,
            end_offset: buffer_len,
        }
    }

    pub(crate) fn shutdown(self) -> anyhow::Result<()> {
        let internal = self.internal.into_inner();

        let mut f = self.f.into_inner();
        f.write_all(&internal.buffer[..internal.offset_end])?;
        f.sync_all()?;

        Ok(())
    }
}

pub(crate) struct WalBackwardIterator<'a> {
    wal: &'a Wal,

    lsn: Lsn,
    buffer: Vec<u8>,
    start_offset: usize,
    end_offset: usize,
}

impl<'a> WalBackwardIterator<'a> {
    pub(crate) fn next(&mut self) -> anyhow::Result<Option<(Lsn, WalEntry)>> {
        let offset = self.lsn.get() - self.wal.relative_lsn_offset;
        if offset as usize <= 2 * WAL_HEADER_SIZE {
            return Ok(None);
        }

        if self.end_offset < 8 {
            self.reset();
        }

        let buff = &self.buffer[self.start_offset..self.end_offset];
        if buff.len() < 8 {
            let s = 8 - buff.len();
            let mut f = self.wal.f.lock();
            f.seek(SeekFrom::Start(offset - 8))?;
            f.read_exact(&mut self.buffer[self.start_offset - s..self.start_offset])?;
            self.start_offset -= s;
        }

        let buff = &self.buffer[self.start_offset..self.end_offset];
        let size = u64::from_be_bytes(buff[buff.len() - 8..].try_into().unwrap());

        if self.end_offset < size as usize {
            self.reset();
        }

        if self.buffer.len() < size as usize {
            let s = size as usize - self.buffer.len();
            let mut f = self.wal.f.lock();
            f.seek(SeekFrom::Start(offset - size))?;
            f.read_exact(&mut self.buffer[self.start_offset - s..self.start_offset])?;
            self.start_offset -= s;
        }

        match WalEntry::decode(&self.buffer[self.end_offset - size as usize..self.end_offset]) {
            WalDecodeResult::Ok(entry) => {
                self.lsn = Lsn::new(self.lsn.get() - size).unwrap();
                self.end_offset -= size as usize;
                Ok(Some((self.lsn, entry)))
            }

            WalDecodeResult::NeedMoreBytes | WalDecodeResult::Incomplete => {
                Err(anyhow!("can't decode wal entry"))
            }
            WalDecodeResult::Err(err) => Err(err),
        }
    }

    fn reset(&mut self) {
        let len = self.end_offset - self.start_offset;
        let buffer_len = self.buffer.len();
        for i in 0..len {
            self.buffer[buffer_len - i] = self.buffer[self.end_offset - 1];
        }
        self.end_offset = buffer_len;
        self.start_offset = buffer_len - len;
    }
}

#[derive(Debug)]
pub(crate) enum WalRecord<'a> {
    Begin,
    Commit,
    Rollback,
    End,

    HeaderSet {
        root: Option<PageId>,
        old_root: Option<PageId>,

        freelist: Option<PageId>,
        old_freelist: Option<PageId>,
    },

    InteriorReset {
        pgid: PageId,
    },
    InteriorInit {
        pgid: PageId,
        last: PageId,
    },
    InteriorInsert {
        pgid: PageId,
        index: usize,
        raw: &'a [u8],
        ptr: PageId,
        key_size: usize,
    },
    InteriorDelete {
        pgid: PageId,
        index: usize,
        old_raw: &'a [u8],
        old_ptr: PageId,
        old_overflow: Option<PageId>,
        old_key_size: usize,
    },

    LeafReset {
        pgid: PageId,
    },
    LeafInit {
        pgid: PageId,
    },
    LeafInsert {
        pgid: PageId,
        index: usize,
        raw: &'a [u8],
        overflow: Option<PageId>,
        key_size: usize,
        value_size: usize,
    },

    CheckpointBegin,
    CheckpointEnd {
        dirty_pages: Vec<DirtyPage>,
        active_tx: TxState,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirtyPage {
    pub(crate) id: PageId,
    pub(crate) rec_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TxState {
    None,
    Active(TxId),
    Committing(TxId),
    Aborting(TxId),
}

const WAL_RECORD_BEGIN_KIND: u8 = 1;
const WAL_RECORD_COMMIT_KIND: u8 = 2;
const WAL_RECORD_ROLLBACK_KIND: u8 = 3;
const WAL_RECORD_END_KIND: u8 = 4;

const WAL_RECORD_HEADER_SET_KIND: u8 = 10;

const WAL_RECORD_INTERIOR_RESET_KIND: u8 = 20;
const WAL_RECORD_INTERIOR_INIT_KIND: u8 = 21;
const WAL_RECORD_INTERIOR_INSERT_KIND: u8 = 22;
const WAL_RECORD_INTERIOR_DELETE_KIND: u8 = 23;

const WAL_RECORD_LEAF_RESET_KIND: u8 = 30;
const WAL_RECORD_LEAF_INIT_KIND: u8 = 31;
const WAL_RECORD_LEAF_INSERT_KIND: u8 = 32;

const WAL_RECORD_CHECKPOINT_BEGIN_KIND: u8 = 100;
const WAL_RECORD_CHECKPOINT_END_KIND: u8 = 101;

impl<'a> WalRecord<'a> {
    fn kind(&self) -> u8 {
        match self {
            WalRecord::Begin => WAL_RECORD_BEGIN_KIND,
            WalRecord::Commit => WAL_RECORD_COMMIT_KIND,
            WalRecord::Rollback => WAL_RECORD_ROLLBACK_KIND,
            WalRecord::End => WAL_RECORD_END_KIND,

            WalRecord::HeaderSet { .. } => WAL_RECORD_HEADER_SET_KIND,

            WalRecord::InteriorReset { .. } => WAL_RECORD_INTERIOR_RESET_KIND,
            WalRecord::InteriorInit { .. } => WAL_RECORD_INTERIOR_INIT_KIND,
            WalRecord::InteriorInsert { .. } => WAL_RECORD_INTERIOR_INSERT_KIND,
            WalRecord::InteriorDelete { .. } => WAL_RECORD_INTERIOR_DELETE_KIND,

            WalRecord::LeafReset { .. } => WAL_RECORD_LEAF_RESET_KIND,
            WalRecord::LeafInit { .. } => WAL_RECORD_LEAF_INIT_KIND,
            WalRecord::LeafInsert { .. } => WAL_RECORD_LEAF_INSERT_KIND,

            WalRecord::CheckpointBegin { .. } => WAL_RECORD_CHECKPOINT_BEGIN_KIND,
            WalRecord::CheckpointEnd { .. } => WAL_RECORD_CHECKPOINT_END_KIND,
        }
    }

    fn size(&self) -> usize {
        match self {
            WalRecord::Begin | WalRecord::Commit | WalRecord::Rollback | WalRecord::End => 0,

            WalRecord::HeaderSet { .. } => 32,

            WalRecord::InteriorReset { .. } => 8,
            WalRecord::InteriorInit { .. } => 16,
            WalRecord::InteriorInsert { raw, .. } => 24 + raw.len(),
            WalRecord::InteriorDelete { old_raw, .. } => 32 + old_raw.len(),

            WalRecord::LeafReset { .. } => 8,
            WalRecord::LeafInit { .. } => 8,
            WalRecord::LeafInsert { raw, .. } => 28 + raw.len(),

            WalRecord::CheckpointBegin => 0,
            WalRecord::CheckpointEnd { dirty_pages, .. } => 8 + 16 * dirty_pages.len() + 8,
        }
    }

    fn encode(&self, mut buff: &mut [u8]) {
        match self {
            WalRecord::Begin | WalRecord::Commit | WalRecord::Rollback | WalRecord::End => (),
            WalRecord::HeaderSet {
                root,
                old_root,
                freelist,
                old_freelist,
            } => {
                buff[0..8].copy_from_slice(&root.to_be_bytes());
                buff[8..16].copy_from_slice(&old_root.to_be_bytes());
                buff[16..24].copy_from_slice(&freelist.to_be_bytes());
                buff[24..32].copy_from_slice(&old_freelist.to_be_bytes());
            }
            WalRecord::InteriorReset { pgid } => {
                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
            }
            WalRecord::InteriorInit { pgid, last } => {
                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
                buff[8..16].copy_from_slice(&last.get().to_be_bytes());
            }
            WalRecord::InteriorInsert {
                pgid,
                index,
                raw,
                ptr,
                key_size,
            } => {
                assert!(raw.len() <= u16::MAX as usize);
                assert!(*key_size <= u32::MAX as usize);
                assert!(*index <= u16::MAX as usize);

                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
                buff[8..12].copy_from_slice(&(*key_size as u32).to_be_bytes());
                buff[12..14].copy_from_slice(&(raw.len() as u16).to_be_bytes());
                buff[14..16].copy_from_slice(&(*index as u16).to_be_bytes());
                buff[16..24].copy_from_slice(&ptr.to_be_bytes());
                buff[24..24 + raw.len()].copy_from_slice(raw);
            }
            WalRecord::InteriorDelete {
                pgid,
                index,
                old_raw,
                old_ptr,
                old_overflow,
                old_key_size,
            } => {
                assert!(old_raw.len() <= u16::MAX as usize);
                assert!(*old_key_size <= u32::MAX as usize);
                assert!(*index <= u16::MAX as usize);

                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
                buff[8..12].copy_from_slice(&(*old_key_size as u32).to_be_bytes());
                buff[12..14].copy_from_slice(&(old_raw.len() as u16).to_be_bytes());
                buff[14..16].copy_from_slice(&(*index as u16).to_be_bytes());
                buff[16..24].copy_from_slice(&old_ptr.to_be_bytes());
                buff[24..32].copy_from_slice(&old_overflow.to_be_bytes());
            }
            WalRecord::LeafReset { pgid } => {
                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
            }
            WalRecord::LeafInit { pgid } => {
                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
            }
            WalRecord::LeafInsert {
                pgid,
                index,
                raw,
                overflow,
                key_size,
                value_size,
            } => {
                assert!(raw.len() <= u16::MAX as usize);
                assert!(*index <= u16::MAX as usize);
                assert!(*key_size <= u32::MAX as usize);
                assert!(*value_size <= u32::MAX as usize);

                buff[0..8].copy_from_slice(&pgid.get().to_be_bytes());
                buff[8..10].copy_from_slice(&(*index as u16).to_be_bytes());
                buff[10..12].copy_from_slice(&(raw.len() as u16).to_be_bytes());
                buff[12..16].copy_from_slice(&(*key_size as u32).to_be_bytes());
                buff[16..24].copy_from_slice(&overflow.to_be_bytes());
                buff[24..28].copy_from_slice(&(*value_size as u32).to_be_bytes());
                buff[28..].copy_from_slice(raw);
            }
            WalRecord::CheckpointBegin => (),
            WalRecord::CheckpointEnd {
                dirty_pages,
                active_tx,
            } => {
                // WalRecord::CheckpointEnd { dirty_pages, .. } => 8 + 8 * dirty_pages.len() + 8,

                buff[0..8].copy_from_slice(&(dirty_pages.len() as u64).to_be_bytes());

                buff = &mut buff[8..];
                for (i, pgid) in dirty_pages.iter().enumerate() {
                    buff[0..8].copy_from_slice(&pgid.id.get().to_be_bytes());
                    buff[8..16].copy_from_slice(&pgid.rec_lsn.get().to_be_bytes());
                    buff = &mut buff[16..];
                }

                match active_tx {
                    TxState::None => buff[0..8].copy_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]),
                    TxState::Active(txid) => {
                        buff[0..8].copy_from_slice(&txid.to_be_bytes());
                        buff[0] = 1;
                    }
                    TxState::Committing(txid) => {
                        buff[0..8].copy_from_slice(&txid.to_be_bytes());
                        buff[0] = 2;
                    }
                    TxState::Aborting(txid) => {
                        buff[0..8].copy_from_slice(&txid.to_be_bytes());
                        buff[0] = 3;
                    }
                }
            }
        }
    }

    fn decode(mut buff: &'a [u8], kind: u8) -> anyhow::Result<Self> {
        match kind {
            WAL_RECORD_BEGIN_KIND => Ok(Self::Begin),
            WAL_RECORD_COMMIT_KIND => Ok(Self::Commit),
            WAL_RECORD_ROLLBACK_KIND => Ok(Self::Rollback),
            WAL_RECORD_END_KIND => Ok(Self::End),

            WAL_RECORD_HEADER_SET_KIND => {
                let root = PageId::from_be_bytes(buff[0..8].try_into().unwrap());
                let old_root = PageId::from_be_bytes(buff[8..16].try_into().unwrap());
                let freelist = PageId::from_be_bytes(buff[16..24].try_into().unwrap());
                let old_freelist = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                Ok(Self::HeaderSet {
                    root,
                    old_root,
                    freelist,
                    old_freelist,
                })
            }

            WAL_RECORD_INTERIOR_RESET_KIND => {
                let Some(pgid) = PageId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::InteriorReset { pgid })
            }
            WAL_RECORD_INTERIOR_INIT_KIND => {
                let Some(pgid) = PageId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let Some(last) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero last pointer in interior node"));
                };
                Ok(Self::InteriorInit { pgid, last })
            }
            WAL_RECORD_INTERIOR_INSERT_KIND => {
                let Some(pgid) = PageId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let key_size = u32::from_be_bytes(buff[8..12].try_into().unwrap());
                let raw_size = u16::from_be_bytes(buff[12..14].try_into().unwrap());
                let index = u16::from_be_bytes(buff[14..16].try_into().unwrap());
                let Some(ptr) = PageId::from_be_bytes(buff[16..24].try_into().unwrap()) else {
                    return Err(anyhow!("zero pointer in interior cell"));
                };
                Ok(Self::InteriorInsert {
                    pgid,
                    index: index as usize,
                    raw: &buff[24..24 + raw_size as usize],
                    ptr,
                    key_size: key_size as usize,
                })
            }
            WAL_RECORD_INTERIOR_DELETE_KIND => {
                let Some(pgid) = PageId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let key_size = u32::from_be_bytes(buff[8..12].try_into().unwrap());
                let raw_size = u16::from_be_bytes(buff[12..14].try_into().unwrap());
                let index = u16::from_be_bytes(buff[14..16].try_into().unwrap());
                let Some(old_ptr) = PageId::from_be_bytes(buff[16..24].try_into().unwrap()) else {
                    return Err(anyhow!("zero pointer in interior cell"));
                };
                let old_overflow = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                Ok(Self::InteriorDelete {
                    pgid,
                    index: index as usize,
                    old_raw: &buff[24..24 + raw_size as usize],
                    old_ptr,
                    old_overflow,
                    old_key_size: key_size as usize,
                })
            }

            WAL_RECORD_LEAF_RESET_KIND => {
                let Some(pgid) = PageId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::LeafReset { pgid })
            }
            WAL_RECORD_LEAF_INIT_KIND => {
                let Some(pgid) = PageId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::LeafInit { pgid })
            }
            WAL_RECORD_LEAF_INSERT_KIND => {
                let Some(pgid) = PageId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let index = u16::from_be_bytes(buff[8..10].try_into().unwrap());
                let raw_size = u16::from_be_bytes(buff[10..12].try_into().unwrap());
                let key_size = u32::from_be_bytes(buff[12..16].try_into().unwrap());
                let overflow = PageId::from_be_bytes(buff[16..24].try_into().unwrap());
                let value_size = u32::from_be_bytes(buff[24..28].try_into().unwrap());
                Ok(Self::LeafInsert {
                    pgid,
                    index: index as usize,
                    raw: &buff[28..28 + raw_size as usize],
                    overflow,
                    key_size: key_size as usize,
                    value_size: value_size as usize,
                })
            }

            WAL_RECORD_CHECKPOINT_BEGIN_KIND => Ok(Self::CheckpointBegin),
            WAL_RECORD_CHECKPOINT_END_KIND => {
                let dirty_pages_len = u64::from_be_bytes(buff[0..8].try_into().unwrap());
                let mut dirty_pages = Vec::with_capacity(dirty_pages_len as usize);

                buff = &buff[8..];
                for i in 0..dirty_pages_len {
                    let Some(pgid) = PageId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                        return Err(anyhow!("zero page id"));
                    };
                    let Some(rec_lsn) = Lsn::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                        return Err(anyhow!("zero lsn"));
                    };
                    buff = &buff[16..];
                    dirty_pages.push(DirtyPage { id: pgid, rec_lsn });
                }

                let mut txid_buff: [u8; 8] = buff[0..8].try_into().unwrap();
                txid_buff[0] = 0;
                let txid = TxId::from_be_bytes(txid_buff).ok_or(anyhow!("zero txn id"));

                let active_tx = match buff[0] {
                    0 => TxState::None,
                    1 => TxState::Active(txid?),
                    2 => TxState::Committing(txid?),
                    3 => TxState::Aborting(txid?),
                    _ => return Err(anyhow!("invalid checkpoint end active txn")),
                };

                Ok(Self::CheckpointEnd {
                    dirty_pages,
                    active_tx,
                })
            }

            _ => Err(anyhow!("invalid wal record kind {kind}")),
        }
    }
}

#[derive(Debug)]
pub(crate) struct WalEntry<'a> {
    pub(crate) txid: TxId,
    pub(crate) clr: Option<Lsn>,
    pub(crate) record: WalRecord<'a>,
}

impl<'a> WalEntry<'a> {
    fn new(txid: TxId, clr: Option<Lsn>, record: WalRecord<'a>) -> Self {
        Self { txid, clr, record }
    }

    pub(crate) fn size(&self) -> usize {
        Self::size_by_record_size(self.record.size())
    }

    fn size_by_record_size(record_size: usize) -> usize {
        // (8 bytes for txid+kind) +
        // (2 bytes for entry size) +
        // (entry) +
        // (padding) +
        // (8 bytes checksum) +
        // (8 bytes for the whole record size. this is for backward iteration)
        8 + 8 + pad8(2 + record_size) + 8 + 8
    }

    fn encode(&self, buff: &mut [u8]) {
        buff[0..8].copy_from_slice(&self.txid.get().to_be_bytes());
        buff[0] = self.record.kind();

        buff[8..16].copy_from_slice(&self.clr.to_be_bytes());

        let record_size = self.record.size();
        assert!(record_size < 1 << 16);

        buff[16..18].copy_from_slice(&(record_size as u16).to_be_bytes());
        self.record.encode(&mut buff[18..18 + record_size]);

        let next = pad8(18 + record_size);
        buff[18 + record_size..next].fill(0);

        let checksum = crc64::crc64(0, &buff[0..next]);
        buff[next..next + 8].copy_from_slice(&checksum.to_be_bytes());

        // TODO: the size is actually only 2 byte, maybe we can optimize
        // the space usage
        let next = next + 8;
        let size = self.size();
        assert!(size < 1 << 16);
        assert_eq!(size, next + 8);
        buff[next..next + 8].copy_from_slice(&(size as u64).to_be_bytes());
    }

    pub(crate) fn decode(buff: &[u8]) -> WalDecodeResult<'_> {
        if buff.len() < 32 {
            return WalDecodeResult::NeedMoreBytes;
        }

        let kind = buff[0];

        let mut txid_buff: [u8; 8] = buff[0..8].try_into().unwrap();
        txid_buff[0] = 0;
        let Some(txid) = TxId::from_be_bytes(txid_buff) else {
            return WalDecodeResult::Err(anyhow!("wal record is corrupted"));
        };

        let clr = Lsn::from_be_bytes(buff[8..16].try_into().unwrap());

        let record_size = u16::from_be_bytes(buff[16..18].try_into().unwrap()) as usize;
        let total_length = Self::size_by_record_size(record_size);
        if buff.len() < total_length {
            return WalDecodeResult::NeedMoreBytes;
        }

        let record = match WalRecord::decode(&buff[18..18 + record_size], kind) {
            Ok(record) => record,
            Err(e) => return WalDecodeResult::Err(e),
        };
        let next = pad8(18 + record_size);

        let calculated_checksum = crc64::crc64(0, &buff[0..next]);
        let stored_checksum = u64::from_be_bytes(buff[next..next + 8].try_into().unwrap());
        if calculated_checksum != stored_checksum {
            // TODO: if there is an incomplete record, followed by complete record, it means
            // something is wrong and we should warn the user
            return WalDecodeResult::Incomplete;
        }

        WalDecodeResult::Ok(WalEntry { txid, clr, record })
    }
}

pub(crate) enum WalDecodeResult<'a> {
    Ok(WalEntry<'a>),
    NeedMoreBytes,
    Incomplete,
    Err(anyhow::Error),
}

fn pad8(size: usize) -> usize {
    (size + 7) & !7
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txid() {
        assert_eq!(None, TxId::new(0), "txid cannot be zero");
        assert_eq!(1, TxId::new(1).unwrap().get());
        assert_eq!(10, TxId::new(10).unwrap().get());
        assert_eq!(10, TxId::new(10).unwrap().get());
        let max_txid = TxId::new(72057594037927935).unwrap();
        assert_eq!(72057594037927935, max_txid.get());
        assert_eq!(
            0,
            max_txid.get().to_be_bytes()[0],
            "first MSB of txid should be zero"
        );

        assert_eq!(
            None,
            TxId::new(72057594037927936),
            "txid cannot be 2^56 or graeter"
        );
    }

    #[test]
    fn test_encode() {
        let entry = WalEntry {
            txid: TxId::new(1).unwrap(),
            clr: None,
            record: WalRecord::Begin,
        };
        let mut buff = vec![0u8; entry.size()];
        entry.encode(&mut buff);
        assert_eq!(
            &[
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // kind=1, txid=1
                0x00, 0x00, // entry size
                // the entry is zero bytes
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // pad to 8 bytes
                0xc6, 0xad, 0x9c, 0xb3, 0xa7, 0xbb, 0x57, 0x88, // checksum
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, // size
            ],
            buff.as_slice()
        );
    }

    #[test]
    fn test_wal() {
        let dir = tempfile::tempdir().unwrap();

        let file_path = dir.path().join("test.wal");
        let mut file = File::create(file_path).unwrap();

        let page_size = 256;
        let wal = Wal::new(file, 0, Lsn::new(64).unwrap(), page_size).unwrap();

        let lsn = wal
            .append(TxId::new(1).unwrap(), None, WalRecord::Begin)
            .unwrap();
        assert_eq!(64, lsn.get());

        let lsn = wal
            .append(
                TxId::new(1).unwrap(),
                None,
                WalRecord::InteriorInit {
                    pgid: PageId::new(10).unwrap(),
                    last: PageId::new(10).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(96, lsn.get());

        dir.close().unwrap();
    }
}
