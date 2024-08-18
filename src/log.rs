use crate::bins::SliceExt;
use crate::content::Bytes;
use crate::id::{Lsn, LsnExt, PageId, PageIdExt, TxId, TxIdExt};
use anyhow::anyhow;

pub(crate) const WAL_HEADER_SIZE: usize = 32;

#[derive(Debug)]
pub(crate) struct WalHeader {
    pub(crate) version: u16,
    pub(crate) checkpoint: Option<Lsn>,
    pub(crate) relative_lsn: u64,
}

impl WalHeader {
    pub(crate) fn decode(buff: &[u8]) -> Option<Self> {
        let version = buff[6..8].read_u16();
        let relative_lsn = buff[8..16].read_u64();
        let checkpoint = Lsn::from_be_bytes(buff[16..24].try_into().unwrap());

        let stored_checksum = buff[24..32].read_u64();
        let calculated_checksum = crc64::crc64(0x1d0f, &buff[0..24]);
        if stored_checksum != calculated_checksum {
            return None;
        }

        Some(WalHeader {
            version,
            checkpoint,
            relative_lsn,
        })
    }

    pub(crate) fn encode(&self, buff: &mut [u8]) {
        assert_eq!(WAL_HEADER_SIZE, buff.len());
        buff[0..6].copy_from_slice(b"db_wal");
        buff[6..8].copy_from_slice(&self.version.to_be_bytes());
        buff[8..16].copy_from_slice(&self.relative_lsn.to_be_bytes());
        buff[16..24].copy_from_slice(&self.checkpoint.to_be_bytes());
        let checksum = crc64::crc64(0x1d0f, &buff[0..24]);
        buff[24..32].copy_from_slice(&checksum.to_be_bytes());
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct WalEntry<'a> {
    pub(crate) clr: Option<Lsn>,
    pub(crate) kind: WalKind<'a>,
}

impl WalEntry<'_> {
    pub(crate) fn size(&self) -> usize {
        Self::size_by_kind_size(self.kind.size())
    }

    fn size_by_kind_size(record_size: usize) -> usize {
        // (8 bytes for clr)
        // (2 bytes for entry size)
        // (1 bytes for kind)
        // (padding 8)
        // (entry) +
        // (padding 8) +
        // (2 bytes for entry size for backward iteration) +
        // (padding 8) +
        // (8 bytes checksum) +
        pad8(8 + 2 + 6 + record_size) + 2 + 6 + 8
    }

    pub(crate) fn encode(&self, buff: &mut [u8]) {
        buff[0..8].copy_from_slice(&self.clr.to_be_bytes());

        let record_size = self.kind.size();
        assert!(
            record_size < 1 << 16,
            "record size should be less than a page"
        );
        buff[8..10].copy_from_slice(&(record_size as u16).to_be_bytes());
        buff[10] = self.kind.kind();

        self.kind.encode(&mut buff[16..16 + record_size]);

        let next = pad8(16 + record_size);
        buff[16 + record_size..next].fill(0);

        buff[next..next + 2].copy_from_slice(&(record_size as u16).to_be_bytes());
        buff[next + 2..next + 8].copy_from_slice(b"abcxyz");

        let next = next + 8;
        let checksum = crc64::crc64(0x1d0f, &buff[0..next]);
        buff[next..next + 8].copy_from_slice(&checksum.to_be_bytes());
    }

    pub(crate) fn decode(buff: &[u8]) -> WalDecodeResult<'_> {
        if buff.len() < 16 {
            return WalDecodeResult::NeedMoreBytes;
        }

        let clr = Lsn::from_be_bytes(buff[0..8].try_into().unwrap());
        let kind_size = buff[8..10].read_u16() as usize;
        let kind = buff[10];

        let total_length = Self::size_by_kind_size(kind_size);
        if buff.len() < total_length {
            return WalDecodeResult::NeedMoreBytes;
        }
        assert!(total_length < 1 << 16);

        let kind = match WalKind::decode(&buff[16..16 + kind_size], kind) {
            Ok(record) => record,
            Err(e) => return WalDecodeResult::Err(e),
        };
        let next = pad8(16 + kind_size);

        let record_size_2 = buff[next..next + 2].read_u16();
        assert_eq!(kind_size, record_size_2 as usize);

        let magic_bytes = &buff[next + 2..next + 8];
        assert_eq!(magic_bytes, b"abcxyz");

        let next = pad8(next + 2);
        let calculated_checksum = crc64::crc64(0x1d0f, &buff[0..next]);
        let stored_checksum = buff[next..next + 8].read_u64();
        if calculated_checksum != stored_checksum {
            return WalDecodeResult::Incomplete;
        }

        WalDecodeResult::Ok(WalEntry { clr, kind })
    }

    pub(crate) fn decode_backward(buff: &[u8]) -> WalDecodeResult<'_> {
        if buff.len() < 16 {
            return WalDecodeResult::NeedMoreBytes;
        }

        let kind_size = buff[buff.len() - 16..].read_u16();
        let size = Self::size_by_kind_size(kind_size as usize);
        if buff.len() < size {
            return WalDecodeResult::NeedMoreBytes;
        }

        let full_buff = &buff[buff.len() - size..];
        Self::decode(full_buff)
    }
}

#[derive(Debug)]
pub(crate) enum WalDecodeResult<'a> {
    Ok(WalEntry<'a>),
    NeedMoreBytes,
    Incomplete,
    Err(anyhow::Error),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) enum WalKind<'a> {
    Begin {
        txid: TxId,
    },
    Commit {
        txid: TxId,
    },
    Rollback {
        txid: TxId,
    },
    End {
        txid: TxId,
    },

    HeaderSet {
        root: Option<PageId>,
        old_root: Option<PageId>,
        freelist: Option<PageId>,
        old_freelist: Option<PageId>,
        page_count: u64,
        old_page_count: u64,
    },
    HeaderSetForUndo {
        txid: TxId,
        root: Option<PageId>,
        freelist: Option<PageId>,
        page_count: u64,
    },
    AllocPage {
        txid: TxId,
        pgid: PageId,
    },
    DeallocPage {
        txid: TxId,
        pgid: PageId,
    },

    InteriorReset {
        txid: TxId,
        pgid: PageId,
        page_version: u16,
        payload: Bytes<'a>,
    },
    InteriorResetForUndo {
        txid: TxId,
        pgid: PageId,
    },
    InteriorSet {
        txid: TxId,
        pgid: PageId,
        page_version: u16,
        payload: Bytes<'a>,
    },
    InteriorInit {
        txid: TxId,
        pgid: PageId,
        last: PageId,
    },
    InteriorInsert {
        txid: TxId,
        pgid: PageId,
        index: usize,
        raw: Bytes<'a>,
        ptr: PageId,
        key_size: usize,
        overflow: Option<PageId>,
    },
    InteriorDelete {
        txid: TxId,
        pgid: PageId,
        index: usize,
        old_raw: Bytes<'a>,
        old_ptr: PageId,
        old_overflow: Option<PageId>,
        old_key_size: usize,
    },
    InteriorDeleteForUndo {
        txid: TxId,
        pgid: PageId,
        index: usize,
    },
    InteriorSetCellOverflow {
        txid: TxId,
        pgid: PageId,
        index: usize,
        overflow: Option<PageId>,
        old_overflow: Option<PageId>,
    },
    InteriorSetCellPtr {
        txid: TxId,
        pgid: PageId,
        index: usize,
        ptr: PageId,
        old_ptr: PageId,
    },
    InteriorSetLast {
        txid: TxId,
        pgid: PageId,
        last: PageId,
        old_last: PageId,
    },

    LeafReset {
        txid: TxId,
        pgid: PageId,
        page_version: u16,
        payload: Bytes<'a>,
    },
    LeafResetForUndo {
        txid: TxId,
        pgid: PageId,
    },
    LeafSet {
        txid: TxId,
        pgid: PageId,
        page_version: u16,
        payload: Bytes<'a>,
    },
    LeafInit {
        txid: TxId,
        pgid: PageId,
    },
    LeafInsert {
        txid: TxId,
        pgid: PageId,
        index: usize,
        raw: Bytes<'a>,
        overflow: Option<PageId>,
        key_size: usize,
        value_size: usize,
    },
    LeafDelete {
        txid: TxId,
        pgid: PageId,
        index: usize,
        old_raw: Bytes<'a>,
        old_overflow: Option<PageId>,
        old_key_size: usize,
        old_val_size: usize,
    },
    LeafDeleteForUndo {
        txid: TxId,
        pgid: PageId,
        index: usize,
    },
    LeafSetOverflow {
        txid: TxId,
        pgid: PageId,
        index: usize,
        overflow: Option<PageId>,
        old_overflow: Option<PageId>,
    },
    LeafSetNext {
        txid: TxId,
        pgid: PageId,
        next: Option<PageId>,
        old_next: Option<PageId>,
    },

    OverflowReset {
        txid: TxId,
        pgid: PageId,
        page_version: u16,
        payload: Bytes<'a>,
    },
    OverflowResetForUndo {
        txid: TxId,
        pgid: PageId,
    },
    OverflowInit {
        txid: TxId,
        pgid: PageId,
    },
    OverflowSetContent {
        txid: TxId,
        pgid: PageId,
        raw: Bytes<'a>,
        next: Option<PageId>,
    },
    OverflowSetContentForUndo {
        txid: TxId,
        pgid: PageId,
    },
    OverflowSetNext {
        txid: TxId,
        pgid: PageId,
        next: Option<PageId>,
        old_next: Option<PageId>,
    },

    Checkpoint {
        active_tx: TxState,
        root: Option<PageId>,
        freelist: Option<PageId>,
        page_count: u64,
    },
}

const WAL_RECORD_BEGIN_KIND: u8 = 1;
const WAL_RECORD_COMMIT_KIND: u8 = 2;
const WAL_RECORD_ROLLBACK_KIND: u8 = 3;
const WAL_RECORD_END_KIND: u8 = 4;

const WAL_RECORD_HEADER_SET_KIND: u8 = 10;
const WAL_RECORD_HEADER_SET_FOR_UNDO_KIND: u8 = 11;
const WAL_RECORD_ALLOC_PAGE_KIND: u8 = 12;
const WAL_RECORD_DEALLOC_PAGE_KIND: u8 = 13;

const WAL_RECORD_INTERIOR_RESET_KIND: u8 = 20;
const WAL_RECORD_INTERIOR_RESET_FOR_UNDO_KIND: u8 = 21;
const WAL_RECORD_INTERIOR_SET_KIND: u8 = 22;
const WAL_RECORD_INTERIOR_INIT_KIND: u8 = 23;
const WAL_RECORD_INTERIOR_INSERT_KIND: u8 = 24;
const WAL_RECORD_INTERIOR_DELETE_KIND: u8 = 25;
const WAL_RECORD_INTERIOR_DELETE_FOR_UNDO_KIND: u8 = 26;
const WAL_RECORD_INTERIOR_SET_CELL_OVERFLOW_KIND: u8 = 27;
const WAL_RECORD_INTERIOR_SET_CELL_PTR_KIND: u8 = 28;
const WAL_RECORD_INTERIOR_SET_LAST_KIND: u8 = 29;

const WAL_RECORD_LEAF_RESET_KIND: u8 = 30;
const WAL_RECORD_LEAF_RESET_FOR_UNDO_KIND: u8 = 31;
const WAL_RECORD_LEAF_SET_KIND: u8 = 32;
const WAL_RECORD_LEAF_INIT_KIND: u8 = 33;
const WAL_RECORD_LEAF_INSERT_KIND: u8 = 34;
const WAL_RECORD_LEAF_DELETE_KIND: u8 = 35;
const WAL_RECORD_LEAF_DELETE_FOR_UNDO_KIND: u8 = 36;
const WAL_RECORD_LEAF_SET_CELL_OVERFLOW_KIND: u8 = 37;
const WAL_RECORD_LEAF_SET_NEXT_KIND: u8 = 38;

const WAL_RECORD_OVERFLOW_RESET_KIND: u8 = 40;
const WAL_RECORD_OVERFLOW_RESET_FOR_UNDO_KIND: u8 = 41;
const WAL_RECORD_OVERFLOW_INIT_KIND: u8 = 42;
const WAL_RECORD_OVERFLOW_SET_CONTENT_KIND: u8 = 43;
const WAL_RECORD_OVERFLOW_SET_CONTENT_FOR_UNDO_KIND: u8 = 44;
const WAL_RECORD_OVERFLOW_SET_NEXT_KIND: u8 = 45;

const WAL_RECORD_CHECKPOINT_KIND: u8 = 100;

impl<'a> WalKind<'a> {
    fn kind(&self) -> u8 {
        match self {
            WalKind::Begin { .. } => WAL_RECORD_BEGIN_KIND,
            WalKind::Commit { .. } => WAL_RECORD_COMMIT_KIND,
            WalKind::Rollback { .. } => WAL_RECORD_ROLLBACK_KIND,
            WalKind::End { .. } => WAL_RECORD_END_KIND,

            WalKind::HeaderSet { .. } => WAL_RECORD_HEADER_SET_KIND,
            WalKind::HeaderSetForUndo { .. } => WAL_RECORD_HEADER_SET_FOR_UNDO_KIND,
            WalKind::AllocPage { .. } => WAL_RECORD_ALLOC_PAGE_KIND,
            WalKind::DeallocPage { .. } => WAL_RECORD_DEALLOC_PAGE_KIND,

            WalKind::InteriorReset { .. } => WAL_RECORD_INTERIOR_RESET_KIND,
            WalKind::InteriorResetForUndo { .. } => WAL_RECORD_INTERIOR_RESET_FOR_UNDO_KIND,
            WalKind::InteriorSet { .. } => WAL_RECORD_INTERIOR_SET_KIND,
            WalKind::InteriorInit { .. } => WAL_RECORD_INTERIOR_INIT_KIND,
            WalKind::InteriorInsert { .. } => WAL_RECORD_INTERIOR_INSERT_KIND,
            WalKind::InteriorDelete { .. } => WAL_RECORD_INTERIOR_DELETE_KIND,
            WalKind::InteriorDeleteForUndo { .. } => WAL_RECORD_INTERIOR_DELETE_FOR_UNDO_KIND,
            WalKind::InteriorSetCellOverflow { .. } => WAL_RECORD_INTERIOR_SET_CELL_OVERFLOW_KIND,
            WalKind::InteriorSetCellPtr { .. } => WAL_RECORD_INTERIOR_SET_CELL_PTR_KIND,
            WalKind::InteriorSetLast { .. } => WAL_RECORD_INTERIOR_SET_LAST_KIND,

            WalKind::LeafReset { .. } => WAL_RECORD_LEAF_RESET_KIND,
            WalKind::LeafResetForUndo { .. } => WAL_RECORD_LEAF_RESET_FOR_UNDO_KIND,
            WalKind::LeafSet { .. } => WAL_RECORD_LEAF_SET_KIND,
            WalKind::LeafInit { .. } => WAL_RECORD_LEAF_INIT_KIND,
            WalKind::LeafInsert { .. } => WAL_RECORD_LEAF_INSERT_KIND,
            WalKind::LeafDelete { .. } => WAL_RECORD_LEAF_DELETE_KIND,
            WalKind::LeafDeleteForUndo { .. } => WAL_RECORD_LEAF_DELETE_FOR_UNDO_KIND,
            WalKind::LeafSetOverflow { .. } => WAL_RECORD_LEAF_SET_CELL_OVERFLOW_KIND,
            WalKind::LeafSetNext { .. } => WAL_RECORD_LEAF_SET_NEXT_KIND,

            WalKind::OverflowReset { .. } => WAL_RECORD_OVERFLOW_RESET_KIND,
            WalKind::OverflowResetForUndo { .. } => WAL_RECORD_OVERFLOW_RESET_FOR_UNDO_KIND,
            WalKind::OverflowInit { .. } => WAL_RECORD_OVERFLOW_INIT_KIND,
            WalKind::OverflowSetContent { .. } => WAL_RECORD_OVERFLOW_SET_CONTENT_KIND,
            WalKind::OverflowSetContentForUndo { .. } => {
                WAL_RECORD_OVERFLOW_SET_CONTENT_FOR_UNDO_KIND
            }
            WalKind::OverflowSetNext { .. } => WAL_RECORD_OVERFLOW_SET_NEXT_KIND,

            WalKind::Checkpoint { .. } => WAL_RECORD_CHECKPOINT_KIND,
        }
    }

    fn size(&self) -> usize {
        match self {
            WalKind::Begin { .. }
            | WalKind::Commit { .. }
            | WalKind::Rollback { .. }
            | WalKind::End { .. } => 8,

            WalKind::HeaderSet { .. } => 48,
            WalKind::HeaderSetForUndo { .. } => 32,
            WalKind::AllocPage { .. } => 16,
            WalKind::DeallocPage { .. } => 16,

            WalKind::InteriorReset { payload, .. } => 8 + 8 + 2 + 2 + payload.len(),
            WalKind::InteriorResetForUndo { .. } => 8 + 8,
            WalKind::InteriorSet { payload, .. } => 8 + 8 + 2 + 2 + payload.len(),
            WalKind::InteriorInit { .. } => 24,
            WalKind::InteriorInsert { raw, .. } => 40 + raw.len(),
            WalKind::InteriorDelete { old_raw, .. } => 40 + old_raw.len(),
            WalKind::InteriorDeleteForUndo { .. } => 18,
            WalKind::InteriorSetCellOverflow { .. } => 34,
            WalKind::InteriorSetCellPtr { .. } => 34,
            WalKind::InteriorSetLast { .. } => 32,

            WalKind::LeafReset { payload, .. } => 8 + 8 + 2 + 2 + payload.len(),
            WalKind::LeafResetForUndo { .. } => 16,
            WalKind::LeafSet { payload, .. } => 8 + 8 + 2 + 2 + payload.len(),
            WalKind::LeafInit { .. } => 16,
            WalKind::LeafInsert { raw, .. } => 36 + raw.len(),
            WalKind::LeafDelete { old_raw, .. } => 36 + old_raw.len(),
            WalKind::LeafDeleteForUndo { .. } => 18,
            WalKind::LeafSetOverflow { .. } => 34,
            WalKind::LeafSetNext { .. } => 32,

            WalKind::OverflowReset { payload, .. } => 8 + 8 + 2 + 2 + payload.len(),
            WalKind::OverflowResetForUndo { .. } => 16,
            WalKind::OverflowInit { .. } => 16,
            WalKind::OverflowSetContent { raw, .. } => 26 + raw.len(),
            WalKind::OverflowSetContentForUndo { .. } => 16,
            WalKind::OverflowSetNext { .. } => 32,

            WalKind::Checkpoint { .. } => 48,
        }
    }

    fn encode(&self, buff: &mut [u8]) {
        match self {
            WalKind::Begin { txid }
            | WalKind::Commit { txid }
            | WalKind::Rollback { txid }
            | WalKind::End { txid } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
            }

            WalKind::HeaderSet {
                root,
                old_root,
                freelist,
                old_freelist,
                page_count,
                old_page_count,
            } => {
                buff[0..8].copy_from_slice(&root.to_be_bytes());
                buff[8..16].copy_from_slice(&old_root.to_be_bytes());
                buff[16..24].copy_from_slice(&freelist.to_be_bytes());
                buff[24..32].copy_from_slice(&old_freelist.to_be_bytes());
                buff[32..40].copy_from_slice(&page_count.to_be_bytes());
                buff[40..48].copy_from_slice(&old_page_count.to_be_bytes());
            }
            WalKind::HeaderSetForUndo {
                txid,
                root,
                freelist,
                page_count,
            } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&root.to_be_bytes());
                buff[16..24].copy_from_slice(&freelist.to_be_bytes());
                buff[24..32].copy_from_slice(&page_count.to_be_bytes());
            }
            WalKind::AllocPage { txid, pgid } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
            }
            WalKind::DeallocPage { txid, pgid } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
            }

            WalKind::InteriorReset {
                txid,
                pgid,
                page_version,
                payload,
            } => {
                assert!(payload.len() <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..18].copy_from_slice(&page_version.to_be_bytes());
                buff[18..20].copy_from_slice(&(payload.len() as u16).to_be_bytes());
                buff[20..20 + payload.len()].copy_from_slice(payload.slice());
            }
            WalKind::InteriorResetForUndo { txid, pgid } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
            }
            WalKind::InteriorSet {
                txid,
                pgid,
                page_version,
                payload,
            } => {
                assert!(payload.len() <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..18].copy_from_slice(&page_version.to_be_bytes());
                buff[18..20].copy_from_slice(&(payload.len() as u16).to_be_bytes());
                buff[20..20 + payload.len()].copy_from_slice(payload.slice());
            }
            WalKind::InteriorInit { txid, pgid, last } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..24].copy_from_slice(&last.to_be_bytes());
            }
            WalKind::InteriorInsert {
                txid,
                pgid,
                index,
                raw,
                ptr,
                key_size,
                overflow,
            } => {
                assert!(raw.len() <= u16::MAX as usize);
                assert!(*key_size <= u32::MAX as usize);
                assert!(*index <= u16::MAX as usize);

                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..20].copy_from_slice(&(*key_size as u32).to_be_bytes());
                buff[20..22].copy_from_slice(&(raw.len() as u16).to_be_bytes());
                buff[22..24].copy_from_slice(&(*index as u16).to_be_bytes());
                buff[24..32].copy_from_slice(&ptr.to_be_bytes());
                buff[32..40].copy_from_slice(&overflow.to_be_bytes());
                buff[40..40 + raw.len()].copy_from_slice(raw.slice());
            }
            WalKind::InteriorDelete {
                txid,
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

                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..20].copy_from_slice(&(*old_key_size as u32).to_be_bytes());
                buff[20..22].copy_from_slice(&(old_raw.len() as u16).to_be_bytes());
                buff[22..24].copy_from_slice(&(*index as u16).to_be_bytes());
                buff[24..32].copy_from_slice(&old_ptr.to_be_bytes());
                buff[32..40].copy_from_slice(&old_overflow.to_be_bytes());
                buff[40..].copy_from_slice(old_raw.slice());
            }
            WalKind::InteriorDeleteForUndo { txid, pgid, index } => {
                assert!(*index <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..18].copy_from_slice(&(*index as u16).to_be_bytes());
            }
            WalKind::InteriorSetCellOverflow {
                txid,
                pgid,
                index,
                overflow,
                old_overflow,
            } => {
                assert!(*index <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..24].copy_from_slice(&overflow.to_be_bytes());
                buff[24..32].copy_from_slice(&old_overflow.to_be_bytes());
                buff[32..34].copy_from_slice(&(*index as u16).to_be_bytes());
            }
            WalKind::InteriorSetCellPtr {
                txid,
                pgid,
                index,
                ptr,
                old_ptr,
            } => {
                assert!(*index <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..24].copy_from_slice(&ptr.to_be_bytes());
                buff[24..32].copy_from_slice(&old_ptr.to_be_bytes());
                buff[32..34].copy_from_slice(&(*index as u16).to_be_bytes());
            }
            WalKind::InteriorSetLast {
                txid,
                pgid,
                last,
                old_last,
            } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..24].copy_from_slice(&last.to_be_bytes());
                buff[24..32].copy_from_slice(&old_last.to_be_bytes());
            }

            WalKind::LeafReset {
                txid,
                pgid,
                page_version,
                payload,
            } => {
                assert!(payload.len() <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..18].copy_from_slice(&page_version.to_be_bytes());
                buff[18..20].copy_from_slice(&(payload.len() as u16).to_be_bytes());
                buff[20..20 + payload.len()].copy_from_slice(payload.slice());
            }
            WalKind::LeafResetForUndo { txid, pgid } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
            }
            WalKind::LeafSet {
                txid,
                pgid,
                page_version,
                payload,
            } => {
                assert!(payload.len() <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..18].copy_from_slice(&page_version.to_be_bytes());
                buff[18..20].copy_from_slice(&(payload.len() as u16).to_be_bytes());
                buff[20..20 + payload.len()].copy_from_slice(payload.slice());
            }
            WalKind::LeafInit { txid, pgid } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
            }
            WalKind::LeafInsert {
                txid,
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

                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..18].copy_from_slice(&(*index as u16).to_be_bytes());
                buff[18..20].copy_from_slice(&(raw.len() as u16).to_be_bytes());
                buff[20..24].copy_from_slice(&(*key_size as u32).to_be_bytes());
                buff[24..32].copy_from_slice(&overflow.to_be_bytes());
                buff[32..36].copy_from_slice(&(*value_size as u32).to_be_bytes());
                buff[36..].copy_from_slice(raw.slice());
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
                assert!(old_raw.len() <= u16::MAX as usize);
                assert!(*index <= u16::MAX as usize);
                assert!(*old_key_size <= u32::MAX as usize);
                assert!(*old_val_size <= u32::MAX as usize);

                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..18].copy_from_slice(&(*index as u16).to_be_bytes());
                buff[18..20].copy_from_slice(&(old_raw.len() as u16).to_be_bytes());
                buff[20..24].copy_from_slice(&(*old_key_size as u32).to_be_bytes());
                buff[24..32].copy_from_slice(&old_overflow.to_be_bytes());
                buff[32..36].copy_from_slice(&(*old_val_size as u32).to_be_bytes());
                buff[36..].copy_from_slice(old_raw.slice());
            }
            WalKind::LeafDeleteForUndo { txid, pgid, index } => {
                assert!(*index <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..18].copy_from_slice(&(*index as u16).to_be_bytes());
            }
            WalKind::LeafSetOverflow {
                txid,
                pgid,
                index,
                overflow,
                old_overflow,
            } => {
                assert!(*index <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..24].copy_from_slice(&overflow.to_be_bytes());
                buff[24..32].copy_from_slice(&old_overflow.to_be_bytes());
                buff[32..34].copy_from_slice(&(*index as u16).to_be_bytes());
            }
            WalKind::LeafSetNext {
                txid,
                pgid,
                next,
                old_next,
            } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..24].copy_from_slice(&next.to_be_bytes());
                buff[24..32].copy_from_slice(&old_next.to_be_bytes());
            }

            WalKind::OverflowReset {
                txid,
                pgid,
                page_version,
                payload,
            } => {
                assert!(payload.len() <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..18].copy_from_slice(&page_version.to_be_bytes());
                buff[18..20].copy_from_slice(&(payload.len() as u16).to_be_bytes());
                buff[20..20 + payload.len()].copy_from_slice(payload.slice());
            }
            WalKind::OverflowResetForUndo { txid, pgid } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
            }
            WalKind::OverflowInit { txid, pgid } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
            }
            WalKind::OverflowSetContent {
                txid,
                pgid,
                next,
                raw,
            } => {
                assert!(raw.len() <= u16::MAX as usize);
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..24].copy_from_slice(&next.to_be_bytes());
                buff[24..26].copy_from_slice(&(raw.len() as u16).to_be_bytes());
                buff[26..26 + raw.len()].copy_from_slice(raw.slice());
            }
            WalKind::OverflowSetContentForUndo { txid, pgid } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
            }
            WalKind::OverflowSetNext {
                txid,
                pgid,
                next,
                old_next,
            } => {
                buff[0..8].copy_from_slice(&txid.to_be_bytes());
                buff[8..16].copy_from_slice(&pgid.to_be_bytes());
                buff[16..24].copy_from_slice(&next.to_be_bytes());
                buff[24..32].copy_from_slice(&old_next.to_be_bytes());
            }

            WalKind::Checkpoint {
                active_tx,
                root,
                freelist,
                page_count,
            } => {
                match active_tx {
                    TxState::None => {
                        buff[0] = 1;
                        buff[8..16].copy_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
                    }
                    TxState::Active(txid) => {
                        buff[0] = 2;
                        buff[8..16].copy_from_slice(&txid.to_be_bytes());
                    }
                    TxState::Committing(txid) => {
                        buff[0] = 3;
                        buff[8..16].copy_from_slice(&txid.to_be_bytes());
                    }
                    TxState::Aborting { txid, last_undone } => {
                        buff[0] = 4;
                        buff[8..16].copy_from_slice(&txid.to_be_bytes());
                        buff[16..24].copy_from_slice(&last_undone.to_be_bytes());
                    }
                }
                buff[24..32].copy_from_slice(&root.to_be_bytes());
                buff[32..40].copy_from_slice(&freelist.to_be_bytes());
                buff[40..48].copy_from_slice(&page_count.to_be_bytes());
            }
        }
    }

    fn decode(buff: &'a [u8], kind: u8) -> anyhow::Result<Self> {
        match kind {
            WAL_RECORD_BEGIN_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                Ok(Self::Begin { txid })
            }
            WAL_RECORD_COMMIT_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                Ok(Self::Commit { txid })
            }
            WAL_RECORD_ROLLBACK_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                Ok(Self::Rollback { txid })
            }
            WAL_RECORD_END_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                Ok(Self::End { txid })
            }

            WAL_RECORD_HEADER_SET_KIND => {
                let root = PageId::from_be_bytes(buff[0..8].try_into().unwrap());
                let old_root = PageId::from_be_bytes(buff[8..16].try_into().unwrap());
                let freelist = PageId::from_be_bytes(buff[16..24].try_into().unwrap());
                let old_freelist = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                let page_count = buff[32..40].read_u64();
                let old_page_count = buff[40..48].read_u64();
                Ok(Self::HeaderSet {
                    root,
                    old_root,
                    freelist,
                    old_freelist,
                    page_count,
                    old_page_count,
                })
            }
            WAL_RECORD_HEADER_SET_FOR_UNDO_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let root = PageId::from_be_bytes(buff[8..16].try_into().unwrap());
                let freelist = PageId::from_be_bytes(buff[16..24].try_into().unwrap());
                let page_count = buff[24..32].read_u64();
                Ok(Self::HeaderSetForUndo {
                    txid,
                    root,
                    freelist,
                    page_count,
                })
            }
            WAL_RECORD_ALLOC_PAGE_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::AllocPage { txid, pgid })
            }
            WAL_RECORD_DEALLOC_PAGE_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::DeallocPage { txid, pgid })
            }

            WAL_RECORD_INTERIOR_RESET_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let page_version = buff[16..18].read_u16();
                let size = buff[18..20].read_u16();
                Ok(Self::InteriorReset {
                    txid,
                    pgid,
                    page_version,
                    payload: Bytes::new(&buff[20..20 + size as usize]),
                })
            }
            WAL_RECORD_INTERIOR_RESET_FOR_UNDO_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::InteriorResetForUndo { txid, pgid })
            }
            WAL_RECORD_INTERIOR_SET_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let page_version = buff[16..18].read_u16();
                let size = buff[18..20].read_u16();
                Ok(Self::InteriorSet {
                    txid,
                    pgid,
                    page_version,
                    payload: Bytes::new(&buff[20..20 + size as usize]),
                })
            }
            WAL_RECORD_INTERIOR_INIT_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let Some(last) = PageId::from_be_bytes(buff[16..24].try_into().unwrap()) else {
                    return Err(anyhow!("zero last pointer in interior node"));
                };
                Ok(Self::InteriorInit { txid, pgid, last })
            }
            WAL_RECORD_INTERIOR_INSERT_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let key_size = buff[16..20].read_u32();
                let raw_size = buff[20..22].read_u16();
                let index = buff[22..24].read_u16();
                let Some(ptr) = PageId::from_be_bytes(buff[24..32].try_into().unwrap()) else {
                    return Err(anyhow!("zero pointer in interior cell"));
                };
                let overflow = PageId::from_be_bytes(buff[32..40].try_into().unwrap());
                Ok(Self::InteriorInsert {
                    txid,
                    pgid,
                    index: index as usize,
                    raw: Bytes::new(&buff[40..40 + raw_size as usize]),
                    ptr,
                    key_size: key_size as usize,
                    overflow,
                })
            }
            WAL_RECORD_INTERIOR_DELETE_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let key_size = buff[16..20].read_u32();
                let raw_size = buff[20..22].read_u16();
                let index = buff[22..24].read_u16();
                let Some(old_ptr) = PageId::from_be_bytes(buff[24..32].try_into().unwrap()) else {
                    return Err(anyhow!("zero pointer in interior cell"));
                };
                let old_overflow = PageId::from_be_bytes(buff[32..40].try_into().unwrap());
                Ok(Self::InteriorDelete {
                    txid,
                    pgid,
                    index: index as usize,
                    old_raw: Bytes::new(&buff[40..40 + raw_size as usize]),
                    old_ptr,
                    old_overflow,
                    old_key_size: key_size as usize,
                })
            }
            WAL_RECORD_INTERIOR_DELETE_FOR_UNDO_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let index = buff[16..18].read_u16();
                Ok(Self::InteriorDeleteForUndo {
                    txid,
                    pgid,
                    index: index as usize,
                })
            }
            WAL_RECORD_INTERIOR_SET_CELL_OVERFLOW_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let overflow = PageId::from_be_bytes(buff[16..24].try_into().unwrap());
                let old_overflow = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                let index = buff[32..34].read_u16();
                Ok(Self::InteriorSetCellOverflow {
                    txid,
                    pgid,
                    index: index as usize,
                    overflow,
                    old_overflow,
                })
            }
            WAL_RECORD_INTERIOR_SET_CELL_PTR_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let Some(ptr) = PageId::from_be_bytes(buff[16..24].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let Some(old_ptr) = PageId::from_be_bytes(buff[24..32].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let index = buff[32..34].read_u16();
                Ok(Self::InteriorSetCellPtr {
                    txid,
                    pgid,
                    index: index as usize,
                    ptr,
                    old_ptr,
                })
            }
            WAL_RECORD_INTERIOR_SET_LAST_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let Some(last) = PageId::from_be_bytes(buff[16..24].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let Some(old_last) = PageId::from_be_bytes(buff[24..32].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::InteriorSetLast {
                    txid,
                    pgid,
                    last,
                    old_last,
                })
            }

            WAL_RECORD_LEAF_RESET_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let page_version = buff[16..18].read_u16();
                let size = buff[18..20].read_u16();
                Ok(Self::LeafReset {
                    txid,
                    pgid,
                    page_version,
                    payload: Bytes::new(&buff[20..20 + size as usize]),
                })
            }
            WAL_RECORD_LEAF_RESET_FOR_UNDO_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::LeafResetForUndo { txid, pgid })
            }
            WAL_RECORD_LEAF_SET_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let page_version = buff[16..18].read_u16();
                let size = buff[18..20].read_u16();
                Ok(Self::LeafSet {
                    txid,
                    pgid,
                    page_version,
                    payload: Bytes::new(&buff[20..20 + size as usize]),
                })
            }
            WAL_RECORD_LEAF_INIT_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::LeafInit { txid, pgid })
            }
            WAL_RECORD_LEAF_INSERT_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let index = buff[16..18].read_u16();
                let raw_size = buff[18..20].read_u16();
                let key_size = buff[20..24].read_u32();
                let overflow = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                let value_size = buff[32..36].read_u32();
                Ok(Self::LeafInsert {
                    txid,
                    pgid,
                    index: index as usize,
                    raw: Bytes::new(&buff[36..36 + raw_size as usize]),
                    overflow,
                    key_size: key_size as usize,
                    value_size: value_size as usize,
                })
            }
            WAL_RECORD_LEAF_DELETE_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let index = buff[16..18].read_u16();
                let raw_size = buff[18..20].read_u16();
                let old_key_size = buff[20..24].read_u32();
                let old_overflow = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                let old_val_size = buff[32..36].read_u32();
                Ok(Self::LeafDelete {
                    txid,
                    pgid,
                    index: index as usize,
                    old_raw: Bytes::new(&buff[36..36 + raw_size as usize]),
                    old_overflow,
                    old_key_size: old_key_size as usize,
                    old_val_size: old_val_size as usize,
                })
            }
            WAL_RECORD_LEAF_DELETE_FOR_UNDO_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let index = buff[16..18].read_u16();
                Ok(Self::LeafDeleteForUndo {
                    txid,
                    pgid,
                    index: index as usize,
                })
            }
            WAL_RECORD_LEAF_SET_CELL_OVERFLOW_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let overflow = PageId::from_be_bytes(buff[16..24].try_into().unwrap());
                let old_overflow = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                let index = buff[32..34].read_u16();
                Ok(Self::LeafSetOverflow {
                    txid,
                    pgid,
                    index: index as usize,
                    overflow,
                    old_overflow,
                })
            }
            WAL_RECORD_LEAF_SET_NEXT_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let next = PageId::from_be_bytes(buff[16..24].try_into().unwrap());
                let old_next = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                Ok(Self::LeafSetNext {
                    txid,
                    pgid,
                    next,
                    old_next,
                })
            }

            WAL_RECORD_OVERFLOW_RESET_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let page_version = buff[16..18].read_u16();
                let size = buff[18..20].read_u16();
                Ok(Self::OverflowReset {
                    txid,
                    pgid,
                    page_version,
                    payload: Bytes::new(&buff[20..20 + size as usize]),
                })
            }
            WAL_RECORD_OVERFLOW_RESET_FOR_UNDO_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::OverflowResetForUndo { txid, pgid })
            }
            WAL_RECORD_OVERFLOW_INIT_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::OverflowInit { txid, pgid })
            }
            WAL_RECORD_OVERFLOW_SET_CONTENT_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let next = PageId::from_be_bytes(buff[16..24].try_into().unwrap());
                let size = buff[24..26].read_u16();
                Ok(Self::OverflowSetContent {
                    txid,
                    pgid,
                    next,
                    raw: Bytes::new(&buff[26..26 + size as usize]),
                })
            }
            WAL_RECORD_OVERFLOW_SET_CONTENT_FOR_UNDO_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                Ok(Self::OverflowSetContentForUndo { txid, pgid })
            }
            WAL_RECORD_OVERFLOW_SET_NEXT_KIND => {
                let Some(txid) = TxId::from_be_bytes(buff[0..8].try_into().unwrap()) else {
                    return Err(anyhow!("empty transaction id"));
                };
                let Some(pgid) = PageId::from_be_bytes(buff[8..16].try_into().unwrap()) else {
                    return Err(anyhow!("zero page id"));
                };
                let next = PageId::from_be_bytes(buff[16..24].try_into().unwrap());
                let old_next = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                Ok(Self::OverflowSetNext {
                    txid,
                    pgid,
                    next,
                    old_next,
                })
            }

            WAL_RECORD_CHECKPOINT_KIND => {
                let active_tx = match buff[0] {
                    1 => TxState::None,
                    2 => {
                        let txid = TxId::from_be_bytes(buff[8..16].try_into().unwrap())
                            .ok_or(anyhow!("found zero transaction id"))?;
                        TxState::Active(txid)
                    }
                    3 => {
                        let txid = TxId::from_be_bytes(buff[8..16].try_into().unwrap())
                            .ok_or(anyhow!("found zero transaction id"))?;
                        TxState::Committing(txid)
                    }
                    4 => {
                        let txid = TxId::from_be_bytes(buff[8..16].try_into().unwrap())
                            .ok_or(anyhow!("found zero transaction id"))?;
                        let last_undone = Lsn::from_be_bytes(buff[16..24].try_into().unwrap())
                            .ok_or(anyhow!("found zero last_undone lsn"))?;
                        TxState::Aborting { txid, last_undone }
                    }
                    kind => return Err(anyhow!("invalid checkout end kind {kind}")),
                };

                let root = PageId::from_be_bytes(buff[24..32].try_into().unwrap());
                let freelist = PageId::from_be_bytes(buff[32..40].try_into().unwrap());
                let page_count = buff[40..48].read_u64();
                Ok(Self::Checkpoint {
                    active_tx,
                    root,
                    freelist,
                    page_count,
                })
            }
            _ => Err(anyhow!("invalid wal record kind {kind}")),
        }
    }
}

fn pad8(size: usize) -> usize {
    (size + 7) & !7
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub(crate) enum TxState {
    None,
    Active(TxId),
    Committing(TxId),
    Aborting { txid: TxId, last_undone: Lsn },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pad8() {
        for i in 0..10000 {
            let result = pad8(i);
            assert!(result % 8 == 0);
            assert!(result >= i);
            assert!(result - i < 8);
        }
    }

    #[test]
    fn test_txid() {
        assert_eq!(None, TxId::new(0), "txid cannot be zero");
        TxId::new(1).unwrap();
        TxId::new(10).unwrap();
    }

    #[test]
    fn test_encode_decode() {
        let testcases = vec![
            WalEntry {
                clr: None,
                kind: WalKind::Begin {
                    txid: TxId::new(1).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(1)),
                kind: WalKind::Commit {
                    txid: TxId::new(2).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(121)),
                kind: WalKind::Rollback {
                    txid: TxId::new(2).unwrap(),
                },
            },
            WalEntry {
                clr: None,
                kind: WalKind::End {
                    txid: TxId::new(2).unwrap(),
                },
            },
            WalEntry {
                clr: None,
                kind: WalKind::HeaderSet {
                    root: None,
                    old_root: PageId::new(1),
                    freelist: PageId::new(101),
                    old_freelist: None,
                    page_count: 10,
                    old_page_count: 0,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::HeaderSet {
                    root: PageId::new(23),
                    old_root: PageId::new(1),
                    freelist: PageId::new(101),
                    old_freelist: PageId::new(33),
                    page_count: 0,
                    old_page_count: 10,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::HeaderSetForUndo {
                    txid: TxId::new(1011).unwrap(),
                    root: PageId::new(23),
                    freelist: PageId::new(101),
                    page_count: 10,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::AllocPage {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::DeallocPage {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorReset {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    page_version: 12,
                    payload: Bytes::new(b"this_is_just_a_dummy_bytes"),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorResetForUndo {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorSet {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    page_version: 0,
                    payload: Bytes::new(b"this_is_just_a_dummy_bytes"),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorInit {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    last: PageId::new(24).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorInsert {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    index: 0,
                    raw: Bytes::new(b"content"),
                    ptr: PageId::new(101).unwrap(),
                    key_size: 2,
                    overflow: None,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorInsert {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    index: u16::MAX as usize,
                    raw: Bytes::new(b"key00000"),
                    ptr: PageId::new(101).unwrap(),
                    key_size: 123456789,
                    overflow: PageId::new(202),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorDelete {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(99).unwrap(),
                    index: 17,
                    old_raw: Bytes::new(b"the_old_raw_content"),
                    old_ptr: PageId::new(10).unwrap(),
                    old_overflow: None,
                    old_key_size: 19,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorDelete {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(99).unwrap(),
                    index: 17,
                    old_raw: Bytes::new(b"the_old_raw_content"),
                    old_ptr: PageId::new(10).unwrap(),
                    old_overflow: PageId::new(1),
                    old_key_size: 1000,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorDeleteForUndo {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(99).unwrap(),
                    index: 17,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorSetCellOverflow {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(99).unwrap(),
                    index: 17,
                    overflow: PageId::new(12),
                    old_overflow: None,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorSetCellOverflow {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(99).unwrap(),
                    index: 17,
                    overflow: None,
                    old_overflow: PageId::new(12),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorSetCellOverflow {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(99).unwrap(),
                    index: 17,
                    overflow: PageId::new(21),
                    old_overflow: PageId::new(12),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorSetCellPtr {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(99).unwrap(),
                    index: 17,
                    ptr: PageId::new(12).unwrap(),
                    old_ptr: PageId::new(21).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::InteriorSetLast {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(99).unwrap(),
                    last: PageId::new(12).unwrap(),
                    old_last: PageId::new(21).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafReset {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    page_version: 12,
                    payload: Bytes::new(b"this_is_just_a_dummy_bytes"),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafResetForUndo {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafSet {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    page_version: 0,
                    payload: Bytes::new(b"this_is_just_a_dummy_bytes"),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafInit {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafInsert {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    index: 0,
                    raw: Bytes::new(b"key00000val00000"),
                    overflow: None,
                    key_size: 8,
                    value_size: 8,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafInsert {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    index: 23,
                    raw: Bytes::new(b"key0"),
                    overflow: PageId::new(101),
                    key_size: 8,
                    value_size: 8,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafDelete {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    index: 22,
                    old_raw: Bytes::new(b"key0"),
                    old_overflow: PageId::new(101),
                    old_key_size: 8,
                    old_val_size: 8,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafDeleteForUndo {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    index: 22,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafSetOverflow {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    index: 22,
                    overflow: PageId::new(12),
                    old_overflow: None,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafSetOverflow {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    index: 22,
                    overflow: None,
                    old_overflow: PageId::new(12),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafSetOverflow {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    index: 22,
                    overflow: PageId::new(21),
                    old_overflow: PageId::new(12),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafSetNext {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    next: PageId::new(21),
                    old_next: PageId::new(12),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafSetNext {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    next: PageId::new(21),
                    old_next: None,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::LeafSetNext {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(100).unwrap(),
                    next: None,
                    old_next: PageId::new(12),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::OverflowReset {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    page_version: 0,
                    payload: Bytes::new(b"this_is_just_a_dummy_bytes"),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::OverflowResetForUndo {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::OverflowInit {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::OverflowSetContent {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    raw: Bytes::new(b"this_is_just_a_dummy_bytes"),
                    next: None,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::OverflowSetContent {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    raw: Bytes::new(b"this_is_just_a_dummy_bytes"),
                    next: PageId::new(912),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::OverflowSetContentForUndo {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::OverflowSetNext {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    next: PageId::new(912),
                    old_next: PageId::new(219),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::OverflowSetNext {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    next: None,
                    old_next: PageId::new(219),
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::OverflowSetNext {
                    txid: TxId::new(1011).unwrap(),
                    pgid: PageId::new(23).unwrap(),
                    next: PageId::new(912),
                    old_next: None,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::Checkpoint {
                    active_tx: TxState::None,
                    root: None,
                    freelist: PageId::new(100),
                    page_count: 99,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::Checkpoint {
                    active_tx: TxState::Active(TxId::new(12).unwrap()),
                    root: PageId::new(100),
                    freelist: None,
                    page_count: 99,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::Checkpoint {
                    active_tx: TxState::Committing(TxId::new(12).unwrap()),
                    root: PageId::new(100),
                    freelist: PageId::new(200),
                    page_count: 99,
                },
            },
            WalEntry {
                clr: Some(Lsn::new(99)),
                kind: WalKind::Checkpoint {
                    active_tx: TxState::Aborting {
                        txid: TxId::new(12).unwrap(),
                        last_undone: Lsn::new(11),
                    },
                    root: PageId::new(100),
                    freelist: PageId::new(200),
                    page_count: 99,
                },
            },
        ];

        for testcase in testcases {
            let mut buff = vec![0u8; testcase.size()];
            testcase.encode(&mut buff);
            let WalDecodeResult::Ok(decoded_entry) = WalEntry::decode(&buff) else {
                panic!("decode fail");
            };
            assert_eq!(testcase, decoded_entry);
            let WalDecodeResult::Ok(decoded_entry) = WalEntry::decode_backward(&buff) else {
                panic!("decode backward fail");
            };
            assert_eq!(testcase, decoded_entry);

            for i in 0..buff.len() - 1 {
                let result = WalEntry::decode(&buff[..i]);
                assert!(
                    matches!(result, WalDecodeResult::NeedMoreBytes),
                    "decode {testcase:?} result should be need more bytes, but got {result:?}"
                );
                let result = WalEntry::decode_backward(&buff[testcase.size() - i..]);
                assert!(matches!(result, WalDecodeResult::NeedMoreBytes), "decode backward {testcase:?} result should be need more bytes, but got {result:?}");
            }
        }
    }
}
