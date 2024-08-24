use crate::content::Bytes;
use crate::id::{Lsn, PageId, TxId};
use crate::log::{WalEntry, WalKind};
use crate::wal::Wal;

pub(crate) enum LogContext<'a> {
    Runtime(&'a Wal),
    Redo(Lsn),
    Undo(&'a Wal, Lsn),
}

impl<'a> LogContext<'a> {
    pub(crate) fn runtime(wal: &'a Wal) -> Self {
        Self::Runtime(wal)
    }

    pub(crate) fn redo(lsn: Lsn) -> Self {
        Self::Redo(lsn)
    }

    pub(crate) fn undo(wal: &'a Wal, clr: Lsn) -> Self {
        Self::Undo(wal, clr)
    }

    fn is_undo(&self) -> bool {
        matches!(self, Self::Undo(..))
    }

    fn clr(&self) -> Option<Lsn> {
        if let Self::Undo(_, lsn) = self {
            Some(*lsn)
        } else {
            None
        }
    }

    pub(crate) fn record1<'e, F>(&self, entry: F) -> anyhow::Result<Lsn>
    where
        F: FnOnce() -> WalKind<'e>,
    {
        let lsn = match self {
            Self::Runtime(wal) => wal.append_log(WalEntry {
                clr: None,
                kind: entry(),
            })?,
            Self::Undo(wal, clr) => wal.append_log(WalEntry {
                clr: Some(*clr),
                kind: entry(),
            })?,
            Self::Redo(lsn) => *lsn,
        };
        Ok(lsn)
    }

    pub(crate) fn record2<'e, F, U>(&self, entry: F, compensation_entry: U) -> anyhow::Result<Lsn>
    where
        F: FnOnce() -> WalKind<'e>,
        U: FnOnce() -> WalKind<'e>,
    {
        let lsn = match self {
            Self::Runtime(wal) => wal.append_log(WalEntry {
                clr: None,
                kind: entry(),
            })?,
            Self::Undo(wal, clr) => wal.append_log(WalEntry {
                clr: Some(*clr),
                kind: compensation_entry(),
            })?,
            Self::Redo(lsn) => *lsn,
        };
        Ok(lsn)
    }

    pub(crate) fn record_set_state(
        &self,
        root: Option<PageId>,
        old_root: Option<PageId>,
        freelist: Option<PageId>,
        old_freelist: Option<PageId>,
        page_count: u64,
        old_page_count: u64,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::HeaderSet {
            root,
            old_root,
            freelist,
            old_freelist,
            page_count,
            old_page_count,
        })
    }

    pub(crate) fn record_alloc(&self, txid: TxId, pgid: PageId) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::AllocPage { txid, pgid })
    }

    pub(crate) fn record_dealloc(&self, txid: TxId, pgid: PageId) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::DeallocPage { txid, pgid })
    }

    pub(crate) fn record_interior_init(
        &self,
        txid: TxId,
        pgid: PageId,
        last: PageId,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::InteriorInit { txid, pgid, last })
    }

    pub(crate) fn record_interior_set(
        &self,
        txid: TxId,
        pgid: PageId,
        payload: Bytes,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::InteriorSet {
            txid,
            pgid,
            page_version: 0,
            payload,
        })
    }

    pub(crate) fn record_interior_reset(
        &self,
        txid: TxId,
        pgid: PageId,
        payload: Bytes,
    ) -> anyhow::Result<Lsn> {
        self.record2(
            || WalKind::InteriorReset {
                txid,
                pgid,
                page_version: 0,
                payload,
            },
            || WalKind::InteriorResetForUndo { txid, pgid },
        )
    }

    pub(crate) fn record_interior_set_last(
        &self,
        txid: TxId,
        pgid: PageId,
        last: PageId,
        old_last: PageId,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::InteriorSetLast {
            txid,
            pgid,
            last,
            old_last,
        })
    }

    pub(crate) fn record_interior_set_cell_ptr(
        &self,
        txid: TxId,
        pgid: PageId,
        index: usize,
        ptr: PageId,
        old_ptr: PageId,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::InteriorSetCellPtr {
            txid,
            pgid,
            index,
            ptr,
            old_ptr,
        })
    }

    pub(crate) fn record_interior_set_cell_overflow(
        &self,
        txid: TxId,
        pgid: PageId,
        index: usize,
        overflow: Option<PageId>,
        old_overflow: Option<PageId>,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::InteriorSetCellOverflow {
            txid,
            pgid,
            index,
            overflow,
            old_overflow,
        })
    }

    pub(crate) fn record_interior_insert(
        &self,
        txid: TxId,
        pgid: PageId,
        index: usize,
        raw: Bytes,
        ptr: PageId,
        key_size: usize,
        overflow: Option<PageId>,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::InteriorInsert {
            txid,
            pgid,
            index,
            raw,
            ptr,
            key_size,
            overflow,
        })
    }

    pub(crate) fn record_interior_delete(
        &self,
        txid: TxId,
        pgid: PageId,
        index: usize,
        old_raw: Bytes,
        old_ptr: PageId,
        old_overflow: Option<PageId>,
        old_key_size: usize,
    ) -> anyhow::Result<Lsn> {
        self.record2(
            || WalKind::InteriorDelete {
                txid,
                pgid,
                index,
                old_raw,
                old_ptr,
                old_overflow,
                old_key_size,
            },
            || WalKind::InteriorDeleteForUndo { txid, pgid, index },
        )
    }

    pub(crate) fn record_leaf_init(&self, txid: TxId, pgid: PageId) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::LeafInit { txid, pgid })
    }

    pub(crate) fn record_leaf_set(
        &self,
        txid: TxId,
        pgid: PageId,
        payload: Bytes,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::LeafSet {
            txid,
            pgid,
            page_version: 0,
            payload,
        })
    }

    pub(crate) fn record_leaf_reset(
        &self,
        txid: TxId,
        pgid: PageId,
        payload: Bytes,
    ) -> anyhow::Result<Lsn> {
        self.record2(
            || WalKind::LeafReset {
                txid,
                pgid,
                page_version: 0,
                payload,
            },
            || WalKind::LeafResetForUndo { txid, pgid },
        )
    }

    pub(crate) fn record_leaf_delete(
        &self,
        txid: TxId,
        pgid: PageId,
        index: usize,
        old_raw: Bytes,
        old_overflow: Option<PageId>,
        old_key_size: usize,
        old_val_size: usize,
    ) -> anyhow::Result<Lsn> {
        self.record2(
            || WalKind::LeafDelete {
                txid,
                pgid,
                index,
                old_raw,
                old_overflow,
                old_key_size,
                old_val_size,
            },
            || WalKind::LeafDeleteForUndo { txid, pgid, index },
        )
    }

    pub(crate) fn record_leaf_set_next(
        &self,
        txid: TxId,
        pgid: PageId,
        next: Option<PageId>,
        old_next: Option<PageId>,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::LeafSetNext {
            txid,
            pgid,
            next,
            old_next,
        })
    }

    pub(crate) fn record_leaf_set_cell_overflow(
        &self,
        txid: TxId,
        pgid: PageId,
        index: usize,
        overflow: Option<PageId>,
        old_overflow: Option<PageId>,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::LeafSetOverflow {
            txid,
            pgid,
            index,
            overflow,
            old_overflow,
        })
    }

    pub(crate) fn record_leaf_insert(
        &self,
        txid: TxId,
        pgid: PageId,
        index: usize,
        raw: Bytes<'a>,
        overflow: Option<PageId>,
        key_size: usize,
        value_size: usize,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::LeafInsert {
            txid,
            pgid,
            index,
            raw,
            overflow,
            key_size,
            value_size,
        })
    }

    pub(crate) fn record_overflow_set_next(
        &self,
        txid: TxId,
        pgid: PageId,
        next: Option<PageId>,
        old_next: Option<PageId>,
    ) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::OverflowSetNext {
            txid,
            pgid,
            next,
            old_next,
        })
    }

    pub(crate) fn record_overflow_set_content(
        &self,
        txid: TxId,
        pgid: PageId,
        raw: Bytes<'a>,
        next: Option<PageId>,
    ) -> anyhow::Result<Lsn> {
        self.record2(
            || WalKind::OverflowSetContent {
                txid,
                pgid,
                raw,
                next,
            },
            || WalKind::OverflowSetContentForUndo { txid, pgid },
        )
    }

    pub(crate) fn record_overflow_init(&self, txid: TxId, pgid: PageId) -> anyhow::Result<Lsn> {
        self.record1(|| WalKind::OverflowInit { txid, pgid })
    }

    pub(crate) fn record_overflow_reset(
        &self,
        txid: TxId,
        pgid: PageId,
        payload: Bytes,
    ) -> anyhow::Result<Lsn> {
        self.record2(
            || WalKind::OverflowReset {
                txid,
                pgid,
                page_version: 0,
                payload,
            },
            || WalKind::OverflowResetForUndo { txid, pgid },
        )
    }
}
