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

    pub(crate) fn record<'e, F, U>(&self, entry: F, compensation_entry: U) -> anyhow::Result<Lsn>
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

    pub(crate) fn record_alloc(&self, txid: TxId, pgid: PageId) -> anyhow::Result<Lsn> {
        self.record(
            || WalKind::AllocPage { txid, pgid },
            || WalKind::AllocPage { txid, pgid },
        )
    }
}
