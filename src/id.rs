use std::num::NonZeroU64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TxId(NonZeroU64);

impl TxId {
    #[inline]
    pub(crate) fn new(id: u64) -> Option<Self> {
        NonZeroU64::new(id).map(Self)
    }

    #[inline]
    pub(crate) fn next(&self) -> Self {
        Self(self.0.checked_add(1).unwrap())
    }

    #[inline]
    pub(crate) fn from_be_bytes(txid: [u8; 8]) -> Option<Self> {
        Self::new(u64::from_be_bytes(txid))
    }

    #[inline]
    pub(crate) fn get(self) -> u64 {
        self.0.get()
    }
}

pub(crate) trait TxIdExt {
    fn to_be_bytes(&self) -> [u8; 8];
}

impl TxIdExt for TxId {
    #[inline]
    fn to_be_bytes(&self) -> [u8; 8] {
        self.0.get().to_be_bytes()
    }
}

impl TxIdExt for Option<TxId> {
    #[inline]
    fn to_be_bytes(&self) -> [u8; 8] {
        if let Some(txid) = self {
            txid.to_be_bytes()
        } else {
            0u64.to_be_bytes()
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Lsn(u64);

impl Lsn {
    #[inline]
    pub(crate) fn new(lsn: u64) -> Self {
        assert!(lsn != u64::MAX);
        Self(lsn)
    }

    #[inline]
    pub(crate) fn from_be_bytes(lsn: [u8; 8]) -> Option<Self> {
        let val = u64::from_be_bytes(lsn);
        if val == u64::MAX {
            None
        } else {
            Some(Self(val))
        }
    }

    #[inline]
    pub(crate) fn get(&self) -> u64 {
        self.0
    }

    #[inline]
    pub(crate) fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
        assert!(self.0 != u64::MAX);
    }

    #[inline]
    pub(crate) fn sub(&self, rhs: u64) -> Self {
        Self::new(self.0.checked_sub(rhs).unwrap())
    }

    #[inline]
    pub(crate) fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
        assert!(self.0 != u64::MAX);
    }
}

pub(crate) trait LsnExt {
    fn to_be_bytes(&self) -> [u8; 8];
}

impl LsnExt for Lsn {
    #[inline]
    fn to_be_bytes(&self) -> [u8; 8] {
        self.get().to_be_bytes()
    }
}

impl LsnExt for Option<Lsn> {
    #[inline]
    fn to_be_bytes(&self) -> [u8; 8] {
        if let Some(lsn) = self {
            lsn.to_be_bytes()
        } else {
            [0xffu8; 8]
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PageId(u64);

impl PageId {
    pub(crate) fn new(id: u64) -> Option<Self> {
        if id == u64::MAX {
            None
        } else {
            Some(Self(id))
        }
    }

    pub(crate) fn get(&self) -> u64 {
        self.0
    }

    pub(crate) fn from_be_bytes(pgid: [u8; 8]) -> Option<Self> {
        Self::new(u64::from_be_bytes(pgid))
    }
}

pub(crate) trait PageIdExt {
    fn to_be_bytes(&self) -> [u8; 8];
}

impl PageIdExt for PageId {
    fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}
impl PageIdExt for Option<PageId> {
    fn to_be_bytes(&self) -> [u8; 8] {
        if let Some(pgid) = self {
            pgid.to_be_bytes()
        } else {
            u64::MAX.to_be_bytes()
        }
    }
}
