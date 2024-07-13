use std::num::NonZeroU64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PageId(NonZeroU64);

impl PageId {
    pub(crate) fn new(id: u64) -> Option<Self> {
        if id >= 1 << 56 {
            None
        } else {
            NonZeroU64::new(id).map(Self)
        }
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.get()
    }
}
