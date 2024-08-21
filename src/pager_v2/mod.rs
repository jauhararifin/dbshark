mod manager;
mod page;
mod buffer;

pub(crate) const MINIMUM_PAGE_SIZE: usize = 256;
pub(crate) const MAXIMUM_PAGE_SIZE: usize = 0x4000;

pub(crate) use crate::pager_v2::manager::Pager;
