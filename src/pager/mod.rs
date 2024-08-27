mod buffer;
mod evictor;
mod file_manager;
mod log;
mod manager;
mod page;

pub(crate) const MINIMUM_PAGE_SIZE: usize = 256;
pub(crate) const MAXIMUM_PAGE_SIZE: usize = 0x4000;

pub(crate) use crate::pager::log::LogContext;
pub(crate) use crate::pager::manager::{DbState, PageRead, PageWrite, Pager};
pub(crate) use crate::pager::page::{
    BTreeCell, InteriorPage, InteriorPageWrite, LeafCell, LeafPage, LeafPageRead, LeafPageWrite,
    OverflowPage, OverflowPageRead, PageOps, PageWriteOps,
};
