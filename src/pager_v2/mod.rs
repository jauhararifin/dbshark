mod buffer;
mod evictor;
mod file_manager;
mod log;
mod manager;
mod page;

pub(crate) const MINIMUM_PAGE_SIZE: usize = 256;
pub(crate) const MAXIMUM_PAGE_SIZE: usize = 0x4000;

pub(crate) use crate::pager_v2::log::LogContext;
pub(crate) use crate::pager_v2::manager::{PageRead, PageWrite, Pager};
pub(crate) use crate::pager_v2::page::{
    InteriorCell, InteriorPage, InteriorPageRead, InteriorPageWrite, LeafCell, LeafPage,
    LeafPageRead, LeafPageWrite, OverflowPageRead, OverflowPageWrite, PageOps, PageWriteOps,
};
