mod evictor;
mod log;
mod page;
mod file_manager;
mod buffer;

pub(crate) const MINIMUM_PAGE_SIZE: usize = 256;
pub(crate) const MAXIMUM_PAGE_SIZE: usize = 0x4000;
