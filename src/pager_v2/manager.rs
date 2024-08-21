use crate::id::{Lsn, PageId};
use crate::pager_v2::buffer::{BufferPool, ReadFrame, WriteFrame};
use crate::pager_v2::evictor::Evictor;
use crate::pager_v2::file_manager::FileManager;
use crate::pager_v2::page::{PageKind, PageMeta};
use crate::pager_v2::{MAXIMUM_PAGE_SIZE, MINIMUM_PAGE_SIZE};
use crate::wal::Wal;
use anyhow::anyhow;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub(crate) struct Pager {
    page_size: usize,
    n: usize,

    state: RwLock<DbState>,
    pool: BufferPool,
    internal: RwLock<PagerInternal>,
    evictor: Mutex<Evictor>,
}

struct PagerInternal {
    page_to_frame: HashMap<PageId, usize>,
    file: FileManager,
}

impl Pager {
    pub(crate) fn new(
        path: &Path,
        wal: Arc<Wal>,
        page_size: usize,
        n: usize,
    ) -> anyhow::Result<Self> {
        Self::check_page_size(page_size)?;
        if n < 10 {
            return Err(anyhow!(
                "the size of buffer poll must be at least 10, but got {n}",
            ));
        }

        Ok(Self {
            page_size,
            n,

            state: RwLock::new(DbState::default()),
            pool: BufferPool::new(page_size, n),
            internal: RwLock::new(PagerInternal {
                page_to_frame: HashMap::with_capacity(n),
                file: FileManager::new(path, wal, page_size, 10)?,
            }),
            evictor: Mutex::new(Evictor::new(n)),
        })
    }

    fn check_page_size(page_size: usize) -> anyhow::Result<()> {
        if page_size.count_ones() != 1 {
            return Err(anyhow!(
                "page size must be a power of 2, but got {}",
                page_size
            ));
        }

        if page_size < MINIMUM_PAGE_SIZE {
            return Err(anyhow!(
                "page size must be at least {} bytes, but got {}",
                MINIMUM_PAGE_SIZE,
                page_size
            ));
        }
        if page_size > MAXIMUM_PAGE_SIZE {
            return Err(anyhow!(
                "page size must be at most 16KB, but got {}",
                page_size
            ));
        }

        Ok(())
    }

    pub(crate) fn read(&self, pgid: PageId) -> anyhow::Result<PageRead> {
        let page_count = self.state.read().page_count;
        assert!(
            pgid.get() < page_count,
            "page {:?} is out of bound for reading since page_count={}",
            pgid,
            page_count,
        );

        let internal = self.internal.read();
        if let Some(frame_id) = internal.page_to_frame.get(&pgid).copied() {
            let frame = self.pool.read(frame_id);
            return Ok(PageRead { pager: self, frame });
        }
        drop(internal);

        let frame = self.acquire::<ReadFrame>(pgid)?;
        Ok(PageRead { pager: self, frame })
    }

    pub(crate) fn write(&self, pgid: PageId) -> anyhow::Result<PageWrite> {
        let page_count = self.state.read().page_count;
        assert!(
            pgid.get() < page_count,
            "page {:?} is out of bound for writing since page_count={}",
            pgid,
            page_count,
        );

        let internal = self.internal.read();
        if let Some(frame_id) = internal.page_to_frame.get(&pgid).copied() {
            let frame = self.pool.write(frame_id);
            return Ok(PageWrite { pager: self, frame });
        }
        drop(internal);

        let frame = self.acquire::<WriteFrame>(pgid)?;
        Ok(PageWrite { pager: self, frame })
    }

    fn acquire<'a, T>(&'a self, pgid: PageId) -> anyhow::Result<T>
    where
        T: BufferPoolFrame<'a> + From<WriteFrame<'a>>,
    {
        let mut internal = self.internal.write();
        let page_count = self.state.read().page_count;
        assert!(
            pgid.get() <= page_count,
            "page {pgid:?} is out of bound when acquiring page since page_count={page_count}",
        );

        let mut evictor = self.evictor.lock();
        if let Some(frame_id) = internal.page_to_frame.get(&pgid).copied() {
            evictor.acquired(frame_id);
            return Ok(T::get(&self.pool, frame_id));
        } else if let Some(frame) = self.pool.alloc(PageMeta::empty(pgid)) {
            *frame.meta = Self::fetch_page(&mut internal, pgid, frame.buffer)?;
            evictor.acquired(frame.index);
            internal.page_to_frame.insert(pgid, frame.index);
            return Ok(frame.into());
        } else {
            let (frame_id, dirty) = evictor.evict()?;
            let frame = self.pool.write(frame_id);
            let old_pgid = frame.meta.id;
            if dirty {
                internal.file.spill(frame.meta, frame.buffer)?;
            }
            *frame.meta = Self::fetch_page(&mut internal, pgid, frame.buffer)?;
            internal.page_to_frame.remove(&old_pgid);
            internal.page_to_frame.insert(pgid, frame_id);
            evictor.reset(frame_id);
            Ok(frame.into())
        }
    }

    fn fetch_page(
        internal: &mut PagerInternal,
        pgid: PageId,
        buff: &mut [u8],
    ) -> anyhow::Result<PageMeta> {
        let found = internal.file.get(pgid, buff)?;
        let default_page = PageMeta {
            id: pgid,
            kind: PageKind::None,
            lsn: Lsn::new(0),
            dirty: false,
        };
        let meta = if found {
            PageMeta::decode(buff)?.unwrap_or(default_page)
        } else {
            default_page
        };

        if meta.id != pgid {
            return Err(anyhow!(
                "page {pgid:?} was written with invalid pgid information {:?}",
                meta.id,
            ));
        }

        Ok(meta)
    }

    fn release(&self, frame_id: usize, is_dirty: bool) {
        self.evictor.lock().released(frame_id, is_dirty);
    }
}

#[derive(Default)]
pub(crate) struct DbState {
    pub(crate) root: Option<PageId>,
    pub(crate) freelist: Option<PageId>,
    pub(crate) page_count: u64,
}

pub(crate) trait BufferPoolFrame<'a> {
    fn get(pool: &'a BufferPool, index: usize) -> Self;
}

impl<'a> BufferPoolFrame<'a> for ReadFrame<'a> {
    fn get(pool: &'a BufferPool, index: usize) -> Self {
        pool.read(index)
    }
}

impl<'a> BufferPoolFrame<'a> for WriteFrame<'a> {
    fn get(pool: &'a BufferPool, index: usize) -> Self {
        pool.write(index)
    }
}

pub(crate) struct PageRead<'a> {
    pager: &'a Pager,
    frame: ReadFrame<'a>,
}

impl<'a> Drop for PageRead<'a> {
    fn drop(&mut self) {
        self.pager.release(self.frame.index, false);
    }
}

pub(crate) struct PageWrite<'a> {
    pager: &'a Pager,
    frame: WriteFrame<'a>,
}

impl<'a> Drop for PageWrite<'a> {
    fn drop(&mut self) {
        self.pager.release(self.frame.index, false);
    }
}
