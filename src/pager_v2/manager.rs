use crate::file_lock::FileLock;
use crate::id::{Lsn, PageId};
use crate::pager_v2::buffer::{BufferPool, ReadFrame, WriteFrame};
use crate::pager_v2::evictor::Evictor;
use crate::pager_v2::page::{PageKind, PageMeta};
use crate::pager_v2::{MAXIMUM_PAGE_SIZE, MINIMUM_PAGE_SIZE};
use anyhow::anyhow;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) struct Pager {
    f: Mutex<File>,
    dbuff: Mutex<File>,
    page_size: usize,
    n: usize,

    state: RwLock<DbState>,
    pool: BufferPool,
    internal: RwLock<PagerInternal>,
}

struct PagerInternal {
    page_to_frame: HashMap<PageId, usize>,
    evictor: Evictor,
}

impl Pager {
    pub(crate) fn new(path: &Path, page_size: usize, n: usize) -> anyhow::Result<Self> {
        let db_path = path.join("main");
        let double_buff_path = path.join("dbuff");

        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(db_path)?
            .lock()?;
        if !db_file.metadata()?.is_file() {
            return Err(anyhow!("db file is not a regular file"));
        }
        let mut double_buff_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(double_buff_path)?
            .lock()?;
        if !double_buff_file.metadata()?.is_file() {
            return Err(anyhow!("double buffer file is not a regular file"));
        }

        Self::check_page_size(page_size)?;
        if n < 10 {
            return Err(anyhow!(
                "the size of buffer poll must be at least 10, but got {n}",
            ));
        }

        Self::recover_non_atomic_writes(&mut db_file, &mut double_buff_file, page_size)?;

        Ok(Self {
            f: Mutex::new(db_file),
            dbuff: Mutex::new(double_buff_file),
            page_size,
            n,

            state: RwLock::new(DbState::default()),
            pool: BufferPool::new(page_size, n),
            internal: RwLock::new(PagerInternal {
                page_to_frame: HashMap::with_capacity(n),
                evictor: Evictor::new(n),
            }),
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

    fn recover_non_atomic_writes(
        f: &mut File,
        dbuff: &mut File,
        page_size: usize,
    ) -> anyhow::Result<()> {
        let size = dbuff.metadata()?.len();
        let count = (size as usize) / page_size;

        let mut buff = vec![0u8; page_size * count];
        dbuff.seek(SeekFrom::Start(0))?;
        dbuff.read_exact(&mut buff)?;

        for i in 0..count {
            let buff = &buff[i * page_size..(i + 1) * page_size];
            let Some(meta) = PageMeta::decode(buff)? else {
                continue;
            };
            Self::write_page_no_sync(f, meta.id(), buff)?;
        }

        f.sync_all()?;
        Ok(())
    }

    fn write_page_no_sync(f: &mut File, id: PageId, buff: &[u8]) -> anyhow::Result<()> {
        let page_size = buff.len() as u64;
        let file_size = f.metadata()?.len();
        let min_size = id.get() * page_size + page_size;
        if min_size > file_size {
            f.set_len(id.get() * page_size + page_size)?;
        }
        f.seek(SeekFrom::Start(id.get() * page_size))?;
        f.write_all(buff)?;
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

        todo!();
    }

    fn acquire(&self, pgid: PageId) -> anyhow::Result<ReadFrame> {
        let mut internal = self.internal.write();
        let page_count = self.state.read().page_count;
        assert!(
            pgid.get() <= page_count,
            "page {:?} is out of bound when acquiring page since page_count={}",
            pgid,
            page_count,
        );

        if let Some(frame_id) = internal.page_to_frame.get(&pgid).copied() {
            internal.evictor.acquired(frame_id);
            return Ok(self.pool.read(frame_id));
        }

        let frame = self.pool.alloc(PageMeta::empty(pgid));
        if let Some(frame) = frame {
            *frame.meta = Self::fetch_page(frame.buffer)?;
            internal.evictor.acquired(frame.index);
            internal.page_to_frame.insert(pgid, frame.index);
            return Ok(frame.into());
        }

        let (frame_id, dirty) = internal.evictor.evict()?;
        let frame = self.pool.write(frame_id);
        let old_pgid = frame.meta.id;
        if dirty {
            Self::spill_page(&frame.meta, frame.buffer)?;
        }
        *frame.meta = Self::fetch_page(frame.buffer)?;
        internal.page_to_frame.remove(&old_pgid);
        internal.page_to_frame.insert(pgid, frame_id);
        internal.evictor.reset(frame_id);

        Ok(frame.into())
    }

    fn spill_page(meta: &PageMeta, buff: &mut [u8]) -> anyhow::Result<()> {
        todo!();
    }

    fn fetch_page(buff: &mut [u8]) -> anyhow::Result<PageMeta> {
        todo!();
    }

    fn release(&self, frame_id: usize, is_dirty: bool) {
        todo!();
    }
}

#[derive(Default)]
pub(crate) struct DbState {
    pub(crate) root: Option<PageId>,
    pub(crate) freelist: Option<PageId>,
    pub(crate) page_count: u64,
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
