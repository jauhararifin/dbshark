use crate::id::{Lsn, PageId, TxId};
use crate::pager::buffer::{BufferPool, ReadFrame, WriteFrame};
use crate::pager::evictor::Evictor;
use crate::pager::file_manager::FileManager;
use crate::pager::log::{LogContext, WalSync};
use crate::pager::page::{
    PageInternal, PageInternalWrite, PageKind, PageMeta, PageOps, PageWriteOps,
};
use crate::pager::{MAXIMUM_PAGE_SIZE, MINIMUM_PAGE_SIZE};
use crate::wal::Wal;
use anyhow::anyhow;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::path::Path;

pub(crate) struct Pager {
    state: RwLock<DbState>,
    pool: BufferPool,
    internal: RwLock<PagerInternal>,
    evictor: Mutex<Evictor>,
    file: RwLock<FileManager>,
}

struct PagerInternal {
    page_to_frame: HashMap<PageId, usize>,
}

impl Pager {
    pub(crate) fn new(path: &Path, page_size: usize, n: usize) -> anyhow::Result<Self> {
        Self::check_page_size(page_size)?;
        if n < 10 {
            return Err(anyhow!(
                "the size of buffer poll must be at least 10, but got {n}",
            ));
        }

        Ok(Self {
            state: RwLock::new(DbState::default()),
            pool: BufferPool::new(page_size, n),
            internal: RwLock::new(PagerInternal {
                page_to_frame: HashMap::with_capacity(n),
            }),
            evictor: Mutex::new(Evictor::new(n)),
            file: RwLock::new(FileManager::new(path, page_size, 10)?),
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

    pub(crate) fn read(
        &self,
        wal: &impl WalSync,
        txid: TxId,
        pgid: PageId,
    ) -> anyhow::Result<PageRead> {
        log::trace!("read {txid:?} {pgid:?}");

        let page_count = self.state.read().page_count;
        assert!(
            pgid.get() < page_count,
            "page {:?} is out of bound for reading since page_count={}",
            pgid,
            page_count,
        );

        let internal = self.internal.read();
        if let Some(frame_id) = internal.page_to_frame.get(&pgid).copied() {
            self.evictor.lock().acquired(frame_id);
            let frame = self.pool.read(frame_id);
            return Ok(PageRead { pager: self, frame });
        }
        drop(internal);

        let frame = self.acquire::<ReadFrame>(wal, txid, pgid)?;
        Ok(PageRead { pager: self, frame })
    }

    pub(crate) fn write(
        &self,
        wal: &impl WalSync,
        txid: TxId,
        pgid: PageId,
    ) -> anyhow::Result<PageWrite> {
        let page_count = self.state.read().page_count;
        assert!(
            pgid.get() < page_count,
            "page {:?} is out of bound for writing since page_count={}",
            pgid,
            page_count,
        );

        let internal = self.internal.read();
        if let Some(frame_id) = internal.page_to_frame.get(&pgid).copied() {
            self.evictor.lock().acquired(frame_id);
            let frame = self.pool.write(txid, frame_id);
            return Ok(PageWrite { pager: self, frame });
        }
        drop(internal);

        let frame = self.acquire::<WriteFrame>(wal, txid, pgid)?;
        Ok(PageWrite { pager: self, frame })
    }

    fn acquire<'a, T>(&'a self, wal: &impl WalSync, txid: TxId, pgid: PageId) -> anyhow::Result<T>
    where
        T: BufferPoolFrame<'a> + From<WriteFrame<'a>>,
    {
        log::trace!("acquire {txid:?} {pgid:?}");
        let mut internal = self.internal.write();
        let page_count = self.state.read().page_count;
        assert!(
            pgid.get() <= page_count,
            "page {pgid:?} is out of bound when acquiring page since page_count={page_count}",
        );

        let mut evictor = self.evictor.lock();
        if let Some(frame_id) = internal.page_to_frame.get(&pgid).copied() {
            evictor.acquired(frame_id);
            return Ok(T::get(&self.pool, txid, frame_id));
        } else if let Some(frame) = self.pool.alloc(txid, PageMeta::empty(pgid)) {
            let mut file = self.file.write();
            *frame.meta = Self::fetch_page(&mut file, pgid, frame.buffer)?;
            evictor.acquired(frame.index);
            internal.page_to_frame.insert(pgid, frame.index);
            return Ok(frame.into());
        } else {
            let (frame_id, dirty) = evictor.evict_and_acquire()?;
            let frame = self.pool.write(txid, frame_id);
            let old_pgid = frame.meta.id;
            let mut file = self.file.write();
            if dirty {
                file.spill(wal, frame.meta, frame.buffer)?;
            }
            *frame.meta = Self::fetch_page(&mut file, pgid, frame.buffer)?;
            internal.page_to_frame.remove(&old_pgid);
            internal.page_to_frame.insert(pgid, frame_id);
            Ok(frame.into())
        }
    }

    fn fetch_page(
        file: &mut FileManager,
        pgid: PageId,
        buff: &mut [u8],
    ) -> anyhow::Result<PageMeta> {
        let found = file.get(pgid, buff)?;
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

    pub(crate) fn alloc(&self, ctx: LogContext, txid: TxId) -> anyhow::Result<PageWrite> {
        log::trace!("alloc {txid:?}");
        let pgid = {
            let mut state = self.state.write();
            state.page_count += 1;
            PageId::new(state.page_count - 1).unwrap()
        };

        let lsn = ctx.record_alloc(txid, pgid)?;

        let mut internal = self.internal.write();
        let mut evictor = self.evictor.lock();
        let frame = if let Some(frame) = self.pool.alloc(txid, PageMeta::init(pgid, lsn)) {
            evictor.acquired(frame.index);
            internal.page_to_frame.insert(pgid, frame.index);
            frame
        } else {
            let (frame_id, dirty) = evictor.evict_and_acquire()?;
            let frame = self.pool.write(txid, frame_id);
            let old_pgid = frame.meta.id;
            if dirty {
                let mut file = self.file.write();
                file.spill(&ctx, frame.meta, frame.buffer)?;
            }
            *frame.meta = PageMeta::init(pgid, lsn);
            internal.page_to_frame.remove(&old_pgid);
            internal.page_to_frame.insert(pgid, frame_id);
            frame
        };

        Ok(PageWrite { pager: self, frame })
    }

    pub(crate) fn dealloc(
        &self,
        ctx: LogContext,
        txid: TxId,
        pgid: PageId,
    ) -> anyhow::Result<PageWrite> {
        let page = self.write(&ctx, txid, pgid)?;
        page.frame.meta.lsn = ctx.record_dealloc(txid, pgid)?;
        page.frame.meta.dirty = true;
        page.frame.meta.kind = PageKind::None;
        Ok(page)
    }

    pub(crate) fn set_state<F>(&self, ctx: LogContext, f: F) -> anyhow::Result<()>
    where
        F: FnOnce(&mut DbState),
    {
        let mut state = self.state.write();
        let current_state = *state;
        f(&mut state);
        ctx.record_set_state(
            state.root,
            current_state.root,
            state.freelist,
            current_state.freelist,
            state.page_count,
            current_state.page_count,
        )?;
        Ok(())
    }

    pub(crate) fn read_state(&self) -> RwLockReadGuard<DbState> {
        self.state.read()
    }

    // Note: unlike the original aries design where the flushing process and checkpoint are
    // considered different component, this DB combines them together. During checkpoint, all
    // dirty pages are flushed. This makes the checkpoint process longer, but simpler. We also
    // don't need checkpoint-end log record.
    pub(crate) fn checkpoint(&self, wal: &Wal) -> anyhow::Result<()> {
        let mut first_unflushed = wal.first_unflushed();
        for item in self.pool.walk() {
            let should_flush = item.meta.lsn >= first_unflushed;
            if should_flush {
                let new_first_unflushed = wal.sync(item.meta.lsn)?;
                first_unflushed = new_first_unflushed;
            }

            let mut file = self.file.write();
            file.spill(wal, item.meta, item.buffer)?;
        }

        let mut file = self.file.write();
        file.sync(wal)?;
        Ok(())
    }

    fn release(&self, frame_id: usize, is_dirty: bool) {
        log::trace!("release frame_id={frame_id} is_dirty={is_dirty}");
        self.evictor.lock().released(frame_id, is_dirty);
    }

    pub(crate) fn shutdown(self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Default, Clone, Copy, Debug)]
pub(crate) struct DbState {
    pub(crate) root: Option<PageId>,
    pub(crate) freelist: Option<PageId>,
    pub(crate) page_count: u64,
}

pub(crate) trait BufferPoolFrame<'a> {
    fn get(pool: &'a BufferPool, txid: TxId, index: usize) -> Self;
}

impl<'a> BufferPoolFrame<'a> for ReadFrame<'a> {
    fn get(pool: &'a BufferPool, _txid: TxId, index: usize) -> Self {
        pool.read(index)
    }
}

impl<'a> BufferPoolFrame<'a> for WriteFrame<'a> {
    fn get(pool: &'a BufferPool, txid: TxId, index: usize) -> Self {
        pool.write(txid, index)
    }
}

pub(crate) struct PageRead<'a> {
    pub(super) pager: &'a Pager,
    pub(super) frame: ReadFrame<'a>,
}

impl<'a> Drop for PageRead<'a> {
    fn drop(&mut self) {
        self.pager.release(self.frame.index, self.frame.meta.dirty);
    }
}

impl<'a> PageOps<'a> for PageRead<'a> {
    #[inline]
    fn internal(&self) -> PageInternal {
        PageInternal {
            meta: self.frame.meta,
            buffer: self.frame.buffer,
        }
    }
}

pub(crate) struct PageWrite<'a> {
    pub(super) pager: &'a Pager,
    pub(super) frame: WriteFrame<'a>,
}

impl<'a> std::fmt::Debug for PageWrite<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.frame.fmt(f)
    }
}

impl<'a> PageOps<'a> for PageWrite<'a> {
    #[inline]
    fn internal(&self) -> PageInternal {
        PageInternal {
            meta: self.frame.meta,
            buffer: self.frame.buffer,
        }
    }
}

impl<'a> PageWriteOps<'a> for PageWrite<'a> {
    #[inline]
    fn internal_mut(&mut self) -> PageInternalWrite {
        PageInternalWrite {
            txid: self.frame.txid,
            meta: self.frame.meta,
            buffer: self.frame.buffer,
        }
    }
}

impl<'a> Drop for PageWrite<'a> {
    fn drop(&mut self) {
        self.pager.release(self.frame.index, self.frame.meta.dirty);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::Bytes;

    struct NoopWalSync;

    impl WalSync for NoopWalSync {
        fn sync(&self, _lsn: Lsn) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let pager = Pager::new(dir.path(), 512, 10).unwrap();
        let txid = TxId::new(1).unwrap();

        for i in 0..20 {
            let ctx = LogContext::Redo(Lsn::new(1));
            let page = pager.alloc(ctx, txid).unwrap();
            assert_eq!(i, page.id().get());
            let mut leaf = page.init_leaf(ctx).unwrap();
            for j in 0..5 {
                leaf.insert_content(ctx, j, &mut Bytes::new(b"abc"), 3, 0, None)
                    .unwrap();
            }
            leaf.set_next(ctx, PageId::new(5)).unwrap();
        }

        for i in (0..20).rev() {
            let page = pager
                .write(&NoopWalSync, txid, PageId::new(i).unwrap())
                .unwrap();
            let ctx = LogContext::Redo(Lsn::new(1));
            let mut leaf = page.into_write_leaf().unwrap();
            leaf.set_next(ctx, None).unwrap();
            for j in (0..5).rev() {
                leaf.delete(ctx, j).unwrap();
            }
            leaf.reset(ctx).unwrap();
        }
    }
}
