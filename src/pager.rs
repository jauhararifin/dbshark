use crate::bins::SliceExt;
use crate::content::{Bytes, Content};
use crate::id::{Lsn, LsnExt, PageId, PageIdExt, TxId};
use crate::log::{TxState, WalEntry, WalKind};
use crate::wal::Wal;
use anyhow::anyhow;
use indexmap::IndexSet;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;

// TODO: use better eviction policy. De-priorize pages with `page_lsn` < `flushed_lsn`.
// Also use better algorithm such as tiny-lfu or secondchance.

#[derive(Clone, Copy)]
pub(crate) enum LogContext<'a> {
    Runtime(&'a Wal),
    Redo(Lsn),
    Undo(&'a Wal, Lsn),
}

impl LogContext<'_> {
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
}

pub(crate) struct Pager {
    f: Mutex<File>,
    double_buff_f: Mutex<File>,
    page_size: usize,
    n: usize,

    internal: RwLock<PagerInternal>,
    flush_internal: RwLock<PagerFlushInternal>,
}

pub(crate) struct DbState {
    pub(crate) root: Option<PageId>,
    pub(crate) freelist: Option<PageId>,
    pub(crate) page_count: u64,
}

impl Default for DbState {
    fn default() -> Self {
        Self {
            root: None,
            freelist: None,
            page_count: 1,
        }
    }
}

pub(crate) struct PagerInternal {
    allocated: usize,
    db_state: DbState,
    metas: *mut RwLock<PageMeta>,
    buffer: *mut u8,
    ref_count: Box<[usize]>,
    page_to_frame: HashMap<PageId, usize>,
    free_frames: HashSet<usize>,
    dirty_frames: HashSet<usize>,
    free_and_clean: HashSet<usize>,
}

struct PagerFlushInternal {
    // TODO: maybe add assert to check if it's not possible that a same page
    // is stored in both flushing_pages and buffer pool.
    flushing_pages: Box<[u8]>,
    flushing_pgids: IndexSet<PageId>,
}

const MINIMUM_PAGE_SIZE: usize = 256;
pub(crate) const MAXIMUM_PAGE_SIZE: usize = 0x4000;

const PAGE_HEADER_SIZE: usize = 24;
const PAGE_HEADER_VERSION_RANGE: Range<usize> = 0..2;
const PAGE_HEADER_KIND_INDEX: usize = 2;
const PAGE_HEADER_PAGE_LSN_RANGE: Range<usize> = 8..16;
const PAGE_HEADER_PAGE_ID_RANGE: Range<usize> = 16..24;

const PAGE_FOOTER_SIZE: usize = 8;
const PAGE_FOOTER_CHECKSUM_RANGE: Range<usize> = 0..8;

const INTERIOR_PAGE_HEADER_SIZE: usize = 16;
const INTERIOR_HEADER_LAST_RANGE: Range<usize> = 0..8;
const INTERIOR_HEADER_COUNT_RANGE: Range<usize> = 8..10;
const INTERIOR_HEADER_OFFSET_RANGE: Range<usize> = 10..12;

const INTERIOR_CELL_SIZE: usize = 24;
const INTERIOR_CELL_PTR_RANGE: Range<usize> = 0..8;
const INTERIOR_CELL_OVERFLOW_RANGE: Range<usize> = 8..16;
const INTERIOR_CELL_KEY_SIZE_RANGE: Range<usize> = 16..20;
const INTERIOR_CELL_OFFSET_RANGE: Range<usize> = 20..22;
const INTERIOR_CELL_SIZE_RANGE: Range<usize> = 22..24;

const LEAF_PAGE_HEADER_SIZE: usize = 16;
const LEAF_HEADER_NEXT_RANGE: Range<usize> = 0..8;
const LEAF_HEADER_COUNT_RANGE: Range<usize> = 8..10;
const LEAF_HEADER_OFFSET_RANGE: Range<usize> = 10..12;

const LEAF_CELL_SIZE: usize = 24;
const LEAF_CELL_OVERFLOW_RANGE: Range<usize> = 0..8;
const LEAF_CELL_KEY_SIZE_RANGE: Range<usize> = 8..12;
const LEAF_CELL_VAL_SIZE_RANGE: Range<usize> = 12..16;
const LEAF_CELL_OFFSET_RANGE: Range<usize> = 16..18;
const LEAF_CELL_SIZE_RANGE: Range<usize> = 18..20;

const OVERFLOW_PAGE_HEADER_SIZE: usize = 16;
const OVERFLOW_HEADER_NEXT_RANGE: Range<usize> = 0..8;
const OVERFLOW_HEADER_SIZE_RANGE: Range<usize> = 8..10;

const FREELIST_PAGE_HEADER_SIZE: usize = 16;
const FREELIST_HEADER_NEXT_RANGE: Range<usize> = 0..8;
const FREELIST_HEADER_COUNT_RANGE: Range<usize> = 8..10;

macro_rules! const_assert {
    ($($tt:tt)*) => {
        const _: () = assert!($($tt)*);
    }
}

const_assert!(PAGE_HEADER_VERSION_RANGE.end <= PAGE_HEADER_SIZE);
const_assert!(range_size(PAGE_HEADER_VERSION_RANGE) == 2);
const_assert!(PAGE_HEADER_KIND_INDEX < PAGE_HEADER_SIZE);
const_assert!(PAGE_HEADER_PAGE_LSN_RANGE.end <= PAGE_HEADER_SIZE);
const_assert!(range_size(PAGE_HEADER_PAGE_LSN_RANGE) == 8);
const_assert!(PAGE_HEADER_PAGE_ID_RANGE.end <= PAGE_HEADER_SIZE);
const_assert!(range_size(PAGE_HEADER_PAGE_ID_RANGE) == 8);

const_assert!(PAGE_FOOTER_CHECKSUM_RANGE.end <= PAGE_FOOTER_SIZE);
const_assert!(range_size(PAGE_FOOTER_CHECKSUM_RANGE) == 8);

const_assert!(INTERIOR_HEADER_LAST_RANGE.end <= INTERIOR_PAGE_HEADER_SIZE);
const_assert!(range_size(INTERIOR_HEADER_LAST_RANGE) == 8);
const_assert!(INTERIOR_HEADER_COUNT_RANGE.end <= INTERIOR_PAGE_HEADER_SIZE);
const_assert!(range_size(INTERIOR_HEADER_COUNT_RANGE) == 2);
const_assert!(INTERIOR_HEADER_OFFSET_RANGE.end <= INTERIOR_PAGE_HEADER_SIZE);
const_assert!(range_size(INTERIOR_HEADER_OFFSET_RANGE) == 2);

const_assert!(INTERIOR_CELL_OVERFLOW_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_OVERFLOW_RANGE) == 8);
const_assert!(INTERIOR_CELL_PTR_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_PTR_RANGE) == 8);
const_assert!(INTERIOR_CELL_KEY_SIZE_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_KEY_SIZE_RANGE) == 4);
const_assert!(INTERIOR_CELL_OFFSET_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_OFFSET_RANGE) == 2);
const_assert!(INTERIOR_CELL_SIZE_RANGE.end <= INTERIOR_CELL_SIZE);
const_assert!(range_size(INTERIOR_CELL_SIZE_RANGE) == 2);

const_assert!(LEAF_CELL_OVERFLOW_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_CELL_OVERFLOW_RANGE) == 8);
const_assert!(LEAF_CELL_KEY_SIZE_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_CELL_KEY_SIZE_RANGE) == 4);
const_assert!(LEAF_CELL_VAL_SIZE_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_CELL_VAL_SIZE_RANGE) == 4);
const_assert!(LEAF_HEADER_NEXT_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_HEADER_NEXT_RANGE) == 8);
const_assert!(LEAF_HEADER_COUNT_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_HEADER_COUNT_RANGE) == 2);
const_assert!(LEAF_HEADER_OFFSET_RANGE.end <= LEAF_PAGE_HEADER_SIZE);
const_assert!(range_size(LEAF_HEADER_OFFSET_RANGE) == 2);

const_assert!(LEAF_CELL_OFFSET_RANGE.end <= LEAF_CELL_SIZE);
const_assert!(range_size(LEAF_CELL_OFFSET_RANGE) == 2);
const_assert!(LEAF_CELL_SIZE_RANGE.end <= LEAF_CELL_SIZE);
const_assert!(range_size(LEAF_CELL_SIZE_RANGE) == 2);

const_assert!(OVERFLOW_HEADER_NEXT_RANGE.end <= OVERFLOW_PAGE_HEADER_SIZE);
const_assert!(range_size(OVERFLOW_HEADER_NEXT_RANGE) == 8);
const_assert!(OVERFLOW_HEADER_SIZE_RANGE.end <= OVERFLOW_PAGE_HEADER_SIZE);
const_assert!(range_size(OVERFLOW_HEADER_SIZE_RANGE) == 2);

const_assert!(FREELIST_HEADER_NEXT_RANGE.end <= FREELIST_PAGE_HEADER_SIZE);
const_assert!(range_size(FREELIST_HEADER_NEXT_RANGE) == 8);
const_assert!(FREELIST_HEADER_COUNT_RANGE.end <= FREELIST_PAGE_HEADER_SIZE);
const_assert!(range_size(FREELIST_HEADER_COUNT_RANGE) == 2);

#[allow(dead_code)]
const fn range_size(range: Range<usize>) -> usize {
    range.end - range.start
}

unsafe impl Send for PagerInternal {}
unsafe impl Sync for PagerInternal {}

impl Drop for Pager {
    fn drop(&mut self) {
        let internal = self.internal.write();
        unsafe {
            drop(Vec::from_raw_parts(
                internal.buffer,
                self.page_size * self.n,
                self.page_size * self.n,
            ));
            drop(Box::from_raw(std::slice::from_raw_parts_mut(
                internal.metas,
                self.n,
            )));
        }
    }
}

impl Pager {
    pub(crate) fn new(
        mut f: File,
        mut double_buff_f: File,
        page_size: usize,
        n: usize,
    ) -> anyhow::Result<Self> {
        Self::check_page_size(page_size)?;

        if n < 10 {
            return Err(anyhow!(
                "number of pages must be at least 10, but got {}",
                n
            ));
        }

        Self::recover_non_atomic_write(page_size, &mut f, &mut double_buff_f)?;

        let Some(buffer_size) = page_size.checked_mul(n) else {
            return Err(anyhow!("page size * n overflows: {} * {}", page_size, n));
        };

        let dummy_pgid = PageId::new(1).unwrap();
        let metas = (0..n)
            .map(|_| {
                RwLock::new(PageMeta {
                    id: dummy_pgid,
                    kind: PageKind::None,
                    lsn: None,
                    is_dirty: false,
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let flushing_area_n = 10;
        let flushing_pages = vec![0u8; page_size * flushing_area_n].into_boxed_slice();

        Ok(Self {
            f: Mutex::new(f),
            double_buff_f: Mutex::new(double_buff_f),
            page_size,
            n,

            internal: RwLock::new(PagerInternal {
                allocated: 0,
                db_state: DbState::default(),
                metas: Box::leak(metas).as_mut_ptr(),
                buffer: vec![0u8; buffer_size].leak().as_mut_ptr(),
                ref_count: vec![0; n].into_boxed_slice(),
                page_to_frame: HashMap::default(),
                free_frames: HashSet::default(),
                dirty_frames: HashSet::default(),
                free_and_clean: HashSet::default(),
            }),
            flush_internal: RwLock::new(PagerFlushInternal {
                flushing_pages,
                flushing_pgids: IndexSet::default(),
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

    fn recover_non_atomic_write(
        page_size: usize,
        file: &mut File,
        double_buff_file: &mut File,
    ) -> anyhow::Result<()> {
        let size = double_buff_file.metadata()?.len();
        let page_count = (size as usize) / page_size;

        let mut buff = vec![0u8; page_size * page_count];
        double_buff_file.seek(SeekFrom::Start(0))?;
        double_buff_file.read_exact(&mut buff)?;

        for i in 0..page_count {
            let buff = &mut buff[i * page_size..(i + 1) * page_size];
            let dummy_pgid = PageId::new(1).unwrap();
            let mut meta = PageMeta {
                id: dummy_pgid,
                kind: PageKind::None,
                lsn: None,
                is_dirty: false,
            };

            let ok = Self::decode_internal(page_size, &mut meta, buff)?;
            if ok {
                write_page(file, meta.id, page_size, buff)?;
            }
        }

        file.sync_all()?;
        Ok(())
    }

    pub(crate) fn set_db_state(
        &self,
        ctx: LogContext<'_>,
        db_state: DbState,
    ) -> anyhow::Result<()> {
        let mut internal = self.internal.write();
        let (old_root, old_freelist, old_page_count) = {
            (
                internal.db_state.root,
                internal.db_state.freelist,
                internal.db_state.page_count,
            )
        };

        record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::HeaderSet {
                    root: db_state.root,
                    freelist: db_state.freelist,
                    page_count: db_state.page_count,
                    old_root,
                    old_freelist,
                    old_page_count,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::HeaderSet {
                    root: db_state.root,
                    freelist: db_state.freelist,
                    page_count: db_state.page_count,
                    old_root,
                    old_freelist,
                    old_page_count,
                },
            },
        )?;

        internal.db_state = db_state;

        Ok(())
    }

    pub(crate) fn root(&self) -> Option<PageId> {
        self.internal.read().db_state.root
    }

    pub(crate) fn freelist(&self) -> Option<PageId> {
        self.internal.read().db_state.freelist
    }

    pub(crate) fn page_count(&self) -> u64 {
        self.internal.read().db_state.page_count
    }

    // TODO: consider using read lock for fast path. Most of the time, acquiring a page
    // doesn't need to mutate the buffer pool if the page is already in the pool.
    // We only need to fallback to write lock when the a page need to be evicted or
    // fetched. We can use atomic integer to increase and decrease reference count.
    // TODO: maybe, we don't even need a read vs write lock for a page since there can only
    // be one write mutation at a time. We might give the `&mut Pager` to the write txn
    // alltogether.
    pub(crate) fn read(&self, pgid: PageId) -> anyhow::Result<PageRead> {
        let internal = self.internal.write();
        assert!(
            pgid.get() < internal.db_state.page_count,
            "page {:?} is out of bound for reading since page_count={}",
            pgid,
            internal.db_state.page_count
        );
        let (frame_id, meta, buffer) = self.acquire(internal, pgid, None)?;

        let meta = meta.read();
        // SAFETY: it's guaranteed that buffer has only one mutable reference or multiple
        // shared reference since it's protected by page meta's lock.
        let buffer = unsafe { std::slice::from_raw_parts(buffer, self.page_size) };

        Ok(PageRead {
            pager: self,
            frame_id,
            meta,
            buffer,
        })
    }

    pub(crate) fn write(&self, txid: TxId, pgid: PageId) -> anyhow::Result<PageWrite> {
        let internal = self.internal.write();
        assert!(
            pgid.get() < internal.db_state.page_count,
            "page {:?} is out of bound for writing since page_count={}",
            pgid,
            internal.db_state.page_count
        );
        let (frame_id, meta, buffer) = self.acquire(internal, pgid, None)?;
        let meta = meta.write();
        // SAFETY: it's guaranteed that buffer has only one mutable reference or multiple
        // shared reference since it's protected by page meta's lock.
        let buffer = unsafe { std::slice::from_raw_parts_mut(buffer, self.page_size) };
        Ok(PageWrite {
            pager: self,
            frame_id,
            txid,
            meta,
            buffer,
        })
    }

    pub(crate) fn alloc_for_redo(
        &self,
        txid: TxId,
        lsn: Lsn,
        pgid: PageId,
    ) -> anyhow::Result<PageWrite> {
        let mut internal = self.internal.write();
        assert!(
            pgid.get() <= internal.db_state.page_count,
            "page {:?} is out of bound for redoing alloc since page_count={}",
            pgid,
            internal.db_state.page_count
        );
        if pgid.get() == internal.db_state.page_count {
            internal.db_state.page_count += 1;
        }
        let (frame_id, meta, buffer) = self.acquire(internal, pgid, Some(lsn))?;
        let meta = meta.write();
        // SAFETY: it's guaranteed that buffer has only one mutable reference or multiple
        // shared reference since it's protected by page meta's lock.
        let buffer = unsafe { std::slice::from_raw_parts_mut(buffer, self.page_size) };
        Ok(PageWrite {
            pager: self,
            frame_id,
            txid,
            meta,
            buffer,
        })
    }

    pub(crate) fn alloc(&self, ctx: LogContext<'_>, txid: TxId) -> anyhow::Result<PageWrite> {
        let mut internal = self.internal.write();
        let pgid = {
            internal.db_state.page_count += 1;
            PageId::new(internal.db_state.page_count - 1).unwrap()
        };

        let lsn = record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::AllocPage { txid, pgid },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::AllocPage { txid, pgid },
            },
        )?;

        let (frame_id, meta, buffer) = self.acquire(internal, pgid, Some(lsn))?;
        let meta = meta.write();
        // SAFETY: it's guaranteed that buffer has only one mutable reference or multiple
        // shared reference since it's protected by page meta's lock.
        let buffer = unsafe { std::slice::from_raw_parts_mut(buffer, self.page_size) };
        Ok(PageWrite {
            pager: self,
            frame_id,
            txid,
            meta,
            buffer,
        })
    }

    pub(crate) fn dealloc(
        &self,
        ctx: LogContext<'_>,
        txid: TxId,
        pgid: PageId,
    ) -> anyhow::Result<()> {
        let mut page = self.write(txid, pgid)?;
        page.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::DeallocPage { txid, pgid },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::DeallocPage { txid, pgid },
            },
        )?);
        page.meta.kind = PageKind::None;
        page.meta.is_dirty = true;
        Ok(())
    }

    fn acquire(
        &self,
        mut internal: RwLockWriteGuard<PagerInternal>,
        pgid: PageId,
        alloc_lsn: Option<Lsn>,
    ) -> anyhow::Result<(usize, &RwLock<PageMeta>, *mut u8)> {
        assert!(
            pgid.get() <= internal.db_state.page_count,
            "page {:?} is out of bound when acquiring page since page_count={}",
            pgid,
            internal.db_state.page_count
        );

        if let Some(frame_id) = internal.page_to_frame.get(&pgid).copied() {
            internal.ref_count[frame_id] += 1;
            internal.free_frames.remove(&frame_id);
            internal.free_and_clean.remove(&frame_id);

            // SAFETY:
            // - it's guaranteed that the address pointed by metas + frame_id is valid
            // - it's guaranteed that there are only shared reference to the meta since we
            //   never make a mutable reference of it, except when dropping the pager
            let meta = unsafe { &*internal.metas.add(frame_id) };

            let offset = frame_id * self.page_size;
            // SAFETY: it's guaranteed that the resulting address is inside internal.buffer
            let buffer = unsafe { internal.buffer.add(offset) };

            Ok((frame_id, meta, buffer))
        } else if internal.allocated < self.n {
            let frame_id = internal.allocated;

            // SAFETY:
            // - it's guaranteed that the address pointed by metas + frame_id is valid
            // - it's guaranteed that there are only shared reference to the meta since we
            //   never make a mutable reference of it, except when dropping the pager
            let meta = unsafe { &*internal.metas.add(frame_id) };
            // It's ok to acquire exclusive lock here when `internal`'s lock is held because
            // acquiring the meta's lock here is instant. It's guaranteed that the lock is not
            // currently held by other thread.
            let mut meta_locked = meta.write();
            let offset = frame_id * self.page_size;
            // SAFETY: it's guaranteed that the buffer + offset is pointed to valid address.
            let buffer_offset = unsafe { internal.buffer.add(offset) };
            // SAFETY: it's guaranteed that buffer has only one mutable reference or multiple
            // shared reference since it's protected by page meta's lock.
            let buffer = unsafe { std::slice::from_raw_parts_mut(buffer_offset, self.page_size) };

            if let Some(lsn) = alloc_lsn {
                *meta_locked = PageMeta {
                    id: pgid,
                    kind: PageKind::None,
                    lsn: Some(lsn),
                    is_dirty: false,
                };
            } else {
                self.decode(pgid, &mut meta_locked, buffer)?;
            }

            internal.allocated += 1;
            internal.ref_count[frame_id] += 1;
            internal.page_to_frame.insert(pgid, frame_id);

            Ok((frame_id, meta, buffer_offset))
        } else {
            let (frame_id, evicted) = Self::get_eviction_candidate(&mut internal)?;

            // SAFETY:
            // - it's guaranteed that the address pointed by metas + frame_id is valid
            // - it's guaranteed that there are only shared reference to the meta since we
            //   never make a mutable reference of it, except when dropping the pager
            let meta = unsafe { &*internal.metas.add(frame_id) };

            let old_pgid = {
                // It's ok to acquire shared lock here when `internal`'s lock is held because
                // acquiring the meta's lock here is instant since only free page can be evicted.
                meta.read().id
            };

            // It's ok to acquire exclusive lock here when `internal`'s lock is held because
            // acquiring the meta's lock here is instant since only free page can be evicted.
            let mut meta_locked = meta.write();
            let offset = frame_id * self.page_size;
            // SAFETY: it's guaranteed that the buffer + offset is pointed to valid address.
            let buffer_offset = unsafe { internal.buffer.add(offset) };
            // SAFETY: it's guaranteed that buffer has only one mutable reference or multiple
            // shared reference since it's protected by page meta's lock.
            let buffer = unsafe { std::slice::from_raw_parts_mut(buffer_offset, self.page_size) };

            if evicted {
                Self::encode(&meta_locked, buffer)?;

                let mut flush_internal = self.flush_internal.write();
                Self::evict_page(
                    &mut flush_internal,
                    &self.f,
                    &self.double_buff_f,
                    self.page_size,
                    old_pgid,
                    buffer,
                )?;
                drop(flush_internal);
            }

            if let Some(lsn) = alloc_lsn {
                *meta_locked = PageMeta {
                    id: pgid,
                    kind: PageKind::None,
                    lsn: Some(lsn),
                    is_dirty: false,
                };
            } else {
                self.decode(pgid, &mut meta_locked, buffer)?;
            }

            internal.free_and_clean.remove(&frame_id);
            internal.free_frames.remove(&frame_id);

            internal.page_to_frame.remove(&old_pgid);
            internal.page_to_frame.insert(pgid, frame_id);
            assert!(internal.ref_count[frame_id] == 0);
            internal.ref_count[frame_id] += 1;
            internal.free_frames.remove(&frame_id);
            internal.free_and_clean.remove(&frame_id);
            internal.dirty_frames.remove(&frame_id);

            Ok((frame_id, meta, buffer_offset))
        }
    }

    fn get_eviction_candidate(internal: &mut PagerInternal) -> anyhow::Result<(usize, bool)> {
        let (pgid, ok) = if let Some(frame_id) = internal.free_and_clean.iter().next().copied() {
            (frame_id, false)
        } else if let Some(frame_id) = internal.free_frames.iter().next().copied() {
            (frame_id, true)
        } else {
            // TODO: consider sleep and retry this process
            return Err(anyhow!("all pages are pinned"));
        };

        Ok((pgid, ok))
    }

    fn flush_page(
        f: &Mutex<File>,
        double_buff_f: &Mutex<File>,
        pgid: PageId,
        buffer: &[u8],
    ) -> anyhow::Result<()> {
        // TODO: reduce the number of syscall here.
        let mut double_buff_f = double_buff_f.lock();
        double_buff_f.seek(SeekFrom::Start(0))?;
        double_buff_f.write_all(buffer)?;
        double_buff_f.sync_all()?;
        drop(double_buff_f);

        let mut f = f.lock();
        let page_size = buffer.len();
        write_page(&mut f, pgid, page_size, buffer)?;
        f.sync_all()?;
        drop(f);

        Ok(())
    }

    fn evict_page(
        internal: &mut PagerFlushInternal,
        f: &Mutex<File>,
        double_buff_f: &Mutex<File>,
        page_size: usize,
        pgid: PageId,
        buffer: &[u8],
    ) -> anyhow::Result<()> {
        if let Some((i, _)) = internal.flushing_pgids.get_full(&pgid) {
            internal.flushing_pages[i * page_size..(i + 1) * page_size].copy_from_slice(buffer);
        } else {
            let is_full =
                internal.flushing_pgids.len() * page_size >= internal.flushing_pages.len();
            if is_full {
                Self::flush_and_clear_flushing_pages(internal, f, double_buff_f, page_size)?;
            }

            let i = internal.flushing_pgids.len();
            internal.flushing_pgids.insert(pgid);
            internal.flushing_pages[i * page_size..(i + 1) * page_size].copy_from_slice(buffer);
        }

        Ok(())
    }

    fn flush_and_clear_flushing_pages(
        internal: &mut PagerFlushInternal,
        f: &Mutex<File>,
        double_buff_f: &Mutex<File>,
        page_size: usize,
    ) -> anyhow::Result<()> {
        let mut double_buff_f = double_buff_f.lock();
        double_buff_f.set_len(0)?;
        double_buff_f.seek(SeekFrom::Start(0))?;
        double_buff_f.write_all(&internal.flushing_pages)?;
        double_buff_f.sync_all()?;
        drop(double_buff_f);

        // TODO: maybe we can use vectorized write to write them all in one single syscall
        // TODO: also, we need to make sure that the WAL log is already written safely before we can
        // write to disk.
        let mut f = f.lock();
        for (i, pgid) in internal.flushing_pgids.iter().enumerate() {
            write_page(
                &mut f,
                *pgid,
                page_size,
                &internal.flushing_pages[i * page_size..(i + 1) * page_size],
            )?;
        }
        f.sync_all()?;
        drop(f);

        internal.flushing_pgids.clear();

        Ok(())
    }

    // Note: unlike the original aries design where the flushing process and checkpoint are
    // considered different component, this DB combines them together. During checkpoint, all
    // dirty pages are flushed. This makes the checkpoint process longer, but simpler. We also
    // don't need checkpoint-end log record.
    pub(crate) fn checkpoint(
        &self,
        wal: &Wal,
        tx_state: impl std::ops::Deref<Target = TxState>,
    ) -> anyhow::Result<()> {
        let internal = self.internal.write();
        let checkpoint_lsn = {
            // It's ok to append WAL while holding tx_state's lock since it's unlikely that the WAL
            // will block.
            wal.append_log(WalEntry {
                clr: None,
                kind: WalKind::Checkpoint {
                    active_tx: *tx_state.deref(),
                    root: internal.db_state.root,
                    freelist: internal.db_state.freelist,
                    page_count: internal.db_state.page_count,
                },
            })?
        };
        drop(internal);

        for frame_id in 0..self.n {
            let (meta, buffer) = {
                let internal = self.internal.write();
                if frame_id >= internal.allocated {
                    continue;
                }

                // SAFETY:
                // - it's guaranteed that the address pointed by metas + frame_id is valid
                // - it's guaranteed that there are only shared reference to the meta since we
                //   never make a mutable reference of it, except when dropping the pager
                let meta = unsafe { &*internal.metas.add(frame_id) };

                let offset = frame_id * self.page_size;
                // SAFETY: it's guaranteed that the buffer + offset is pointed to valid address.
                let buffer_offset = unsafe { internal.buffer.add(offset) };
                // SAFETY: it's guaranteed that buffer has only one mutable reference or multiple
                // shared reference since it's protected by page meta's lock.
                let buffer =
                    unsafe { std::slice::from_raw_parts_mut(buffer_offset, self.page_size) };

                (meta, buffer)
            };

            // TODO: maybe acquire read lock first and skip it if it's already clean.
            // only when it's dirty, we acquire write lock and flush it.
            // TODO: maybe we can batch few pages together to reduce the number of syscall.
            let frame = meta.read();
            if let Some(lsn) = frame.lsn {
                wal.sync(lsn)?;
            }
            Self::encode(&frame, buffer)?;
            Self::flush_page(&self.f, &self.double_buff_f, frame.id, buffer)?;
        }

        wal.complete_checkpoint(checkpoint_lsn)?;

        Ok(())
    }

    fn decode(&self, pgid: PageId, meta: &mut PageMeta, buff: &mut [u8]) -> anyhow::Result<()> {
        assert!(buff.len() == self.page_size);
        meta.id = pgid;

        let ok = {
            let flush_internal = self.flush_internal.read();
            if let Some((i, _)) = flush_internal.flushing_pgids.get_full(&pgid) {
                buff.copy_from_slice(
                    &flush_internal.flushing_pages[i * self.page_size..(i + 1) * self.page_size],
                );
                true
            } else {
                drop(flush_internal);
                let mut f = self.f.lock();
                read_page(&mut f, pgid, self.page_size, buff)?
            }
        };
        if ok {
            let ok = Self::decode_internal(self.page_size, meta, buff)?;
            if !ok {
                meta.kind = PageKind::None;
                meta.lsn = None;
                meta.is_dirty = false;
            }
        } else {
            meta.kind = PageKind::None;
            meta.lsn = None;
            meta.is_dirty = false;
        }

        if meta.id != pgid {
            return Err(anyhow!(
                "page {} is written with invalid pgid information {}",
                pgid.get(),
                meta.id.get(),
            ));
        }

        log::debug!(
            "decode page pgid={:?} kind={:?} lsn={:?}",
            meta.id,
            meta.kind,
            meta.lsn
        );

        Ok(())
    }

    fn decode_internal(
        page_size: usize,
        meta: &mut PageMeta,
        buff: &mut [u8],
    ) -> anyhow::Result<bool> {
        assert!(page_size.count_ones() == 1 && page_size >= MINIMUM_PAGE_SIZE);

        let header = &buff[..PAGE_HEADER_SIZE];
        let footer = &buff[page_size - PAGE_FOOTER_SIZE..];
        let payload = &buff[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE];

        let buff_checksum = &footer[PAGE_FOOTER_CHECKSUM_RANGE];
        let buff_version = &header[PAGE_HEADER_VERSION_RANGE];
        let buff_kind = &header[PAGE_HEADER_KIND_INDEX];
        let buff_page_lsn = &header[PAGE_HEADER_PAGE_LSN_RANGE];
        let buff_page_id = &header[PAGE_HEADER_PAGE_ID_RANGE];
        let buff_checksum_content = &buff[..page_size - PAGE_FOOTER_SIZE];

        let checksum = crc64::crc64(0x1d0f, buff_checksum_content);
        let page_sum = buff_checksum.read_u64();
        if checksum != page_sum {
            return Ok(false);
        }
        let version = buff_version.read_u16();
        if version != 0 {
            return Err(anyhow!("page version {} is not supported", version));
        }

        let Some(page_lsn) = Lsn::from_be_bytes(buff_page_lsn.try_into().unwrap()) else {
            return Err(anyhow!("found an empty lsn field when decoding page",));
        };
        let Some(page_id) = PageId::from_be_bytes(buff_page_id.try_into().unwrap()) else {
            return Err(anyhow!("found an empty page_id field when decoding page",));
        };

        let kind = match buff_kind {
            0 => PageKind::None,
            1 => Self::decode_interior_page(payload)?,
            2 => Self::decode_leaf_page(payload)?,
            3 => Self::decode_overflow_page(buff)?,
            4 => Self::decode_freelist_page(buff)?,
            _ => return Err(anyhow!("page kind {buff_kind} is not recognized")),
        };
        meta.id = page_id;
        meta.kind = kind;
        meta.lsn = Some(page_lsn);

        Ok(true)
    }

    fn decode_interior_page(payload: &[u8]) -> anyhow::Result<PageKind> {
        let header = &payload[..INTERIOR_PAGE_HEADER_SIZE];
        let buff_last = &header[INTERIOR_HEADER_LAST_RANGE];
        let buff_count = &header[INTERIOR_HEADER_COUNT_RANGE];
        let buff_offset = &header[INTERIOR_HEADER_OFFSET_RANGE];

        let Some(last) = PageId::from_be_bytes(buff_last.try_into().unwrap()) else {
            return Err(anyhow!("got zero last ptr on interior page"));
        };
        let count = buff_count.read_u16();
        let offset = buff_offset.read_u16();

        let mut remaining = payload.len() - INTERIOR_PAGE_HEADER_SIZE;
        for i in 0..count {
            let cell_offset = INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * i as usize;
            let buf =
                PageId::from_be_bytes(payload[cell_offset..cell_offset + 8].try_into().unwrap());
            if buf.is_none() {
                return Err(anyhow!("got zero ptr on interior page"));
            }

            let cell = get_interior_cell(payload, i as usize);
            remaining -= INTERIOR_CELL_SIZE + cell.raw().len();
        }

        Ok(PageKind::Interior {
            count: count as usize,
            offset: offset as usize,
            remaining,
            last,
        })
    }

    fn decode_leaf_page(payload: &[u8]) -> anyhow::Result<PageKind> {
        let header = &payload[..LEAF_PAGE_HEADER_SIZE];
        let buff_next = &header[LEAF_HEADER_NEXT_RANGE];
        let buff_count = &header[LEAF_HEADER_COUNT_RANGE];
        let buff_offset = &header[LEAF_HEADER_OFFSET_RANGE];

        let next = PageId::from_be_bytes(buff_next.try_into().unwrap());
        let count = buff_count.read_u16();
        let offset = buff_offset.read_u16();

        let mut remaining = payload.len() - LEAF_PAGE_HEADER_SIZE;
        for i in 0..count {
            let cell = get_leaf_cell(payload, i as usize);
            remaining -= LEAF_CELL_SIZE + cell.raw().len();
        }

        Ok(PageKind::Leaf {
            count: count as usize,
            offset: offset as usize,
            remaining,
            next,
        })
    }

    fn decode_overflow_page(buff: &[u8]) -> anyhow::Result<PageKind> {
        let page_size = buff.len();

        let payload = &buff[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE];
        let header = &payload[..OVERFLOW_PAGE_HEADER_SIZE];
        let buff_next = &header[OVERFLOW_HEADER_NEXT_RANGE];
        let buff_size = &header[OVERFLOW_HEADER_SIZE_RANGE];

        let next = PageId::from_be_bytes(buff_next.try_into().unwrap());
        let size = buff_size.read_u16();

        Ok(PageKind::Overflow {
            next,
            size: size as usize,
        })
    }

    fn decode_freelist_page(buff: &[u8]) -> anyhow::Result<PageKind> {
        let page_size = buff.len();

        let payload = &buff[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE];
        let header = &payload[..FREELIST_PAGE_HEADER_SIZE];
        let buff_next = &header[FREELIST_HEADER_NEXT_RANGE];
        let buff_count = &header[FREELIST_HEADER_COUNT_RANGE];

        let next = PageId::from_be_bytes(buff_next.try_into().unwrap());
        let count = buff_count.read_u16();

        Ok(PageKind::Freelist {
            next,
            count: count as usize,
        })
    }

    fn encode(meta: &PageMeta, buff: &mut [u8]) -> anyhow::Result<()> {
        log::debug!(
            "encode page pgid={:?} kind={:?} lsn={:?}",
            meta.id,
            meta.kind,
            meta.lsn
        );

        let page_size = buff.len();
        let header = &mut buff[..PAGE_HEADER_SIZE];

        header[PAGE_HEADER_VERSION_RANGE].fill(0);

        let kind = match meta.kind {
            PageKind::None => 0,
            PageKind::Interior { .. } => 1,
            PageKind::Leaf { .. } => 2,
            PageKind::Overflow { .. } => 3,
            PageKind::Freelist { .. } => 4,
        };
        header[PAGE_HEADER_KIND_INDEX] = kind;

        header[PAGE_HEADER_PAGE_LSN_RANGE].copy_from_slice(&meta.lsn.to_be_bytes());
        header[PAGE_HEADER_PAGE_ID_RANGE].copy_from_slice(&meta.id.to_be_bytes());

        let payload_buff = &mut buff[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE];
        match &meta.kind {
            PageKind::None => (),
            PageKind::Interior {
                count,
                offset,
                last,
                remaining: _,
            } => {
                let header = &mut payload_buff[..INTERIOR_PAGE_HEADER_SIZE];
                header[INTERIOR_HEADER_LAST_RANGE].copy_from_slice(&last.to_be_bytes());
                header[INTERIOR_HEADER_COUNT_RANGE].copy_from_slice(&(*count as u16).to_be_bytes());
                header[INTERIOR_HEADER_OFFSET_RANGE]
                    .copy_from_slice(&(*offset as u16).to_be_bytes());
            }
            PageKind::Leaf {
                count,
                offset,
                next,
                remaining: _,
            } => {
                let header = &mut payload_buff[..LEAF_PAGE_HEADER_SIZE];
                header[LEAF_HEADER_NEXT_RANGE].copy_from_slice(&next.to_be_bytes());
                header[LEAF_HEADER_COUNT_RANGE].copy_from_slice(&(*count as u16).to_be_bytes());
                header[LEAF_HEADER_OFFSET_RANGE].copy_from_slice(&(*offset as u16).to_be_bytes());
            }
            PageKind::Overflow { next, size } => {
                let header = &mut payload_buff[..OVERFLOW_PAGE_HEADER_SIZE];
                header[OVERFLOW_HEADER_NEXT_RANGE].copy_from_slice(&next.to_be_bytes());
                header[OVERFLOW_HEADER_SIZE_RANGE].copy_from_slice(&(*size as u16).to_be_bytes());
            }
            PageKind::Freelist { next, count } => {
                let header = &mut payload_buff[..FREELIST_PAGE_HEADER_SIZE];
                header[FREELIST_HEADER_NEXT_RANGE].copy_from_slice(&next.to_be_bytes());
                header[FREELIST_HEADER_COUNT_RANGE].copy_from_slice(&(*count as u16).to_be_bytes());
            }
        }

        let checksum = crc64::crc64(0x1d0f, &buff[..page_size - PAGE_FOOTER_SIZE]);
        let footer = &mut buff[page_size - PAGE_FOOTER_SIZE..];
        footer[PAGE_FOOTER_CHECKSUM_RANGE].copy_from_slice(&checksum.to_be_bytes());

        Ok(())
    }

    pub(crate) fn shutdown(mut self) -> anyhow::Result<()> {
        let internal = self.internal.get_mut();
        let mut f = self.f.lock();

        for (pgid, frame_id) in internal.page_to_frame.iter() {
            let meta = unsafe { &*internal.metas.add(*frame_id) }.read();
            let offset = *frame_id * self.page_size;
            // SAFETY: we own the pager, of course this is safe
            let buffer = unsafe {
                std::slice::from_raw_parts_mut(internal.buffer.add(offset), self.page_size)
            };
            Self::encode(&meta, buffer)?;
            // TODO: try to seek and write using a single syscall
            // TODO(important): this is not safe. We can get non-atomic write.
            // In order to make the write atomic, we have to do a double-write. Write to a shadow page
            // first, then to the actual page.
            // TODO: also, we need to make sure that the WAL log is already written safely before we can
            // write to disk.
            write_page(&mut f, *pgid, self.page_size, buffer)?;
        }

        // TODO(important): this is not safe. We can get non-atomic write.
        // In order to make the write atomic, we have to do a double-write. Write to a shadow page
        // first, then to the actual page.
        f.sync_all()?;
        Ok(())
    }

    fn release(&self, frame_id: usize, is_dirty: bool) {
        let mut internal = self.internal.write();
        internal.ref_count[frame_id] -= 1;

        let now_free = if internal.ref_count[frame_id] == 0 {
            internal.free_frames.insert(frame_id);
            true
        } else {
            false
        };

        let maybe_clean = if is_dirty {
            internal.dirty_frames.insert(frame_id);
            internal.free_and_clean.remove(&frame_id);
            false
        } else {
            true
        };

        if now_free && maybe_clean {
            let is_clean = !internal.dirty_frames.contains(&frame_id);
            if is_clean {
                internal.free_and_clean.insert(frame_id);
            }
        }
    }
}

#[derive(Debug)]
struct PageMeta {
    id: PageId,
    kind: PageKind,
    lsn: Option<Lsn>,
    is_dirty: bool,
}

#[derive(Debug)]
enum PageKind {
    None,
    Interior {
        count: usize,
        offset: usize,
        remaining: usize,
        last: PageId,
    },
    Leaf {
        count: usize,
        offset: usize,
        remaining: usize,
        next: Option<PageId>,
    },
    Overflow {
        next: Option<PageId>,
        size: usize,
    },
    Freelist {
        next: Option<PageId>,
        count: usize,
    },
}

pub(crate) struct PageRead<'a> {
    pager: &'a Pager,
    frame_id: usize,
    meta: RwLockReadGuard<'a, PageMeta>,
    buffer: &'a [u8],
}

impl<'a> Drop for PageRead<'a> {
    fn drop(&mut self) {
        self.pager.release(self.frame_id, false);
    }
}

impl<'a> PageRead<'a> {
    pub(crate) fn is_none(&self) -> bool {
        matches!(&self.meta.kind, PageKind::None)
    }

    pub(crate) fn is_interior(&self) -> bool {
        matches!(&self.meta.kind, PageKind::Interior { .. })
    }

    pub(crate) fn into_interior(self) -> Option<InteriorPageRead<'a>> {
        if let PageKind::Interior { .. } = &self.meta.kind {
            Some(InteriorPageRead(self))
        } else {
            None
        }
    }

    pub(crate) fn into_leaf(self) -> Option<LeafPageRead<'a>> {
        if let PageKind::Leaf { .. } = &self.meta.kind {
            Some(LeafPageRead(self))
        } else {
            None
        }
    }

    pub(crate) fn into_overflow(self) -> Option<OverflowPageRead<'a>> {
        if let PageKind::Overflow { .. } = &self.meta.kind {
            Some(OverflowPageRead(self))
        } else {
            None
        }
    }
}

pub(crate) struct PageWrite<'a> {
    pager: &'a Pager,
    frame_id: usize,
    txid: TxId,
    meta: RwLockWriteGuard<'a, PageMeta>,
    buffer: &'a mut [u8],
}

impl Drop for PageWrite<'_> {
    fn drop(&mut self) {
        self.pager.release(self.frame_id, self.meta.is_dirty);
    }
}

impl<'a> PageWrite<'a> {
    pub(crate) fn id(&self) -> PageId {
        self.meta.id
    }

    pub(crate) fn is_interior(&self) -> bool {
        matches!(&self.meta.kind, PageKind::Interior { .. })
    }

    pub(crate) fn page_lsn(&self) -> Option<Lsn> {
        self.meta.lsn
    }

    pub(crate) fn init_interior(
        mut self,
        ctx: LogContext<'_>,
        last: PageId,
    ) -> anyhow::Result<Option<InteriorPageWrite<'a>>> {
        if let PageKind::None = self.meta.kind {
            let pgid = self.id();
            self.meta.lsn = Some(record_mutation(
                ctx,
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::InteriorInit {
                        txid: self.txid,
                        pgid,
                        last,
                    },
                },
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::InteriorInit {
                        txid: self.txid,
                        pgid,
                        last,
                    },
                },
            )?);
            self.meta.is_dirty = true;

            self.meta.kind = PageKind::Interior {
                count: 0,
                offset: self.pager.page_size - PAGE_FOOTER_SIZE,
                remaining: self.pager.page_size
                    - PAGE_HEADER_SIZE
                    - PAGE_FOOTER_SIZE
                    - INTERIOR_PAGE_HEADER_SIZE,
                last,
            };
        }

        Ok(self.into_interior())
    }

    pub(crate) fn set_interior(
        mut self,
        ctx: LogContext<'_>,
        payload: &'a [u8],
    ) -> anyhow::Result<InteriorPageWrite<'a>> {
        assert!(
            matches!(self.meta.kind, PageKind::None),
            "page is not empty"
        );

        let pgid = self.id();
        let page_size = self.buffer.len();
        self.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorSet {
                    txid: self.txid,
                    pgid,
                    page_version: 0,
                    payload: Bytes::new(payload),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorSet {
                    txid: self.txid,
                    pgid,
                    page_version: 0,
                    payload: Bytes::new(payload),
                },
            },
        )?);
        self.meta.is_dirty = true;

        self.meta.kind = Pager::decode_interior_page(payload)?;
        self.buffer[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE].copy_from_slice(payload);

        Ok(self
            .into_interior()
            .expect("the page should be an interior now"))
    }

    pub(crate) fn into_interior(self) -> Option<InteriorPageWrite<'a>> {
        if let PageKind::Interior { .. } = &self.meta.kind {
            Some(InteriorPageWrite(self))
        } else {
            None
        }
    }

    pub(crate) fn init_leaf(mut self, ctx: LogContext<'_>) -> anyhow::Result<LeafPageWrite<'a>> {
        if let PageKind::None = self.meta.kind {
            let pgid = self.id();
            self.meta.lsn = Some(record_mutation(
                ctx,
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::LeafInit {
                        txid: self.txid,
                        pgid,
                    },
                },
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::LeafInit {
                        txid: self.txid,
                        pgid,
                    },
                },
            )?);
            self.meta.is_dirty = true;

            self.meta.kind = PageKind::Leaf {
                count: 0,
                offset: self.pager.page_size - PAGE_FOOTER_SIZE,
                remaining: self.pager.page_size
                    - PAGE_HEADER_SIZE
                    - PAGE_FOOTER_SIZE
                    - LEAF_PAGE_HEADER_SIZE,
                next: None,
            };
        }

        let Some(leaf) = self.into_leaf() else {
            return Err(anyhow!(
                "cannot init leaf page because page is not an empty page nor leaf page",
            ));
        };

        Ok(leaf)
    }

    pub(crate) fn set_leaf(
        mut self,
        ctx: LogContext<'_>,
        payload: &'a [u8],
    ) -> anyhow::Result<LeafPageWrite<'a>> {
        assert!(
            matches!(self.meta.kind, PageKind::None),
            "page is not empty"
        );

        let pgid = self.id();
        let page_size = self.buffer.len();
        self.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafSet {
                    txid: self.txid,
                    pgid,
                    page_version: 0,
                    payload: Bytes::new(payload),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafSet {
                    txid: self.txid,
                    pgid,
                    page_version: 0,
                    payload: Bytes::new(payload),
                },
            },
        )?);
        self.meta.is_dirty = true;

        self.meta.kind = Pager::decode_leaf_page(payload)?;
        self.buffer[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE].copy_from_slice(payload);

        Ok(self.into_leaf().expect("the page should be a leaf now"))
    }

    pub(crate) fn into_leaf(self) -> Option<LeafPageWrite<'a>> {
        if let PageKind::Leaf { .. } = &self.meta.kind {
            Some(LeafPageWrite(self))
        } else {
            None
        }
    }

    // TODO: consider merging `init_overflow` and `OverflowPageWrite::insert_content` into a single
    // `init_overflow` method with a single WAL entry for optimization.
    pub(crate) fn init_overflow(
        mut self,
        ctx: LogContext<'_>,
    ) -> anyhow::Result<Option<OverflowPageWrite<'a>>> {
        if let PageKind::None = self.meta.kind {
            let pgid = self.id();
            self.meta.lsn = Some(record_mutation(
                ctx,
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::OverflowInit {
                        txid: self.txid,
                        pgid,
                    },
                },
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::OverflowInit {
                        txid: self.txid,
                        pgid,
                    },
                },
            )?);
            self.meta.is_dirty = true;
            self.meta.kind = PageKind::Overflow {
                next: None,
                size: 0,
            };
        }

        Ok(self.into_overflow())
    }

    pub(crate) fn set_overflow(
        mut self,
        ctx: LogContext<'_>,
        payload: &'a [u8],
    ) -> anyhow::Result<OverflowPageWrite<'a>> {
        assert!(
            matches!(self.meta.kind, PageKind::None),
            "page is not empty"
        );
        let LogContext::Redo(lsn) = ctx else {
            panic!("set_overflow only can be used for redo-ing wal");
        };

        self.meta.lsn = Some(lsn);
        self.meta.is_dirty = true;
        self.meta.kind = Pager::decode_overflow_page(payload)?;
        self.buffer.copy_from_slice(payload);

        Ok(self
            .into_overflow()
            .expect("the page should be an overflow page now"))
    }

    pub(crate) fn into_overflow(self) -> Option<OverflowPageWrite<'a>> {
        if let PageKind::Overflow { .. } = &self.meta.kind {
            Some(OverflowPageWrite(self))
        } else {
            None
        }
    }
}

// TODO: during recovery, we need to pass the LSN of the log to upadte this page's rec_lsn and page_lsn.
fn record_mutation(
    ctx: LogContext<'_>,
    entry: WalEntry,
    compensation_entry: WalEntry,
) -> anyhow::Result<Lsn> {
    let entry = if ctx.is_undo() {
        compensation_entry
    } else {
        entry
    };

    let lsn = match ctx {
        LogContext::Runtime(wal) => wal.append_log(entry)?,
        LogContext::Undo(wal, ..) => wal.append_log(entry)?,
        LogContext::Redo(lsn) => lsn,
    };
    Ok(lsn)
}

pub(crate) trait BTreePage<'a> {
    type Cell: BTreeCell<'a>;
    fn count(&self) -> usize;
    fn get(&'a self, index: usize) -> Self::Cell;
}

pub(crate) struct InteriorPageRead<'a>(PageRead<'a>);

impl<'a, 'b> BTreePage<'b> for InteriorPageRead<'a> {
    type Cell = InteriorCell<'b>;

    fn count(&self) -> usize {
        let PageKind::Interior { count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        count
    }

    fn get(&'b self, index: usize) -> Self::Cell {
        get_interior_cell(
            &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            index,
        )
    }
}

impl<'a> InteriorPageRead<'a> {
    pub(crate) fn last(&self) -> PageId {
        let PageKind::Interior { last, .. } = self.0.meta.kind else {
            unreachable!();
        };
        last
    }
}

fn get_interior_cell(buff: &[u8], index: usize) -> InteriorCell<'_> {
    let cell_offset = INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * index;
    let cell = &buff[cell_offset..cell_offset + INTERIOR_CELL_SIZE];
    let offset = cell[INTERIOR_CELL_OFFSET_RANGE].read_u16() as usize;
    let size = cell[INTERIOR_CELL_SIZE_RANGE].read_u16() as usize;
    let offset = offset - PAGE_HEADER_SIZE;
    let raw = &buff[offset..offset + size];
    InteriorCell { cell, raw }
}

fn interior_might_split(page_size: usize, remaining: usize) -> bool {
    let payload_size = page_size - PAGE_HEADER_SIZE - INTERIOR_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
    let max_before_overflow = payload_size / 4 - INTERIOR_CELL_SIZE;
    let min_content_not_overflow = max_before_overflow / 2;
    let remaining = if remaining < INTERIOR_CELL_SIZE {
        0
    } else {
        remaining - INTERIOR_CELL_SIZE
    };
    remaining < min_content_not_overflow
}

pub(crate) struct InteriorCell<'a> {
    cell: &'a [u8],
    raw: &'a [u8],
}

pub(crate) trait BTreeCell<'a> {
    fn raw(&self) -> &'a [u8];
    fn key_size(&self) -> usize;
    fn overflow(&self) -> Option<PageId>;
}

impl<'a> BTreeCell<'a> for InteriorCell<'a> {
    fn raw(&self) -> &'a [u8] {
        self.raw
    }

    fn key_size(&self) -> usize {
        self.cell[INTERIOR_CELL_KEY_SIZE_RANGE].read_u32() as usize
    }

    fn overflow(&self) -> Option<PageId> {
        PageId::from_be_bytes(self.cell[INTERIOR_CELL_OVERFLOW_RANGE].try_into().unwrap())
    }
}

impl InteriorCell<'_> {
    pub(crate) fn ptr(&self) -> PageId {
        PageId::from_be_bytes(self.cell[INTERIOR_CELL_PTR_RANGE].try_into().unwrap()).unwrap()
    }
}

pub(crate) struct InteriorPageWrite<'a>(PageWrite<'a>);

impl<'a, 'b> BTreePage<'b> for InteriorPageWrite<'a> {
    type Cell = InteriorCell<'b>;

    fn count(&self) -> usize {
        let PageKind::Interior { count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        count
    }

    fn get(&'b self, index: usize) -> Self::Cell {
        get_interior_cell(
            &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            index,
        )
    }
}

impl<'a> InteriorPageWrite<'a> {
    pub(crate) fn id(&self) -> PageId {
        self.0.meta.id
    }

    pub(crate) fn count(&self) -> usize {
        let PageKind::Interior { count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        count
    }

    pub(crate) fn last(&self) -> PageId {
        let PageKind::Interior { last, .. } = self.0.meta.kind else {
            unreachable!();
        };
        last
    }

    pub(crate) fn get(&self, index: usize) -> InteriorCell<'_> {
        get_interior_cell(
            &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            index,
        )
    }

    pub(crate) fn reset(mut self, ctx: LogContext<'_>) -> anyhow::Result<PageWrite<'a>> {
        Pager::encode(&self.0.meta, self.0.buffer)?;

        let pgid = self.id();
        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorReset {
                    txid: self.0.txid,
                    pgid,
                    page_version: 0,
                    payload: Bytes::new(
                        &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
                    ),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorResetForUndo {
                    txid: self.0.txid,
                    pgid,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        self.0.meta.kind = PageKind::None;
        Ok(self.0)
    }

    pub(crate) fn set_last(&mut self, ctx: LogContext, new_last: PageId) -> anyhow::Result<()> {
        let pgid = self.id();
        let PageKind::Interior { last, .. } = self.0.meta.kind else {
            unreachable!();
        };
        let old_last = last;

        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorSetLast {
                    txid: self.0.txid,
                    pgid,
                    last: new_last,
                    old_last,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorSetLast {
                    txid: self.0.txid,
                    pgid,
                    last: new_last,
                    old_last,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        let PageKind::Interior { ref mut last, .. } = self.0.meta.kind else {
            unreachable!();
        };
        *last = new_last;

        Ok(())
    }

    pub(crate) fn set_cell_ptr(
        &mut self,
        ctx: LogContext,
        index: usize,
        ptr: PageId,
    ) -> anyhow::Result<()> {
        // TODO: refactor this. There are a multiple places where this logic is written.
        // check the `get_interior_cell` implementation.
        let pgid = self.id();
        let cell_offset = PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * index;
        let cell = &mut self.0.buffer[cell_offset..cell_offset + INTERIOR_CELL_SIZE];
        let old_ptr = PageId::from_be_bytes(cell[0..8].try_into().unwrap()).unwrap();
        if old_ptr == ptr {
            return Ok(());
        }

        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorSetCellPtr {
                    txid: self.0.txid,
                    pgid,
                    index,
                    ptr,
                    old_ptr,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorSetCellPtr {
                    txid: self.0.txid,
                    pgid,
                    index,
                    ptr,
                    old_ptr,
                },
            },
        )?);
        self.0.meta.is_dirty = true;
        cell[0..8].copy_from_slice(&ptr.to_be_bytes());

        Ok(())
    }

    pub(crate) fn set_cell_overflow(
        &mut self,
        ctx: LogContext,
        index: usize,
        overflow_pgid: Option<PageId>,
    ) -> anyhow::Result<()> {
        // TODO: refactor this. There are a multiple places where this logic is written.
        // check the `get_interior_cell` implementation.
        let pgid = self.id();
        let cell_offset = PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * index;
        let cell = &mut self.0.buffer[cell_offset..cell_offset + INTERIOR_CELL_SIZE];
        let old_overflow =
            PageId::from_be_bytes(cell[INTERIOR_CELL_OVERFLOW_RANGE].try_into().unwrap());
        if old_overflow == overflow_pgid {
            return Ok(());
        }

        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorSetCellOverflow {
                    txid: self.0.txid,
                    pgid,
                    index,
                    overflow: overflow_pgid,
                    old_overflow,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorSetCellOverflow {
                    txid: self.0.txid,
                    pgid,
                    index,
                    overflow: overflow_pgid,
                    old_overflow,
                },
            },
        )?);
        self.0.meta.is_dirty = true;
        cell[8..16].copy_from_slice(&overflow_pgid.to_be_bytes());

        Ok(())
    }

    pub(crate) fn insert_cell(
        &mut self,
        ctx: LogContext<'_>,
        i: usize,
        cell: InteriorCell,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        log::debug!(
            "insert_interior_cell {pgid:?} kind={:?} page_lsn={:?} i={i} cell_raw_len={:?}",
            self.0.meta.kind,
            self.0.meta.lsn,
            cell.raw().len(),
        );

        let PageKind::Interior { remaining, .. } = self.0.meta.kind else {
            unreachable!();
        };
        let raw = cell.raw();
        assert!(
            raw.len() + INTERIOR_CELL_SIZE <= remaining,
            "insert cell only called in the context of moving a splitted page to a new page, so it should always fit",
        );

        // TODO: insert after the wal is recorded, and also just follow leaf page way to reserve
        // page.
        let reserved_offset = self.insert_cell_meta(
            i,
            cell.ptr(),
            cell.overflow(),
            cell.key_size(),
            cell.raw().len(),
        );
        Bytes::new(cell.raw())
            .put(&mut self.0.buffer[reserved_offset..reserved_offset + raw.len()])?;

        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorInsert {
                    txid: self.0.txid,
                    pgid,
                    index: i,
                    raw: Bytes::new(&self.0.buffer[reserved_offset..reserved_offset + raw.len()]),
                    ptr: cell.ptr(),
                    overflow: cell.overflow(),
                    key_size: cell.key_size(),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorInsert {
                    txid: self.0.txid,
                    pgid,
                    index: i,
                    raw: Bytes::new(&self.0.buffer[reserved_offset..reserved_offset + raw.len()]),
                    ptr: cell.ptr(),
                    overflow: cell.overflow(),
                    key_size: cell.key_size(),
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        log::debug!(
            "insert_interior_cell_finish {pgid:?} kind={:?} page_lsn={:?} i={i}",
            self.0.meta.kind,
            self.0.meta.lsn,
        );

        Ok(())
    }

    pub(crate) fn insert_content(
        &mut self,
        ctx: LogContext<'_>,
        i: usize,
        content: &mut impl Content,
        key_size: usize,
        ptr: PageId,
        overflow: Option<PageId>,
    ) -> anyhow::Result<bool> {
        let pgid = self.id();
        log::debug!(
            "insert_interior_content {pgid:?} kind={:?} page_lsn={:?} i={i} raw_size={}",
            self.0.meta.kind,
            self.0.meta.lsn,
            content.remaining(),
        );

        let total_size = content.remaining();
        let payload_size =
            self.0.buffer.len() - PAGE_HEADER_SIZE - INTERIOR_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
        let max_before_overflow = payload_size / 4 - INTERIOR_CELL_SIZE;
        let min_content_not_overflow = max_before_overflow / 2;
        let PageKind::Interior { remaining, .. } = self.0.meta.kind else {
            unreachable!();
        };
        if remaining < INTERIOR_CELL_SIZE {
            return Ok(false);
        }
        let remaining = remaining - INTERIOR_CELL_SIZE;
        if remaining < min_content_not_overflow && remaining < total_size {
            return Ok(false);
        }

        let raw_size = std::cmp::min(max_before_overflow, total_size);
        let raw_size = std::cmp::min(raw_size, remaining);

        let content_offset = self.insert_cell_meta(i, ptr, overflow, key_size, raw_size);
        content.put(&mut self.0.buffer[content_offset..content_offset + raw_size])?;

        // TODO(important): record the mutation before the insert cell happen. It is important
        // to make sure that the WAL is written. If the page is mutated, but the WAL is not
        // written, we might have a corrupted page.
        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorInsert {
                    txid: self.0.txid,
                    pgid,
                    index: i,
                    raw: Bytes::new(&self.0.buffer[content_offset..content_offset + raw_size]),
                    ptr,
                    key_size,
                    overflow,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorInsert {
                    txid: self.0.txid,
                    pgid,
                    index: i,
                    raw: Bytes::new(&self.0.buffer[content_offset..content_offset + raw_size]),
                    ptr,
                    key_size,
                    overflow,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        log::debug!(
            "insert_interior_content_finish {pgid:?} kind={:?} page_lsn={:?} i={i} raw_size={}",
            self.0.meta.kind,
            self.0.meta.lsn,
            content.remaining(),
        );
        Ok(true)
    }

    fn insert_cell_meta(
        &mut self,
        index: usize,
        ptr: PageId,
        overflow: Option<PageId>,
        key_size: usize,
        raw_size: usize,
    ) -> usize {
        let PageKind::Interior { offset, count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        let added = INTERIOR_CELL_SIZE + raw_size;
        let current_cell_size =
            PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * count;
        if current_cell_size + added > offset {
            self.rearrange();
        }

        let PageKind::Interior {
            ref mut offset,
            ref mut remaining,
            ref mut count,
            ..
        } = self.0.meta.kind
        else {
            unreachable!();
        };

        let shifted = *count - index;
        for i in 0..shifted {
            let x =
                PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * (*count - i);
            let (a, b) = self.0.buffer.split_at_mut(x);
            b[..INTERIOR_CELL_SIZE].copy_from_slice(&a[a.len() - INTERIOR_CELL_SIZE..]);
        }

        let cell_offset = PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * index;
        let cell = &mut self.0.buffer[cell_offset..cell_offset + INTERIOR_CELL_SIZE];

        *offset -= raw_size;
        *remaining -= added;
        *count += 1;

        cell[INTERIOR_CELL_PTR_RANGE].copy_from_slice(&ptr.to_be_bytes());
        cell[INTERIOR_CELL_OVERFLOW_RANGE].copy_from_slice(&overflow.to_be_bytes());
        cell[INTERIOR_CELL_KEY_SIZE_RANGE].copy_from_slice(&(key_size as u32).to_be_bytes());
        cell[INTERIOR_CELL_OFFSET_RANGE].copy_from_slice(&(*offset as u16).to_be_bytes());
        cell[INTERIOR_CELL_SIZE_RANGE].copy_from_slice(&(raw_size as u16).to_be_bytes());

        *offset
    }

    fn rearrange(&mut self) {
        // TODO: try not to copy
        let copied = self.0.buffer.to_vec();

        let mut new_offset = self.0.pager.page_size - PAGE_FOOTER_SIZE;
        for i in 0..self.count() {
            let copied_cell = get_interior_cell(
                &copied[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
                i,
            );
            let copied_content = copied_cell.raw();
            new_offset -= copied_content.len();
            self.0.buffer[new_offset..new_offset + copied_content.len()]
                .copy_from_slice(copied_content);

            let cell_offset = PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * i;
            let cell = &mut self.0.buffer[cell_offset..cell_offset + INTERIOR_CELL_SIZE];
            cell[INTERIOR_CELL_OFFSET_RANGE].copy_from_slice(&(new_offset as u16).to_be_bytes());
        }

        let PageKind::Interior { ref mut offset, .. } = self.0.meta.kind else {
            unreachable!();
        };
        *offset = new_offset;
    }

    pub(crate) fn split(&mut self, ctx: LogContext<'_>) -> anyhow::Result<InteriorPageSplit> {
        let pgid = self.id();
        log::debug!(
            "interior_split {pgid:?} kind={:?} page_lsn={:?}",
            self.0.meta.kind,
            self.0.meta.lsn,
        );

        let payload_size = self.0.pager.page_size
            - PAGE_HEADER_SIZE
            - INTERIOR_PAGE_HEADER_SIZE
            - PAGE_FOOTER_SIZE;
        let half_payload = payload_size / 2;
        let PageKind::Interior { count, .. } = self.0.meta.kind else {
            unreachable!();
        };

        let mut cummulative_size = 0;
        let mut n_cells_to_keep = 0;

        for i in 0..count {
            let cell = get_interior_cell(&self.0.buffer[PAGE_HEADER_SIZE..], i);
            let new_cummulative_size = cummulative_size + cell.raw.len() + INTERIOR_CELL_SIZE;
            if new_cummulative_size >= half_payload {
                n_cells_to_keep = i;
                break;
            }
            cummulative_size = new_cummulative_size;
        }
        assert!(
            n_cells_to_keep < count,
            "there is no point splitting the page if it doesn't move any entries"
        );

        // TODO: we can reduce the wal entry size by merging all of these mutations together.
        for i in (n_cells_to_keep..count).rev() {
            let cell = get_interior_cell(&self.0.buffer[PAGE_HEADER_SIZE..], i);
            self.0.meta.lsn = Some(record_mutation(
                ctx,
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::InteriorDelete {
                        txid: self.0.txid,
                        pgid,
                        index: i,
                        old_raw: Bytes::new(cell.raw()),
                        old_ptr: cell.ptr(),
                        old_overflow: cell.overflow(),
                        old_key_size: cell.key_size(),
                    },
                },
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::InteriorDeleteForUndo {
                        txid: self.0.txid,
                        pgid,
                        index: i,
                    },
                },
            )?);
        }
        self.0.meta.is_dirty = true;

        let original_count = count;
        let PageKind::Interior {
            ref mut count,
            ref mut remaining,
            ..
        } = self.0.meta.kind
        else {
            unreachable!();
        };
        *count = n_cells_to_keep;
        *remaining = payload_size - cummulative_size;

        log::debug!(
            "interior_split_finish {pgid:?} kind={:?} page_lsn={:?}",
            self.0.meta.kind,
            self.0.meta.lsn,
        );

        Ok(InteriorPageSplit {
            n: n_cells_to_keep,

            buff: &self.0.buffer[PAGE_HEADER_SIZE..],
            i: n_cells_to_keep,
            end: original_count,
        })
    }

    pub(crate) fn delete(&mut self, ctx: LogContext<'_>, index: usize) -> anyhow::Result<()> {
        let pgid = self.id();
        log::debug!(
            "insert_delete {pgid:?} kind={:?} page_lsn={:?} i={index}",
            self.0.meta.kind,
            self.0.meta.lsn,
        );

        let cell = get_interior_cell(
            &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            index,
        );
        let content_offset = cell.cell[INTERIOR_CELL_OFFSET_RANGE].read_u16() as usize;
        let content_size = cell.cell[INTERIOR_CELL_SIZE_RANGE].read_u16() as usize;

        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorDelete {
                    txid: self.0.txid,
                    pgid,
                    index,
                    old_raw: Bytes::new(
                        &self.0.buffer[content_offset..content_offset + content_size],
                    ),
                    old_ptr: cell.ptr(),
                    old_overflow: cell.overflow(),
                    old_key_size: cell.key_size(),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::InteriorDeleteForUndo {
                    txid: self.0.txid,
                    pgid,
                    index,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        let PageKind::Interior {
            ref mut offset,
            ref mut remaining,
            ref mut count,
            ..
        } = self.0.meta.kind
        else {
            unreachable!();
        };
        if *offset == content_offset {
            *offset += content_size;
        }
        *remaining += INTERIOR_CELL_SIZE + content_size;
        *count -= 1;

        for i in index..*count {
            let x = PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_CELL_SIZE * i;
            let (a, b) = self.0.buffer.split_at_mut(x + INTERIOR_CELL_SIZE);
            let a_len = a.len();
            a[a_len - INTERIOR_CELL_SIZE..].copy_from_slice(&b[..INTERIOR_CELL_SIZE]);
        }

        log::debug!(
            "insert_delete_finish {pgid:?} kind={:?} page_lsn={:?} i={index}",
            self.0.meta.kind,
            self.0.meta.lsn,
        );
        Ok(())
    }

    pub(crate) fn might_split(&self) -> bool {
        let PageKind::Interior { remaining, .. } = self.0.meta.kind else {
            unreachable!();
        };
        interior_might_split(self.0.pager.page_size, remaining)
    }
}

pub(crate) struct InteriorPageSplit<'a> {
    pub(crate) n: usize,

    buff: &'a [u8],
    i: usize,
    end: usize,
}

impl<'a> Iterator for InteriorPageSplit<'a> {
    type Item = InteriorCell<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.end {
            return None;
        }

        let cell = get_interior_cell(self.buff, self.i);
        self.i += 1;
        Some(cell)
    }
}

pub(crate) struct LeafPageRead<'a>(PageRead<'a>);

impl<'a, 'b> BTreePage<'b> for LeafPageRead<'a> {
    type Cell = LeafCell<'b>;
    fn count(&self) -> usize {
        let PageKind::Leaf { count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        count
    }

    fn get(&'b self, index: usize) -> Self::Cell {
        get_leaf_cell(
            &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            index,
        )
    }
}

impl<'a> LeafPageRead<'a> {
    pub(crate) fn id(&self) -> PageId {
        self.0.meta.id
    }

    pub(crate) fn next(&self) -> Option<PageId> {
        let PageKind::Leaf { next, .. } = self.0.meta.kind else {
            unreachable!();
        };
        next
    }
}

pub(crate) struct LeafPageWrite<'a>(PageWrite<'a>);

impl<'a, 'b> BTreePage<'b> for LeafPageWrite<'a> {
    type Cell = LeafCell<'b>;
    fn count(&self) -> usize {
        let PageKind::Leaf { count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        count
    }

    fn get(&'b self, index: usize) -> Self::Cell {
        get_leaf_cell(
            &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            index,
        )
    }
}

impl<'a> LeafPageWrite<'a> {
    pub(crate) fn id(&self) -> PageId {
        self.0.meta.id
    }

    pub(crate) fn count(&self) -> usize {
        let PageKind::Leaf { count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        count
    }

    pub(crate) fn get(&self, index: usize) -> LeafCell {
        get_leaf_cell(
            &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            index,
        )
    }

    pub(crate) fn next(&self) -> Option<PageId> {
        let PageKind::Leaf { next, .. } = self.0.meta.kind else {
            unreachable!();
        };
        next
    }

    pub(crate) fn reset(mut self, ctx: LogContext<'_>) -> anyhow::Result<PageWrite<'a>> {
        Pager::encode(&self.0.meta, self.0.buffer)?;

        let pgid = self.id();
        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafReset {
                    txid: self.0.txid,
                    pgid,
                    page_version: 0,
                    payload: Bytes::new(
                        &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
                    ),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafResetForUndo {
                    txid: self.0.txid,
                    pgid,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        self.0.meta.kind = PageKind::None;
        Ok(self.0)
    }

    pub(crate) fn delete(&mut self, ctx: LogContext<'_>, index: usize) -> anyhow::Result<()> {
        let pgid = self.0.meta.id;
        let cell = get_leaf_cell(
            &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            index,
        );
        let content_offset = cell.cell[LEAF_CELL_OFFSET_RANGE].read_u16() as usize;
        let content_size = cell.cell[LEAF_CELL_SIZE_RANGE].read_u16() as usize;

        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafDelete {
                    txid: self.0.txid,
                    pgid,
                    index,
                    old_raw: Bytes::new(
                        &self.0.buffer[content_offset..content_offset + content_size],
                    ),
                    old_overflow: cell.overflow(),
                    old_key_size: cell.key_size(),
                    old_val_size: cell.val_size(),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafDeleteForUndo {
                    txid: self.0.txid,
                    pgid,
                    index,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        let PageKind::Leaf {
            ref mut offset,
            ref mut remaining,
            ref mut count,
            ..
        } = self.0.meta.kind
        else {
            unreachable!();
        };
        assert!(index < *count, "deleting {index} of {count}");
        assert!(*count > 0);

        if *offset == content_offset {
            *offset += content_size;
        }
        *remaining += LEAF_CELL_SIZE + content_size;
        *count -= 1;

        for i in index..*count {
            let cell_offset = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * i;
            let (a, b) = self.0.buffer.split_at_mut(cell_offset + LEAF_CELL_SIZE);
            let a_len = a.len();
            a[a_len - LEAF_CELL_SIZE..].copy_from_slice(&b[..LEAF_CELL_SIZE]);
        }

        Ok(())
    }

    pub(crate) fn set_next(
        &mut self,
        ctx: LogContext,
        new_next: Option<PageId>,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let PageKind::Leaf { next, .. } = self.0.meta.kind else {
            unreachable!();
        };
        let old_next = next;
        if old_next == new_next {
            return Ok(());
        }

        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafSetNext {
                    txid: self.0.txid,
                    pgid,
                    next: new_next,
                    old_next,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafSetNext {
                    txid: self.0.txid,
                    pgid,
                    next: new_next,
                    old_next,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        let PageKind::Leaf { ref mut next, .. } = self.0.meta.kind else {
            unreachable!();
        };
        *next = new_next;

        Ok(())
    }

    pub(crate) fn set_cell_overflow(
        &mut self,
        ctx: LogContext,
        index: usize,
        overflow_pgid: Option<PageId>,
    ) -> anyhow::Result<()> {
        // TODO: refactor this. There are a multiple places where this logic is written.
        // check the `get_leaf_cell` implementation.
        let pgid = self.id();
        let cell_offset = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * index;
        let cell = &mut self.0.buffer[cell_offset..cell_offset + LEAF_CELL_SIZE];
        let old_overflow = PageId::from_be_bytes(cell[0..8].try_into().unwrap());
        if old_overflow == overflow_pgid {
            return Ok(());
        }

        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafSetOverflow {
                    txid: self.0.txid,
                    pgid,
                    index,
                    overflow: overflow_pgid,
                    old_overflow,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafSetOverflow {
                    txid: self.0.txid,
                    pgid,
                    index,
                    overflow: overflow_pgid,
                    old_overflow,
                },
            },
        )?);
        self.0.meta.is_dirty = true;
        cell[0..8].copy_from_slice(&overflow_pgid.to_be_bytes());

        Ok(())
    }

    pub(crate) fn insert_cell(
        &mut self,
        ctx: LogContext<'_>,
        i: usize,
        cell: LeafCell,
    ) -> anyhow::Result<()> {
        let PageKind::Leaf { remaining, .. } = self.0.meta.kind else {
            unreachable!();
        };
        let raw = cell.raw();
        assert!(
            raw.len() + LEAF_CELL_SIZE <= remaining,
            "insert cell only called in the context of moving a splitted page to a new page, so it should always fit",
        );

        let reserved_offset = self.reserve_cell(raw.len());
        self.0.buffer[reserved_offset..reserved_offset + raw.len()].copy_from_slice(raw);

        let pgid = self.id();
        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafInsert {
                    txid: self.0.txid,
                    pgid,
                    index: i,
                    raw: Bytes::new(cell.raw()),
                    overflow: cell.overflow(),
                    key_size: cell.key_size(),
                    value_size: cell.val_size(),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafInsert {
                    txid: self.0.txid,
                    pgid,
                    index: i,
                    raw: Bytes::new(cell.raw()),
                    overflow: cell.overflow(),
                    key_size: cell.key_size(),
                    value_size: cell.val_size(),
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        self.insert_cell_meta(
            i,
            cell.overflow(),
            cell.key_size(),
            cell.val_size(),
            raw.len(),
        );

        Ok(())
    }

    pub(crate) fn insert_content(
        &mut self,
        ctx: LogContext<'_>,
        i: usize,
        content: &mut impl Content,
        key_size: usize,
        value_size: usize,
        overflow: Option<PageId>,
    ) -> anyhow::Result<bool> {
        let content_size = content.remaining();
        let payload_size =
            self.0.buffer.len() - PAGE_HEADER_SIZE - LEAF_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
        let max_before_overflow = payload_size / 4 - LEAF_CELL_SIZE;
        let min_content_not_overflow = max_before_overflow / 2;
        let PageKind::Leaf { remaining, .. } = self.0.meta.kind else {
            unreachable!();
        };
        if remaining < LEAF_CELL_SIZE {
            return Ok(false);
        }
        let remaining = remaining - LEAF_CELL_SIZE;
        if remaining < min_content_not_overflow && remaining < content_size {
            return Ok(false);
        }

        let raw_size = std::cmp::min(max_before_overflow, content_size);
        let raw_size = std::cmp::min(raw_size, remaining);

        let reserved_offset = self.reserve_cell(raw_size);
        content.put(&mut self.0.buffer[reserved_offset..reserved_offset + raw_size])?;

        let pgid = self.id();
        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafInsert {
                    txid: self.0.txid,
                    pgid,
                    index: i,
                    raw: Bytes::new(&self.0.buffer[reserved_offset..reserved_offset + raw_size]),
                    overflow,
                    key_size,
                    value_size,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::LeafInsert {
                    txid: self.0.txid,
                    pgid,
                    index: i,
                    raw: Bytes::new(&self.0.buffer[reserved_offset..reserved_offset + raw_size]),
                    overflow,
                    key_size,
                    value_size,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        self.insert_cell_meta(i, overflow, key_size, value_size, raw_size);
        Ok(true)
    }

    fn reserve_cell(&mut self, raw_size: usize) -> usize {
        let added = LEAF_CELL_SIZE + raw_size;
        let PageKind::Leaf {
            offset,
            count,
            remaining,
            ..
        } = self.0.meta.kind
        else {
            unreachable!();
        };
        assert!(added <= remaining, "added cell should be fit in the page");
        let current_cell_size = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * count;
        if current_cell_size + added > offset {
            self.rearrange();
        }

        let PageKind::Leaf { offset, .. } = self.0.meta.kind else {
            unreachable!();
        };
        assert!(current_cell_size + added <= offset, "added cell overflowed to the offset. current_cell_size={current_cell_size} added={added} offset={offset}");
        offset - raw_size
    }

    // insert_cell_meta assumes that the raw content is already inserted to the page
    fn insert_cell_meta(
        &mut self,
        index: usize,
        overflow: Option<PageId>,
        key_size: usize,
        val_size: usize,
        raw_size: usize,
    ) {
        let added = LEAF_CELL_SIZE + raw_size;
        let pgid = self.id();
        let PageKind::Leaf {
            ref mut offset,
            ref mut count,
            ref mut remaining,
            ..
        } = self.0.meta.kind
        else {
            unreachable!();
        };
        assert!(
            index <= *count,
            "insert cell meta on pgid={pgid:?} index out of bound. index={index}, count={count}",
        );
        let current_cell_size = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * *count;
        assert!(current_cell_size + added <= *offset);

        let shifted = *count - index;
        for i in 0..shifted {
            let cell_offset =
                PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * (*count - i);
            let (a, b) = self.0.buffer.split_at_mut(cell_offset);
            b[..LEAF_CELL_SIZE].copy_from_slice(&a[a.len() - LEAF_CELL_SIZE..]);
        }

        let cell_offset = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * index;
        let cell = &mut self.0.buffer[cell_offset..cell_offset + LEAF_CELL_SIZE];

        *offset -= raw_size;
        *remaining -= added;
        *count += 1;
        cell[LEAF_CELL_OVERFLOW_RANGE].copy_from_slice(&overflow.to_be_bytes());
        cell[LEAF_CELL_KEY_SIZE_RANGE].copy_from_slice(&(key_size as u32).to_be_bytes());
        cell[LEAF_CELL_VAL_SIZE_RANGE].copy_from_slice(&(val_size as u32).to_be_bytes());
        cell[LEAF_CELL_OFFSET_RANGE].copy_from_slice(&(*offset as u16).to_be_bytes());
        cell[LEAF_CELL_SIZE_RANGE].copy_from_slice(&(raw_size as u16).to_be_bytes());
    }

    fn rearrange(&mut self) {
        // TODO: try not to copy
        let copied_payload =
            self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE].to_vec();

        let mut new_offset = self.0.pager.page_size - PAGE_FOOTER_SIZE;
        for i in 0..self.count() {
            let copied_cell = get_leaf_cell(&copied_payload, i);
            let copied_content = copied_cell.raw();
            new_offset -= copied_content.len();
            self.0.buffer[new_offset..new_offset + copied_content.len()]
                .copy_from_slice(copied_content);

            let cell_offset = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * i;
            let cell = &mut self.0.buffer[cell_offset..cell_offset + LEAF_CELL_SIZE];
            cell[LEAF_CELL_OFFSET_RANGE].copy_from_slice(&(new_offset as u16).to_be_bytes());
        }

        let PageKind::Leaf { ref mut offset, .. } = self.0.meta.kind else {
            unreachable!();
        };
        *offset = new_offset;
    }

    pub(crate) fn split(&mut self, ctx: LogContext<'_>) -> anyhow::Result<LeafPageSplit> {
        let pgid = self.id();
        let payload_size =
            self.0.pager.page_size - PAGE_HEADER_SIZE - LEAF_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
        let half_payload = payload_size / 2;
        let PageKind::Leaf { count, .. } = self.0.meta.kind else {
            unreachable!();
        };

        let mut cummulative_size = 0;
        let mut n_cells_to_keep = 0;

        for i in 0..count {
            let cell = get_leaf_cell(
                &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
                i,
            );
            let new_cummulative_size = cummulative_size + cell.raw.len() + LEAF_CELL_SIZE;
            if new_cummulative_size >= half_payload {
                n_cells_to_keep = i;
                break;
            }
            cummulative_size = new_cummulative_size;
        }
        assert!(
            n_cells_to_keep < count,
            "there is no point splitting the page if it doesn't move any entries"
        );

        // TODO: we can reduce the wal entry size by merging all of these mutations together.
        for i in (n_cells_to_keep..count).rev() {
            let cell = get_leaf_cell(
                &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
                i,
            );
            self.0.meta.lsn = Some(record_mutation(
                ctx,
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::LeafDelete {
                        txid: self.0.txid,
                        pgid,
                        index: i,
                        old_raw: Bytes::new(cell.raw()),
                        old_overflow: cell.overflow(),
                        old_key_size: cell.key_size(),
                        old_val_size: cell.val_size(),
                    },
                },
                WalEntry {
                    clr: ctx.clr(),
                    kind: WalKind::LeafDeleteForUndo {
                        txid: self.0.txid,
                        pgid,
                        index: i,
                    },
                },
            )?);
        }
        self.0.meta.is_dirty = true;

        let original_count = count;
        let PageKind::Leaf {
            ref mut count,
            ref mut remaining,
            ..
        } = self.0.meta.kind
        else {
            unreachable!();
        };
        *count = n_cells_to_keep;
        *remaining = payload_size - cummulative_size;

        Ok(LeafPageSplit {
            n: n_cells_to_keep,

            buff: self.0.buffer,
            i: n_cells_to_keep,
            end: original_count,
        })
    }
}

fn get_leaf_cell(payload: &[u8], index: usize) -> LeafCell<'_> {
    let cell_offset = LEAF_PAGE_HEADER_SIZE + LEAF_CELL_SIZE * index;
    let cell = &payload[cell_offset..cell_offset + LEAF_CELL_SIZE];
    let offset = cell[LEAF_CELL_OFFSET_RANGE].read_u16() as usize;
    assert!(
        offset >= PAGE_HEADER_SIZE,
        "cannot get leaf cell {index}, offset={offset} page_header_size={PAGE_HEADER_SIZE} cell={cell:x?} payload={payload:x?}",
    );
    let offset = offset - PAGE_HEADER_SIZE;
    let size = cell[LEAF_CELL_SIZE_RANGE].read_u16() as usize;
    assert!(
        offset + size <= payload.len(),
        "offset + size is overflow. offset={offset} size={size}"
    );
    let raw = &payload[offset..offset + size];
    LeafCell { cell, raw }
}

// TODO: maybe it's a good idea to store the pointer to the value directly in the cell.
// This is because we often want to redirected to the value instead of just the key.
pub(crate) struct LeafCell<'a> {
    cell: &'a [u8],
    raw: &'a [u8],
}

impl<'a> BTreeCell<'a> for LeafCell<'a> {
    fn raw(&self) -> &'a [u8] {
        self.raw
    }

    fn key_size(&self) -> usize {
        self.cell[LEAF_CELL_KEY_SIZE_RANGE].read_u32() as usize
    }

    fn overflow(&self) -> Option<PageId> {
        PageId::from_be_bytes(self.cell[LEAF_CELL_OVERFLOW_RANGE].try_into().unwrap())
    }
}

impl<'a> LeafCell<'a> {
    pub(crate) fn val_size(&self) -> usize {
        self.cell[LEAF_CELL_VAL_SIZE_RANGE].read_u32() as usize
    }
}

pub(crate) struct LeafPageSplit<'a> {
    pub(crate) n: usize,
    buff: &'a [u8],
    i: usize,
    end: usize,
}

impl<'a> Iterator for LeafPageSplit<'a> {
    type Item = LeafCell<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.end {
            return None;
        }

        let cell = get_leaf_cell(
            &self.buff[PAGE_HEADER_SIZE..self.buff.len() - PAGE_FOOTER_SIZE],
            self.i,
        );
        self.i += 1;
        Some(cell)
    }
}

pub(crate) struct OverflowPageRead<'a>(PageRead<'a>);

impl<'a> OverflowPageRead<'a> {
    pub(crate) fn next(&self) -> Option<PageId> {
        let PageKind::Overflow { next, .. } = self.0.meta.kind else {
            unreachable!();
        };
        next
    }

    pub(crate) fn content(&self) -> &[u8] {
        let PageKind::Overflow { size, .. } = self.0.meta.kind else {
            unreachable!();
        };
        get_overflow_content(self.0.buffer, size)
    }
}

fn get_overflow_content(buff: &[u8], size: usize) -> &[u8] {
    let offset = PAGE_HEADER_SIZE + OVERFLOW_PAGE_HEADER_SIZE;
    &buff[offset..offset + size]
}

pub(crate) struct OverflowPageWrite<'a>(PageWrite<'a>);

impl<'a> OverflowPageWrite<'a> {
    pub(crate) fn id(&self) -> PageId {
        self.0.meta.id
    }

    pub(crate) fn set_next(
        &mut self,
        ctx: LogContext,
        new_next: Option<PageId>,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let PageKind::Overflow { ref next, .. } = self.0.meta.kind else {
            unreachable!();
        };
        let old_next = *next;
        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowSetNext {
                    txid: self.0.txid,
                    pgid,
                    next: new_next,
                    old_next,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowSetNext {
                    txid: self.0.txid,
                    pgid,
                    next: new_next,
                    old_next,
                },
            },
        )?);
        self.0.meta.is_dirty = true;
        let PageKind::Overflow { ref mut next, .. } = self.0.meta.kind else {
            unreachable!();
        };
        *next = new_next;
        Ok(())
    }

    pub(crate) fn set_content(
        &mut self,
        ctx: LogContext<'_>,
        content: &mut impl Content,
        next: Option<PageId>,
    ) -> anyhow::Result<()> {
        let pgid = self.id();
        let max_size = self.0.pager.page_size
            - PAGE_HEADER_SIZE
            - PAGE_FOOTER_SIZE
            - OVERFLOW_PAGE_HEADER_SIZE;

        let PageKind::Overflow {
            size: ref p_size,
            next: ref p_next,
        } = self.0.meta.kind
        else {
            unreachable!();
        };
        if *p_size != 0 || p_next.is_some() {
            return Err(anyhow!("overflow page is already filled"));
        }

        let inserted_size = std::cmp::min(content.remaining(), max_size);

        let offset = PAGE_HEADER_SIZE + OVERFLOW_PAGE_HEADER_SIZE;
        let raw = &mut self.0.buffer[offset..offset + inserted_size];
        content.put(raw)?;
        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowSetContent {
                    txid: self.0.txid,
                    pgid,
                    raw: Bytes::new(raw),
                    next,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowSetContent {
                    txid: self.0.txid,
                    pgid,
                    raw: Bytes::new(raw),
                    next,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        let PageKind::Overflow {
            size: ref mut p_size,
            next: ref mut p_next,
        } = self.0.meta.kind
        else {
            unreachable!();
        };

        *p_size = inserted_size;
        *p_next = next;

        Ok(())
    }

    pub(crate) fn unset_content(&mut self, ctx: LogContext<'_>) -> anyhow::Result<()> {
        let pgid = self.id();
        let PageKind::Overflow { size, .. } = self.0.meta.kind else {
            unreachable!();
        };
        if size == 0 {
            return Ok(());
        }

        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowSetContentForUndo {
                    txid: self.0.txid,
                    pgid,
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowSetContentForUndo {
                    txid: self.0.txid,
                    pgid,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        let PageKind::Overflow { ref mut size, .. } = self.0.meta.kind else {
            unreachable!();
        };
        *size = 0;

        Ok(())
    }

    pub(crate) fn reset(mut self, ctx: LogContext<'_>) -> anyhow::Result<PageWrite<'a>> {
        Pager::encode(&self.0.meta, self.0.buffer)?;

        let pgid = self.id();
        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowReset {
                    txid: self.0.txid,
                    pgid,
                    page_version: 0,
                    payload: Bytes::new(
                        &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
                    ),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowResetForUndo {
                    txid: self.0.txid,
                    pgid,
                },
            },
        )?);
        self.0.meta.lsn = Some(record_mutation(
            ctx,
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowReset {
                    txid: self.0.txid,
                    pgid,
                    page_version: 0,
                    payload: Bytes::new(
                        &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
                    ),
                },
            },
            WalEntry {
                clr: ctx.clr(),
                kind: WalKind::OverflowResetForUndo {
                    txid: self.0.txid,
                    pgid,
                },
            },
        )?);
        self.0.meta.is_dirty = true;

        self.0.meta.kind = PageKind::None;
        Ok(self.0)
    }
}

fn read_page(f: &mut File, id: PageId, page_size: usize, buff: &mut [u8]) -> anyhow::Result<bool> {
    // TODO: try to seek and read using a single syscall
    let page_size = page_size as u64;
    let file_size = f.metadata()?.len();
    let min_size = id.get() * page_size + page_size;
    if min_size > file_size {
        return Ok(false);
    }
    f.seek(SeekFrom::Start(id.get() * page_size))?;
    f.read_exact(buff)?;
    Ok(true)
}

fn write_page(f: &mut File, id: PageId, page_size: usize, buff: &[u8]) -> anyhow::Result<()> {
    let page_size = page_size as u64;
    let file_size = f.metadata()?.len();
    let min_size = id.get() * page_size + page_size;
    if min_size > file_size {
        f.set_len(id.get() * page_size + page_size)?;
    }
    f.seek(SeekFrom::Start(id.get() * page_size))?;
    f.write_all(buff)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_lock::FileLock;
    use std::fs::OpenOptions;

    #[test]
    fn test_pager_interior() {
        let page_size = 256;

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.wal");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(file_path)
            .unwrap()
            .lock()
            .unwrap();

        let double_buff_file_path = dir.path().join("test.wal");
        let double_buff_file = File::create(double_buff_file_path).unwrap();

        let pager = Pager::new(file, double_buff_file, page_size, 10).unwrap();
        let wal = crate::recovery::recover(dir.path(), &pager).unwrap().wal;
        let txid = TxId::new(1).unwrap();

        let ctx = LogContext::Redo(Lsn::new(1));
        let page1 = pager.alloc(ctx, txid).unwrap();
        let pgid_last = PageId::new(7).unwrap();
        let mut page1 = page1
            .init_interior(LogContext::Runtime(&wal), pgid_last)
            .unwrap()
            .unwrap();
        let pgid_ptr = PageId::new(8).unwrap();
        for i in 0..4 {
            let ok = page1
                .insert_content(
                    LogContext::Runtime(&wal),
                    i,
                    &mut Bytes::new(format!("{i:026}").as_bytes()),
                    26,
                    pgid_ptr,
                    None,
                )
                .unwrap();
            assert!(ok);
        }
        assert_eq!(4, page1.count());
        assert_eq!(b"00000000000000000000000000", page1.get(0).raw());
        assert_eq!(b"00000000000000000000000001", page1.get(1).raw());
        assert_eq!(b"00000000000000000000000002", page1.get(2).raw());
        assert_eq!(b"00000000000000000000000003", page1.get(3).raw());

        page1.delete(LogContext::Runtime(&wal), 2).unwrap();
        assert_eq!(3, page1.count());
        assert_eq!(b"00000000000000000000000000", page1.get(0).raw());
        assert_eq!(b"00000000000000000000000001", page1.get(1).raw());
        assert_eq!(b"00000000000000000000000003", page1.get(2).raw());

        page1
            .insert_content(
                LogContext::Runtime(&wal),
                2,
                &mut Bytes::new(b"00000000000000000000000002"),
                26,
                pgid_ptr,
                None,
            )
            .unwrap();
        assert_eq!(4, page1.count());
        assert_eq!(b"00000000000000000000000000", page1.get(0).raw());
        assert_eq!(b"00000000000000000000000001", page1.get(1).raw());
        assert_eq!(b"00000000000000000000000002", page1.get(2).raw());
        assert_eq!(b"00000000000000000000000003", page1.get(3).raw());

        page1.delete(LogContext::Runtime(&wal), 3).unwrap();
        assert_eq!(3, page1.count());
        assert_eq!(b"00000000000000000000000000", page1.get(0).raw());
        assert_eq!(b"00000000000000000000000001", page1.get(1).raw());
        assert_eq!(b"00000000000000000000000002", page1.get(2).raw());

        drop(page1);

        pager.shutdown().unwrap();
        dir.close().unwrap();
    }
}
