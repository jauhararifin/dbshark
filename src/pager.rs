use crate::content::Content;
use crate::wal::{Lsn, LsnExt, TxId, TxState, Wal, WalRecord};
use anyhow::anyhow;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::os::unix::fs::MetadataExt;

pub(crate) const MINIMUM_PAGE_SIZE: usize = 256;

// TODO: idea for double buffering
// Manage a separate buffer of N pages (maybe we can set N to be 10 and 20).
// This buffer lives on memory. Before flushing a page, put it to this buffer first.
// When full, or timeout, or forced, flush the buffer to a double-write buffer file somewhere
// If success, then flush those N pages to the main db file.
// When loading pages from master db file, first load it from master file first. It the checksum mismatch
// load it from double-write file.

// TODO: use better eviction policy. De-priorize pages with `page_lsn` < `flushed_lsn`.
// Also use better algorithm such as tiny-lfu or secondchance.

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PageId(NonZeroU64);

impl PageId {
    pub(crate) fn new(id: u64) -> Option<Self> {
        NonZeroU64::new(id).map(Self)
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.get()
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
        self.0.get().to_be_bytes()
    }
}
impl PageIdExt for Option<PageId> {
    fn to_be_bytes(&self) -> [u8; 8] {
        if let Some(pgid) = self {
            pgid.to_be_bytes()
        } else {
            0u64.to_be_bytes()
        }
    }
}

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
}

pub(crate) struct Pager {
    f: Mutex<File>,
    double_buff_f: Mutex<File>,
    page_size: usize,
    n: usize,

    internal: RwLock<PagerInternal>,
    flush_internal: RwLock<PagerFlushInternal>,
}

pub(crate) struct PagerInternal {
    allocated: usize,
    file_page_count: usize,
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
    flushing_pgids: Vec<PageId>,
}

const PAGE_HEADER_SIZE: usize = 32;
const PAGE_FOOTER_SIZE: usize = 8;
const INTERIOR_PAGE_HEADER_SIZE: usize = 16;
const INTERIOR_PAGE_CELL_SIZE: usize = 24;
const LEAF_PAGE_HEADER_SIZE: usize = 16;
const LEAF_PAGE_CELL_SIZE: usize = 24;
const OVERFLOW_PAGE_HEADER_SIZE: usize = 16;
const FREELIST_PAGE_HEADER_SIZE: usize = 16;

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

        let dummy_pgid = PageId(NonZeroU64::new(1).unwrap());
        let metas = (0..n)
            .map(|_| {
                RwLock::new(PageMeta {
                    id: dummy_pgid,
                    kind: PageKind::None,
                    wal: None,
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();

        let file_size = f.metadata()?.size();
        let mut file_page_count = file_size as usize / page_size;
        if file_page_count == 0 {
            file_page_count = 1;
        }

        let flushing_area_n = 10;
        let flushing_pages = vec![0u8; page_size * flushing_area_n].into_boxed_slice();

        Ok(Self {
            f: Mutex::new(f),
            double_buff_f: Mutex::new(double_buff_f),
            page_size,
            n,

            internal: RwLock::new(PagerInternal {
                allocated: 0,
                file_page_count,
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
                flushing_pgids: vec![],
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
        if page_size > 0x4000 {
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
                wal: None,
            };

            let ok = Self::decode_internal(page_size, &mut meta, buff)?;
            if ok {
                let offset = meta.id.get() * page_size as u64;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(buff)?;
            }
        }

        file.sync_all()?;
        Ok(())
    }

    pub(crate) fn page_count(&self) -> usize {
        self.internal.read().file_page_count
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
        let (frame_id, meta, buffer) = self.acquire(internal, pgid, true)?;

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
        let (frame_id, meta, buffer) = self.acquire(internal, pgid, true)?;
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

    pub(crate) fn alloc(&self, txid: TxId) -> anyhow::Result<PageWrite> {
        let mut internal = self.internal.write();
        let pgid = {
            internal.file_page_count += 1;
            PageId::new((internal.file_page_count - 1) as u64).unwrap()
        };

        let (frame_id, meta, buffer) = self.acquire(internal, pgid, false)?;
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

    fn acquire(
        &self,
        mut internal: RwLockWriteGuard<PagerInternal>,
        pgid: PageId,
        mut is_existing_page: bool,
    ) -> anyhow::Result<(usize, &RwLock<PageMeta>, *mut u8)> {
        // TODO: if the page is in the flushing area, bring it back to buffer pool without
        // reading from disk.

        if pgid.get() as usize >= internal.file_page_count {
            is_existing_page = false;
        }

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
            internal.allocated += 1;
            internal.ref_count[frame_id] += 1;
            internal.page_to_frame.insert(pgid, frame_id);

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

            drop(internal);

            if is_existing_page {
                self.decode(pgid, &mut meta_locked, buffer)?;
            } else {
                *meta_locked = PageMeta {
                    id: pgid,
                    kind: PageKind::None,
                    wal: None,
                };
            }
            drop(meta_locked);

            Ok((frame_id, meta, buffer_offset))
        } else {
            let (frame_id, evicted) = Self::evict_one(&mut internal)?;

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

            internal.page_to_frame.remove(&old_pgid);
            internal.page_to_frame.insert(pgid, frame_id);
            assert!(internal.ref_count[frame_id] == 0);
            internal.ref_count[frame_id] += 1;
            internal.free_frames.remove(&frame_id);
            internal.free_and_clean.remove(&frame_id);

            // It's ok to acquire exclusive lock here when `internal`'s lock is held because
            // acquiring the meta's lock here is instant since only free page can be evicted.
            let mut meta_locked = meta.write();
            let offset = frame_id * self.page_size;
            // SAFETY: it's guaranteed that the buffer + offset is pointed to valid address.
            let buffer_offset = unsafe { internal.buffer.add(offset) };
            // SAFETY: it's guaranteed that buffer has only one mutable reference or multiple
            // shared reference since it's protected by page meta's lock.
            let buffer = unsafe { std::slice::from_raw_parts_mut(buffer_offset, self.page_size) };
            internal.dirty_frames.remove(&frame_id);

            drop(internal);
            if evicted {
                Self::encode(&meta_locked, buffer)?;

                let mut flush_internal = self.flush_internal.write();
                Self::evict_page(
                    &mut flush_internal,
                    &self.f,
                    &self.double_buff_f,
                    self.page_size,
                    pgid,
                    buffer,
                )?;
                drop(flush_internal);
            }

            if is_existing_page {
                self.decode(pgid, &mut meta_locked, buffer)?;
            } else {
                *meta_locked = PageMeta {
                    id: pgid,
                    kind: PageKind::None,
                    wal: None,
                };
            }
            drop(meta_locked);

            Ok((frame_id, meta, buffer_offset))
        }
    }

    fn evict_one(internal: &mut PagerInternal) -> anyhow::Result<(usize, bool)> {
        if let Some(frame_id) = internal.free_and_clean.iter().next().copied() {
            internal.free_and_clean.remove(&frame_id);
            Ok((frame_id, false))
        } else if let Some(frame_id) = internal.free_frames.iter().next().copied() {
            internal.free_frames.remove(&frame_id);
            Ok((frame_id, true))
        } else {
            // TODO: consider sleep and retry this process
            Err(anyhow!("all pages are pinned"))
        }
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
        f.seek(SeekFrom::Start(pgid.get() * page_size as u64))?;
        f.write_all(buffer)?;
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
        if let Some(i) = internal.flushing_pgids.iter().position(|p| p == &pgid) {
            internal.flushing_pages[i * page_size..(i + 1) * page_size].copy_from_slice(buffer);
        } else {
            let is_full =
                internal.flushing_pgids.len() * page_size >= internal.flushing_pages.len();
            if is_full {
                Self::flush_and_clear_flushing_pages(internal, f, double_buff_f, page_size)?;
            }

            let i = internal.flushing_pgids.len();
            internal.flushing_pgids.push(pgid);
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
            f.seek(SeekFrom::Start(pgid.get() * page_size as u64))?;
            f.write_all(&internal.flushing_pages[i * page_size..(i + 1) * page_size])?;
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
    pub(crate) fn checkpoint(&self, wal: &Wal) -> anyhow::Result<()> {
        for frame_id in 0..self.n {
            let (meta, buffer) = {
                let internal = self.internal.write();

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
            let mut frame = meta.write();
            if let Some(ref wal_info) = frame.wal {
                wal.sync(wal_info.page)?;
                frame.wal = None;
                let frame = RwLockWriteGuard::downgrade(frame);

                Self::encode(&frame, buffer)?;
                Self::flush_page(&self.f, &self.double_buff_f, frame.id, buffer)?;
            }
        }

        Ok(())
    }

    fn decode(&self, pgid: PageId, meta: &mut PageMeta, buff: &mut [u8]) -> anyhow::Result<()> {
        assert!(buff.len() == self.page_size);
        meta.id = pgid;

        {
            let mut f = self.f.lock();
            // TODO: try to seek and read using a single syscall
            f.seek(SeekFrom::Start(pgid.get() * self.page_size as u64))?;
            f.read_exact(buff)?;
        }

        let ok = Self::decode_internal(self.page_size, meta, buff)?;
        if !ok {
            return Err(anyhow!(
                "page {} is corrupted, checksum mismatch",
                pgid.get(),
            ));
        }

        if meta.id != pgid {
            return Err(anyhow!(
                "page {} is written with invalid pgid information {}",
                pgid.get(),
                meta.id.get(),
            ));
        }

        Ok(())
    }

    fn decode_internal(
        page_size: usize,
        meta: &mut PageMeta,
        buff: &mut [u8],
    ) -> anyhow::Result<bool> {
        let header = &buff[..PAGE_HEADER_SIZE];
        let payload = &buff[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE];

        let buff_checksum = &buff[page_size - PAGE_FOOTER_SIZE..];
        let buff_version = &header[..2];
        let buff_kind = &header[2];
        let _ = &header[3..8];
        let buff_rec_lsn = &header[8..16];
        let buff_page_lsn = &header[16..24];
        let buff_page_id = &header[24..32];
        let buff_checksum_content = &buff[..page_size - PAGE_FOOTER_SIZE];

        let checksum = crc64::crc64(0, buff_checksum_content);
        let page_sum = u64::from_be_bytes(buff_checksum.try_into().unwrap());
        if checksum != page_sum {
            return Ok(false);
        }
        let version = u16::from_be_bytes(buff_version.try_into().unwrap());
        if version != 0 {
            return Err(anyhow!("page version {} is not supported", version));
        }

        let rec_lsn = Lsn::from_be_bytes(buff_rec_lsn.try_into().unwrap());
        let page_lsn = Lsn::from_be_bytes(buff_page_lsn.try_into().unwrap());
        let Some(page_id) = PageId::from_be_bytes(buff_page_id.try_into().unwrap()) else {
            return Err(anyhow!("found an empty page_id field when decoding page",));
        };

        let kind = match buff_kind {
            0 => PageKind::None,
            1 => Self::decode_interior_page(payload)?,
            2 => Self::decode_leaf_page(buff)?,
            3 => Self::decode_overflow_page(buff)?,
            4 => Self::decode_freelist_page(buff)?,
            _ => return Err(anyhow!("page kind {buff_kind} is not recognized")),
        };
        meta.id = page_id;
        meta.kind = kind;

        if let Some(rec_lsn) = rec_lsn {
            let Some(page_lsn) = page_lsn else {
                return Err(anyhow!(
                    "page {} has rec_lsn but no page_lsn",
                    meta.id.get()
                ));
            };
            meta.wal = Some(PageWalInfo {
                rec: rec_lsn,
                page: page_lsn,
            });
        } else {
            if page_lsn.is_some() {
                return Err(anyhow!(
                    "page {} has page_lsn but no rec_lsn",
                    meta.id.get()
                ));
            }
            meta.wal = None;
        }

        Ok(true)
    }

    fn decode_interior_page(payload: &[u8]) -> anyhow::Result<PageKind> {
        let header = &payload[..INTERIOR_PAGE_HEADER_SIZE];
        let buff_last = &header[..8];
        let buff_count = &header[8..10];
        let buff_offset = &header[10..12];
        _ = &header[12..16];

        let Some(last) = PageId::from_be_bytes(buff_last.try_into().unwrap()) else {
            return Err(anyhow!("got zero last ptr on interior page"));
        };
        let count = u16::from_be_bytes(buff_count.try_into().unwrap());
        let offset = u16::from_be_bytes(buff_offset.try_into().unwrap());

        let mut remaining = payload.len() - INTERIOR_PAGE_HEADER_SIZE;
        for i in 0..count {
            let cell_offset = INTERIOR_PAGE_HEADER_SIZE + INTERIOR_PAGE_CELL_SIZE * i as usize;
            let buf =
                PageId::from_be_bytes(payload[cell_offset..cell_offset + 8].try_into().unwrap());
            if buf.is_none() {
                return Err(anyhow!("got zero ptr on interior page"));
            }

            let cell = get_interior_cell(payload, i as usize);
            remaining -= INTERIOR_PAGE_CELL_SIZE + cell.raw().len();
        }

        Ok(PageKind::Interior {
            count: count as usize,
            offset: offset as usize,
            remaining,
            last,
        })
    }

    fn decode_leaf_page(buff: &[u8]) -> anyhow::Result<PageKind> {
        let page_size = buff.len();

        let payload = &buff[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE];
        let header = &payload[..LEAF_PAGE_HEADER_SIZE];
        let buff_next = &header[..8];
        let buff_count = &header[8..10];
        let buff_offset = &header[10..12];
        _ = &header[12..16];

        let next = PageId::from_be_bytes(buff_next.try_into().unwrap());
        let count = u16::from_be_bytes(buff_count.try_into().unwrap());
        let offset = u16::from_be_bytes(buff_offset.try_into().unwrap());

        let mut remaining = payload.len() - LEAF_PAGE_HEADER_SIZE;
        for i in 0..count {
            let cell = get_leaf_cell(buff, i as usize);
            remaining -= LEAF_PAGE_CELL_SIZE + cell.raw().len();
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
        let buff_next = &header[..8];
        let buff_size = &header[8..10];
        _ = &header[10..16];

        let next = PageId::from_be_bytes(buff_next.try_into().unwrap());
        let size = u16::from_be_bytes(buff_size.try_into().unwrap());

        Ok(PageKind::Overflow {
            next,
            size: size as usize,
        })
    }

    fn decode_freelist_page(buff: &[u8]) -> anyhow::Result<PageKind> {
        let page_size = buff.len();

        let payload = &buff[PAGE_HEADER_SIZE..page_size - PAGE_FOOTER_SIZE];
        let header = &payload[..FREELIST_PAGE_HEADER_SIZE];
        let buff_next = &header[..8];
        let buff_count = &header[8..10];
        _ = &header[10..16];

        let next = PageId::from_be_bytes(buff_next.try_into().unwrap());
        let count = u16::from_be_bytes(buff_count.try_into().unwrap());

        Ok(PageKind::Freelist {
            next,
            count: count as usize,
        })
    }

    fn encode(meta: &PageMeta, buff: &mut [u8]) -> anyhow::Result<()> {
        let page_size = buff.len();
        let header = &mut buff[..PAGE_HEADER_SIZE];

        header[..2].copy_from_slice(&0u16.to_be_bytes());

        let kind = match meta.kind {
            PageKind::None => 0,
            PageKind::Interior { .. } => 1,
            PageKind::Leaf { .. } => 2,
            PageKind::Overflow { .. } => 3,
            PageKind::Freelist { .. } => 4,
        };
        header[2] = kind;
        header[3..8].copy_from_slice(&[0, 0, 0, 0, 0]);

        let (rec_lsn, page_lsn) = if let Some(ref wal_info) = meta.wal {
            (Some(wal_info.rec), Some(wal_info.page))
        } else {
            (None, None)
        };

        header[8..16].copy_from_slice(&rec_lsn.to_be_bytes());
        header[16..24].copy_from_slice(&page_lsn.to_be_bytes());
        header[24..32].copy_from_slice(&meta.id.to_be_bytes());

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
                header[..8].copy_from_slice(&last.0.get().to_be_bytes());
                header[8..10].copy_from_slice(&(*count as u16).to_be_bytes());
                header[10..12].copy_from_slice(&(*offset as u16).to_be_bytes());
            }
            PageKind::Leaf {
                count,
                offset,
                next,
                remaining: _,
            } => {
                let header = &mut payload_buff[..LEAF_PAGE_HEADER_SIZE];
                let next = next.map(|p| p.0.get()).unwrap_or(0);
                header[..8].copy_from_slice(&next.to_be_bytes());
                header[8..10].copy_from_slice(&(*count as u16).to_be_bytes());
                header[10..12].copy_from_slice(&(*offset as u16).to_be_bytes());
            }
            PageKind::Overflow { next, size } => {
                let header = &mut payload_buff[..OVERFLOW_PAGE_HEADER_SIZE];
                let next = next.map(|p| p.0.get()).unwrap_or(0);
                header[..8].copy_from_slice(&next.to_be_bytes());
                header[8..10].copy_from_slice(&(*size as u16).to_be_bytes());
            }
            PageKind::Freelist { next, count } => {
                let header = &mut payload_buff[..FREELIST_PAGE_HEADER_SIZE];
                let next = next.map(|p| p.0.get()).unwrap_or(0);
                header[..8].copy_from_slice(&next.to_be_bytes());
                header[8..10].copy_from_slice(&(*count as u16).to_be_bytes());
            }
        }

        let checksum = crc64::crc64(0, &buff[..page_size - PAGE_FOOTER_SIZE]);
        buff[page_size - 8..].copy_from_slice(&checksum.to_be_bytes());

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
            f.seek(SeekFrom::Start(pgid.0.get() * self.page_size as u64))?;
            f.write_all(buffer)?;
        }

        // TODO(important): this is not safe. We can get non-atomic write.
        // In order to make the write atomic, we have to do a double-write. Write to a shadow page
        // first, then to the actual page.
        f.sync_all()?;
        Ok(())
    }

    fn release(&self, frame_id: usize, is_mutated: bool) {
        let mut internal = self.internal.write();
        internal.ref_count[frame_id] -= 1;

        let now_free = if internal.ref_count[frame_id] == 0 {
            internal.free_frames.insert(frame_id);
            true
        } else {
            false
        };

        let maybe_clean = if is_mutated {
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

struct PageMeta {
    id: PageId,
    kind: PageKind,

    // TODO: maybe we don't need to `is_mutated` field. We can
    // infer it from the `rec_lsn` and `page_lsn` fields. If
    // they are None, then it's a clean page, otherwise it's
    // a dirty page.
    // is_mutated: bool,
    // rec_lsn: Option<Lsn>,
    // page_lsn: Option<Lsn>,
    wal: Option<PageWalInfo>,
}

struct PageWalInfo {
    rec: Lsn,
    page: Lsn,
}

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
    pub(crate) fn id(&self) -> PageId {
        self.meta.id
    }

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
        self.pager.release(self.frame_id, self.meta.wal.is_some());
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
        self.meta.wal.as_ref().map(|wal| wal.page)
    }

    pub(crate) fn init_interior(
        mut self,
        ctx: LogContext<'_>,
        last: PageId,
    ) -> anyhow::Result<Option<InteriorPageWrite<'a>>> {
        if let PageKind::None = self.meta.kind {
            let pgid = self.id();
            record_mutation(
                self.txid,
                ctx,
                &mut self.meta,
                WalRecord::InteriorInit { pgid, last },
                WalRecord::InteriorInit { pgid, last },
            )?;

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
        let LogContext::Redo(lsn) = ctx else {
            panic!("set_interior only can be used for redo-ing wal");
        };

        let pgid = self.id();
        record_redo_mutation(lsn, &mut self.meta);

        self.meta.kind = Pager::decode_interior_page(payload)?;
        self.buffer.copy_from_slice(payload);

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

    pub(crate) fn init_leaf(
        mut self,
        ctx: LogContext<'_>,
    ) -> anyhow::Result<Option<LeafPageWrite<'a>>> {
        if let PageKind::None = self.meta.kind {
            let pgid = self.id();
            record_mutation(
                self.txid,
                ctx,
                &mut self.meta,
                WalRecord::LeafInit { pgid },
                WalRecord::LeafInit { pgid },
            )?;

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

        Ok(self.into_leaf())
    }

    pub(crate) fn into_leaf(self) -> Option<LeafPageWrite<'a>> {
        if let PageKind::Leaf { .. } = &self.meta.kind {
            Some(LeafPageWrite(self))
        } else {
            None
        }
    }
}

// TODO: during recovery, we need to pass the LSN of the log to upadte this page's rec_lsn and page_lsn.
fn record_mutation(
    txid: TxId,
    ctx: LogContext<'_>,
    meta: &mut PageMeta,
    entry: WalRecord,
    compensation_entry: WalRecord,
) -> anyhow::Result<()> {
    let entry = if ctx.is_undo() {
        compensation_entry
    } else {
        entry
    };

    let lsn = match ctx {
        LogContext::Runtime(wal) => wal.append(txid, None, entry)?,
        LogContext::Undo(wal, clr) => wal.append(txid, Some(clr), entry)?,
        LogContext::Redo(lsn) => lsn,
    };

    if let Some(ref mut wal_info) = meta.wal {
        wal_info.page = lsn;
    } else {
        meta.wal = Some(PageWalInfo {
            rec: lsn,
            page: lsn,
        });
    }

    Ok(())
}

fn record_redo_mutation(lsn: Lsn, meta: &mut PageMeta) {
    if let Some(ref mut wal_info) = meta.wal {
        wal_info.page = lsn;
    } else {
        meta.wal = Some(PageWalInfo {
            rec: lsn,
            page: lsn,
        });
    }
}

pub(crate) trait BTreePage<'a> {
    type Cell: BTreeCell;
    fn count(&'a self) -> usize;
    fn get(&'a self, index: usize) -> Self::Cell;
}

pub(crate) struct InteriorPageRead<'a>(PageRead<'a>);

impl<'a, 'b> BTreePage<'b> for InteriorPageRead<'a> {
    type Cell = InteriorCell<'b>;

    fn count(&'b self) -> usize {
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
    fn id(&self) -> PageId {
        self.0.meta.id
    }

    pub(crate) fn last(&self) -> PageId {
        let PageKind::Interior { last, .. } = self.0.meta.kind else {
            unreachable!();
        };
        last
    }

    pub(crate) fn might_split(&self) -> bool {
        let PageKind::Interior { remaining, .. } = self.0.meta.kind else {
            unreachable!();
        };
        interior_might_split(self.0.pager.page_size, remaining)
    }
}

fn get_interior_cell(buff: &[u8], index: usize) -> InteriorCell<'_> {
    let cell_offset = INTERIOR_PAGE_HEADER_SIZE + INTERIOR_PAGE_CELL_SIZE * index;
    let cell = &buff[cell_offset..cell_offset + INTERIOR_PAGE_CELL_SIZE];
    let offset = u16::from_be_bytes(cell[20..22].try_into().unwrap()) as usize;
    let size = u16::from_be_bytes(cell[22..24].try_into().unwrap()) as usize;
    let offset = offset - PAGE_HEADER_SIZE;
    let raw = &buff[offset..offset + size];
    InteriorCell { cell, raw }
}

fn interior_might_split(page_size: usize, remaining: usize) -> bool {
    let payload_size = page_size - PAGE_HEADER_SIZE - INTERIOR_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
    let max_before_overflow = payload_size / 4 - INTERIOR_PAGE_CELL_SIZE;
    let min_content_not_overflow = max_before_overflow / 2;
    let remaining = if remaining < INTERIOR_PAGE_CELL_SIZE {
        0
    } else {
        remaining - INTERIOR_PAGE_CELL_SIZE
    };
    remaining < min_content_not_overflow
}

pub(crate) struct InteriorCell<'a> {
    cell: &'a [u8],
    raw: &'a [u8],
}

pub(crate) trait BTreeCell {
    fn raw(&self) -> &[u8];
    fn key_size(&self) -> usize;
    fn overflow(&self) -> Option<PageId>;
}

impl BTreeCell for InteriorCell<'_> {
    fn raw(&self) -> &[u8] {
        self.raw
    }

    fn key_size(&self) -> usize {
        u32::from_be_bytes(self.cell[16..20].try_into().unwrap()) as usize
    }

    fn overflow(&self) -> Option<PageId> {
        PageId::from_be_bytes(self.cell[8..16].try_into().unwrap())
    }
}

impl InteriorCell<'_> {
    pub(crate) fn ptr(&self) -> PageId {
        PageId::from_be_bytes(self.cell[0..8].try_into().unwrap()).unwrap()
    }
}

pub(crate) struct InteriorPageWrite<'a>(PageWrite<'a>);

impl<'a, 'b> BTreePage<'b> for InteriorPageWrite<'a> {
    type Cell = InteriorCell<'b>;

    fn count(&'b self) -> usize {
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
        let pgid = self.id();
        record_mutation(
            self.0.txid,
            ctx,
            &mut self.0.meta,
            WalRecord::InteriorReset {
                pgid,
                page_version: 0,
                payload: &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            },
            WalRecord::InteriorUndoReset { pgid },
        )?;

        self.0.meta.kind = PageKind::None;
        Ok(self.0)
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
        let total_size = content.remaining();
        let payload_size =
            self.0.buffer.len() - PAGE_HEADER_SIZE - INTERIOR_PAGE_HEADER_SIZE - PAGE_FOOTER_SIZE;
        let max_before_overflow = payload_size / 4 - INTERIOR_PAGE_CELL_SIZE;
        let min_content_not_overflow = max_before_overflow / 2;
        let PageKind::Interior { remaining, .. } = self.0.meta.kind else {
            unreachable!();
        };
        if remaining < INTERIOR_PAGE_CELL_SIZE {
            return Ok(false);
        }
        let remaining = remaining - INTERIOR_PAGE_CELL_SIZE;
        if remaining < min_content_not_overflow && remaining < total_size {
            return Ok(false);
        }

        let raw_size = std::cmp::min(max_before_overflow, total_size);
        let raw_size = std::cmp::min(raw_size, remaining);

        let content_offset = self.insert_cell(i, ptr, overflow, key_size, raw_size);
        content.put(&mut self.0.buffer[content_offset..content_offset + raw_size])?;
        let pgid = self.id();

        // TODO(important): record the mutation before the insert cell happen. It is important
        // to make sure that the WAL is written. If the page is mutated, but the WAL is not
        // written, we might have a corrupted page.
        record_mutation(
            self.0.txid,
            ctx,
            &mut self.0.meta,
            WalRecord::InteriorInsert {
                pgid,
                index: i,
                raw: &self.0.buffer[content_offset..content_offset + raw_size],
                ptr,
                key_size,
                overflow,
            },
            WalRecord::InteriorInsert {
                pgid,
                index: i,
                raw: &self.0.buffer[content_offset..content_offset + raw_size],
                ptr,
                key_size,
                overflow,
            },
        )?;

        Ok(true)
    }

    fn insert_cell(
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
        let added = INTERIOR_PAGE_CELL_SIZE + raw_size;
        let current_cell_size =
            PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_PAGE_CELL_SIZE * count;
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
            let x = PAGE_HEADER_SIZE
                + INTERIOR_PAGE_HEADER_SIZE
                + INTERIOR_PAGE_CELL_SIZE * (*count - i);
            let (a, b) = self.0.buffer.split_at_mut(x);
            b[..INTERIOR_PAGE_CELL_SIZE].copy_from_slice(&a[a.len() - INTERIOR_PAGE_CELL_SIZE..]);
        }

        let cell_offset =
            PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_PAGE_CELL_SIZE * index;
        let cell = &mut self.0.buffer[cell_offset..cell_offset + INTERIOR_PAGE_CELL_SIZE];

        *offset -= raw_size;
        *remaining -= added;
        *count += 1;

        cell[0..8].copy_from_slice(&ptr.0.get().to_be_bytes());
        cell[8..16].copy_from_slice(&overflow.map(|p| p.0.get()).unwrap_or(0).to_be_bytes());
        cell[16..20].copy_from_slice(&(key_size as u32).to_be_bytes());
        cell[20..22].copy_from_slice(&(*offset as u16).to_be_bytes());
        cell[22..24].copy_from_slice(&(raw_size as u16).to_be_bytes());

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

            let cell_offset =
                PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_PAGE_CELL_SIZE * i;
            let cell = &mut self.0.buffer[cell_offset..cell_offset + INTERIOR_PAGE_CELL_SIZE];
            cell[20..22].copy_from_slice(&(new_offset as u16).to_be_bytes());
        }

        let PageKind::Interior { ref mut offset, .. } = self.0.meta.kind else {
            unreachable!();
        };
        *offset = new_offset;
    }

    pub(crate) fn delete(&mut self, ctx: LogContext<'_>, index: usize) -> anyhow::Result<()> {
        let id = self.0.meta.id;

        let cell = get_interior_cell(
            &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            index,
        );
        let content_offset = u16::from_be_bytes(cell.cell[20..22].try_into().unwrap()) as usize;
        let content_size = u16::from_be_bytes(cell.cell[22..24].try_into().unwrap()) as usize;

        let pgid = self.id();
        record_mutation(
            self.0.txid,
            ctx,
            &mut self.0.meta,
            WalRecord::InteriorDelete {
                pgid,
                index,
                old_raw: &self.0.buffer[content_offset..content_offset + content_size],
                old_ptr: cell.ptr(),
                old_overflow: cell.overflow(),
                old_key_size: cell.key_size(),
            },
            WalRecord::InteriorUndoDelete { pgid, index },
        )?;

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
        *remaining += INTERIOR_PAGE_CELL_SIZE + content_size;
        *count -= 1;

        for i in index..*count {
            let x = PAGE_HEADER_SIZE + INTERIOR_PAGE_HEADER_SIZE + INTERIOR_PAGE_CELL_SIZE * i;
            let (a, b) = self.0.buffer.split_at_mut(x + INTERIOR_PAGE_CELL_SIZE);
            let a_len = a.len();
            a[a_len - INTERIOR_PAGE_CELL_SIZE..].copy_from_slice(&b[..INTERIOR_PAGE_CELL_SIZE]);
        }

        Ok(())
    }

    pub(crate) fn might_split(&self) -> bool {
        let PageKind::Interior { remaining, .. } = self.0.meta.kind else {
            unreachable!();
        };
        interior_might_split(self.0.pager.page_size, remaining)
    }
}

pub(crate) struct LeafPageRead<'a>(PageRead<'a>);

impl<'a, 'b> BTreePage<'b> for LeafPageRead<'a> {
    type Cell = LeafCell<'b>;
    fn count(&'b self) -> usize {
        let PageKind::Leaf { count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        count
    }

    fn get(&'b self, index: usize) -> Self::Cell {
        get_leaf_cell(self.0.buffer, index)
    }
}

impl<'a> LeafPageRead<'a> {
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
        get_leaf_cell(self.0.buffer, index)
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
    fn count(&'b self) -> usize {
        let PageKind::Leaf { count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        count
    }

    fn get(&'b self, index: usize) -> Self::Cell {
        get_leaf_cell(self.0.buffer, index)
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
        get_leaf_cell(self.0.buffer, index)
    }

    pub(crate) fn next(&self) -> Option<PageId> {
        let PageKind::Leaf { next, .. } = self.0.meta.kind else {
            unreachable!();
        };
        next
    }

    pub(crate) fn reset(mut self, ctx: LogContext<'_>) -> anyhow::Result<PageWrite<'a>> {
        let pgid = self.id();

        record_mutation(
            self.0.txid,
            ctx,
            &mut self.0.meta,
            WalRecord::LeafReset {
                pgid,
                page_version: 0,
                payload: &self.0.buffer[PAGE_HEADER_SIZE..self.0.buffer.len() - PAGE_FOOTER_SIZE],
            },
            WalRecord::LeafUndoReset { pgid },
        )?;

        self.0.meta.kind = PageKind::None;
        Ok(self.0)
    }

    pub(crate) fn delete(&mut self, ctx: LogContext<'_>, index: usize) -> anyhow::Result<()> {
        let pgid = self.0.meta.id;
        let cell = get_leaf_cell(self.0.buffer, index);
        let content_offset = u16::from_be_bytes(cell.cell[16..18].try_into().unwrap()) as usize;
        let content_size = u16::from_be_bytes(cell.cell[18..20].try_into().unwrap()) as usize;

        record_mutation(
            self.0.txid,
            ctx,
            &mut self.0.meta,
            WalRecord::LeafDelete {
                pgid,
                index,
                old_raw: &self.0.buffer[content_offset..content_offset + content_size],
                old_overflow: cell.overflow(),
                old_key_size: cell.key_size(),
                old_val_size: cell.val_size(),
            },
            WalRecord::LeafUndoDelete { pgid, index },
        )?;

        let PageKind::Leaf {
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
        *remaining += LEAF_PAGE_CELL_SIZE + content_size;
        *count -= 1;

        for i in index..*count {
            let x = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_PAGE_CELL_SIZE * i;
            let (a, b) = self.0.buffer.split_at_mut(x + LEAF_PAGE_CELL_SIZE);
            let a_len = a.len();
            a[a_len - LEAF_PAGE_CELL_SIZE..].copy_from_slice(&b[..LEAF_PAGE_CELL_SIZE]);
        }

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
        let max_before_overflow = payload_size / 4 - LEAF_PAGE_CELL_SIZE;
        let min_content_not_overflow = max_before_overflow / 2;
        let PageKind::Leaf { remaining, .. } = self.0.meta.kind else {
            unreachable!();
        };
        if remaining < LEAF_PAGE_CELL_SIZE {
            return Ok(false);
        }
        let remaining = remaining - LEAF_PAGE_CELL_SIZE;
        if remaining < min_content_not_overflow && remaining < content_size {
            return Ok(false);
        }

        let raw_size = std::cmp::min(max_before_overflow, content_size);
        let raw_size = std::cmp::min(raw_size, remaining);

        let reserved_offset = self.reserve_cell(i, key_size, value_size, raw_size);
        content.put(&mut self.0.buffer[reserved_offset..reserved_offset + raw_size])?;

        let pgid = self.id();
        record_mutation(
            self.0.txid,
            ctx,
            &mut self.0.meta,
            WalRecord::LeafInsert {
                pgid,
                index: i,
                raw: &self.0.buffer[reserved_offset..reserved_offset + raw_size],
                overflow,
                key_size,
                value_size,
            },
            WalRecord::LeafInsert {
                pgid,
                index: i,
                raw: &self.0.buffer[reserved_offset..reserved_offset + raw_size],
                overflow,
                key_size,
                value_size,
            },
        )?;

        self.insert_cell_meta(i, overflow, key_size, value_size, raw_size);
        Ok(true)
    }

    fn reserve_cell(
        &mut self,
        index: usize,
        key_size: usize,
        val_size: usize,
        raw_size: usize,
    ) -> usize {
        let added = LEAF_PAGE_CELL_SIZE + raw_size;
        let PageKind::Leaf { offset, count, .. } = self.0.meta.kind else {
            unreachable!();
        };
        let current_cell_size =
            PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_PAGE_CELL_SIZE * count;
        if current_cell_size + added > offset {
            self.rearrange();
        }

        let PageKind::Leaf {
            ref mut offset,
            ref mut remaining,
            ref mut count,
            ..
        } = self.0.meta.kind
        else {
            unreachable!();
        };
        assert!(current_cell_size + added <= *offset);

        *offset - raw_size
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
        let added = LEAF_PAGE_CELL_SIZE + raw_size;
        let PageKind::Leaf {
            ref mut offset,
            ref mut count,
            ref mut remaining,
            ..
        } = self.0.meta.kind
        else {
            unreachable!();
        };
        let current_cell_size =
            PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_PAGE_CELL_SIZE * *count;
        assert!(current_cell_size + added <= *offset);

        let shifted = *count - index;
        for i in 0..shifted {
            let x = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_PAGE_CELL_SIZE * (*count - i);
            let (a, b) = self.0.buffer.split_at_mut(x);
            b[..LEAF_PAGE_CELL_SIZE].copy_from_slice(&a[a.len() - LEAF_PAGE_CELL_SIZE..]);
        }

        let cell_offset = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_PAGE_CELL_SIZE * index;
        let cell = &mut self.0.buffer[cell_offset..cell_offset + LEAF_PAGE_CELL_SIZE];

        *offset -= raw_size;
        *remaining -= added;
        *count += 1;
        cell[0..8].copy_from_slice(&overflow.map(|p| p.0.get()).unwrap_or(0).to_be_bytes());
        cell[8..12].copy_from_slice(&(key_size as u32).to_be_bytes());
        cell[12..16].copy_from_slice(&(val_size as u32).to_be_bytes());
        cell[16..18].copy_from_slice(&(*offset as u16).to_be_bytes());
        cell[18..20].copy_from_slice(&(raw_size as u16).to_be_bytes());
    }

    fn rearrange(&mut self) {
        // TODO: try not to copy
        let copied = self.0.buffer.to_vec();

        let mut new_offset = self.0.pager.page_size - PAGE_FOOTER_SIZE;
        for i in 0..self.count() {
            let copied_cell = get_leaf_cell(&copied, i);
            let copied_content = copied_cell.raw();
            new_offset -= copied_content.len();
            self.0.buffer[new_offset..new_offset + copied_content.len()]
                .copy_from_slice(copied_content);

            let cell_offset = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_PAGE_CELL_SIZE * i;
            let cell = &mut self.0.buffer[cell_offset..cell_offset + LEAF_PAGE_CELL_SIZE];
            cell[16..18].copy_from_slice(&(new_offset as u16).to_be_bytes());
        }

        let PageKind::Leaf { ref mut offset, .. } = self.0.meta.kind else {
            unreachable!();
        };
        *offset = new_offset;
    }
}

fn get_leaf_cell(buff: &[u8], index: usize) -> LeafCell<'_> {
    let cell_offset = PAGE_HEADER_SIZE + LEAF_PAGE_HEADER_SIZE + LEAF_PAGE_CELL_SIZE * index;
    let cell = &buff[cell_offset..cell_offset + LEAF_PAGE_CELL_SIZE];
    let offset = u16::from_be_bytes(cell[16..18].try_into().unwrap()) as usize;
    let size = u16::from_be_bytes(cell[18..20].try_into().unwrap()) as usize;
    let raw = &buff[offset..offset + size];
    LeafCell { cell, raw }
}

pub(crate) struct LeafCell<'a> {
    cell: &'a [u8],
    raw: &'a [u8],
}

impl<'a> BTreeCell for LeafCell<'a> {
    fn raw(&self) -> &'a [u8] {
        self.raw
    }

    fn key_size(&self) -> usize {
        u32::from_be_bytes(self.cell[8..12].try_into().unwrap()) as usize
    }

    fn overflow(&self) -> Option<PageId> {
        PageId::from_be_bytes(self.cell[0..8].try_into().unwrap())
    }
}

impl<'a> LeafCell<'a> {
    pub(crate) fn val_size(&self) -> usize {
        u32::from_be_bytes(self.cell[12..16].try_into().unwrap()) as usize
    }
}

pub(crate) struct OverflowPageRead<'a>(PageRead<'a>);

impl<'a> OverflowPageRead<'a> {
    pub(crate) fn id(&self) -> PageId {
        self.0.meta.id
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::Bytes;
    use std::sync::Arc;

    #[test]
    fn test_pager_interior() {
        let page_size = 256;

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.wal");
        let mut file = File::create(file_path).unwrap();

        let double_buff_file_path = dir.path().join("test.wal");
        let mut double_buff_file = File::create(double_buff_file_path).unwrap();

        let wal_path = dir.path().join("test.wal");
        let mut wal_file = File::create(wal_path).unwrap();
        let wal = Arc::new(Wal::new(wal_file, 0, Lsn::new(64).unwrap(), page_size).unwrap());

        let pager = Pager::new(file, double_buff_file, page_size, 10).unwrap();
        let txid = TxId::new(1).unwrap();

        let mut page1 = pager.alloc(txid).unwrap();
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
