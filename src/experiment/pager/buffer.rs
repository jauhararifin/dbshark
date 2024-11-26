use super::page::PageMeta;
use crate::experiment::id::TxId;
use crate::experiment::runtime::{Atomic, Runtime, RwMutex, RwMutexReadGuard, RwMutexWriteGuard};
use std::mem::MaybeUninit;

pub(crate) struct BufferPool<R: Runtime> {
    page_size: usize,
    n: usize,
    allocated: R::AtomicUsize,
    locks: Box<[R::RwMutex<()>]>,
    metas: *mut MaybeUninit<PageMeta>,
    buffer: *mut u8,
}

unsafe impl<R: Runtime> std::marker::Send for BufferPool<R> {}
unsafe impl<R: Runtime> std::marker::Sync for BufferPool<R> {}

impl<R: Runtime> BufferPool<R> {
    pub(crate) fn new(page_size: usize, n: usize) -> Self {
        let metas = (0..n)
            .map(|_| MaybeUninit::uninit())
            .collect::<Vec<_>>()
            .leak()
            .as_mut_ptr();
        let locks = (0..n)
            .map(|_| R::RwMutex::new(()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            page_size,
            n,
            allocated: R::AtomicUsize::new(0),
            locks,
            metas,
            buffer: vec![0u8; page_size * n].leak().as_mut_ptr(),
        }
    }

    pub(crate) fn read(&self, index: usize) -> ReadFrame<R> {
        assert!(index < self.allocated.load());
        assert!(index < self.n);

        let _guard = self.locks[index].read();
        // SAFETY: since i < n, and self.metas' len is n, this operation is safe because
        // it will never point to address beyond buffer.
        let meta = unsafe { self.metas.add(index) };
        // SAFETY: It is guaranteed that there are no mutable reference to `self.metas[index]`
        // since we held a guard for self.locks[index] and we always acquire shared-lock for
        // `index` before we fetch `self.metas[index]` as shared.
        let meta = unsafe { &*meta };
        // SAFETY: Since i < self.allocated, we can be sure that `self.metas[index]` is initialized
        // during `alloc`.
        let meta = unsafe { meta.assume_init_ref() };

        let offset = index * self.page_size;
        // SAFETY: since i < n, and buffer's len is n * page_size, this operation is safe because
        // it will never point to address beyond buffer.
        let buffer = unsafe { self.buffer.add(offset) };
        // SAFETY: it's guaranteed that buffer doesn't have mutable reference since the meta
        // is locked with shared-lock and we always lock the meta first before grabbing the buffer.
        let buffer = unsafe { std::slice::from_raw_parts(buffer, self.page_size) };

        ReadFrame {
            index,
            _guard,
            meta,
            buffer,
        }
    }

    pub(crate) fn write(&self, txid: TxId, index: usize) -> WriteFrame<R> {
        let item = self.write_internal(index);
        WriteFrame {
            index,
            guard: item.guard,
            txid,
            meta: item.meta,
            buffer: item.buffer,
        }
    }

    pub(crate) fn write_internal(&self, index: usize) -> BufferPoolItem<R> {
        assert!(index < self.allocated.load());
        assert!(index < self.n);

        let guard = self.locks[index].write();
        // SAFETY: since i < n, and self.metas' len is n, this operation is safe because
        // it will never point to address beyond buffer.
        let meta = unsafe { self.metas.add(index) };
        // SAFETY: It is guaranteed that there are no mutable reference to `self.metas[index]`
        // since we held a guard for self.locks[index] and we always acquire exclusive-lock for
        // `index` before we fetch `self.metas[index]` as mutable.
        let meta = unsafe { &mut *meta };
        // SAFETY: Since i < self.allocated, we can be sure that `self.metas[index]` is initialized
        // during `alloc`.
        let meta = unsafe { meta.assume_init_mut() };
        let offset = index * self.page_size;

        // SAFETY: since i < n, and buffer's len is n * page_size, this operation is safe because
        // it will never point to address beyond buffer.
        let buffer = unsafe { self.buffer.add(offset) };
        // SAFETY: it's guaranteed that buffer doesn't have mutable reference yet since the meta
        // is locked with exclusive-lock and we always lock the meta first before grabbing the buffer.
        let buffer = unsafe { std::slice::from_raw_parts_mut(buffer, self.page_size) };

        BufferPoolItem {
            guard,
            meta,
            buffer,
        }
    }

    pub(crate) fn alloc(&self, txid: TxId, init: PageMeta) -> Option<WriteFrame<R>> {
        let (guard, index) = loop {
            let allocated = self.allocated.load();
            if allocated >= self.n {
                return None;
            }
            let Some(guard) = self.locks[allocated].try_write() else {
                continue;
            };
            let result = self
                .allocated
                .compare_and_exchange(allocated, allocated + 1);
            if result {
                break (guard, allocated);
            }
        };

        // SAFETY: since i < n, and self.metas' len is n, this operation is safe because
        // it will never point to address beyond buffer.
        let meta = unsafe { self.metas.add(index) };
        // SAFETY: It is guaranteed that there are no mutable reference to `self.metas[index]`
        // since we are the first one grabbing its lock and fetch_add the self.allocated.
        let meta = unsafe { &mut *meta };
        // SAFETY: It is guaranteed that thie meta only initialized once since the self.allocated
        // is already increased and we only allocated the meta from the self.allocated.
        meta.write(init);
        // SAFETY: we just initialized the meta in the line above.
        let meta = unsafe { meta.assume_init_mut() };

        let offset = index * self.page_size;
        // SAFETY: since i < n, and buffer's len is n * page_size, this operation is safe because
        // it will never point to address beyond buffer.
        let buffer = unsafe { self.buffer.add(offset) };
        // SAFETY: it's guaranteed that buffer doesn't have mutable reference yet since the meta
        // is locked with exclusive-lock and we always lock the meta first before grabbing the buffer.
        let buffer = unsafe { std::slice::from_raw_parts_mut(buffer, self.page_size) };

        Some(WriteFrame {
            index,
            guard,
            txid,
            meta,
            buffer,
        })
    }
}

impl<R: Runtime> Drop for BufferPool<R> {
    fn drop(&mut self) {
        unsafe {
            drop(Vec::from_raw_parts(self.metas, self.n, self.n));
            drop(Vec::from_raw_parts(
                self.buffer,
                self.page_size * self.n,
                self.page_size * self.n,
            ));
        }
    }
}

pub(crate) struct ReadFrame<'a, R: Runtime> {
    pub(crate) index: usize,
    _guard: RwMutexReadGuard<'a, R, ()>,
    pub(super) meta: &'a PageMeta,
    pub(super) buffer: &'a [u8],
}

impl<'a, R: Runtime> From<WriteFrame<'a, R>> for ReadFrame<'a, R> {
    fn from(value: WriteFrame<'a, R>) -> Self {
        Self {
            index: value.index,
            _guard: RwMutexReadGuard::<'a, R, ()>::from(value.guard),
            meta: value.meta,
            buffer: value.buffer,
        }
    }
}

pub(crate) struct WriteFrame<'a, R: Runtime> {
    pub(super) index: usize,
    guard: RwMutexWriteGuard<'a, R, ()>,
    pub(super) txid: TxId,
    pub(super) meta: &'a mut PageMeta,
    pub(super) buffer: &'a mut [u8],
}

impl<R: Runtime> std::fmt::Debug for WriteFrame<'_, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.meta.fmt(f)
    }
}

impl<R: Runtime> BufferPool<R> {
    pub(crate) fn walk(&self) -> impl Iterator<Item = BufferPoolItem<R>> {
        let allocated = self.allocated.load();
        let allocated = std::cmp::min(allocated, self.n);
        BufferPoolWalk {
            pool: self,
            i: 0,
            count: allocated,
        }
    }
}

pub(crate) struct BufferPoolWalk<'a, R: Runtime> {
    pool: &'a BufferPool<R>,
    i: usize,
    count: usize,
}

pub(crate) struct BufferPoolItem<'a, R: Runtime> {
    guard: RwMutexWriteGuard<'a, R, ()>,
    pub(super) meta: &'a mut PageMeta,
    pub(super) buffer: &'a mut [u8],
}

impl<'a, R: Runtime> Iterator for BufferPoolWalk<'a, R> {
    type Item = BufferPoolItem<'a, R>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.count {
            return None;
        }

        let item = self.pool.write_internal(self.i);
        self.i += 1;
        Some(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experiment::id::{Lsn, PageId};
    use crate::experiment::os::OsRuntime;
    use crate::experiment::pager::page::PageKind;
    use crate::experiment::runtime::{Atomic, Runtime};
    use rand::Rng;

    #[test]
    fn test_allocation() {
        let iteration = 100;
        let txid = TxId::new(1).unwrap();
        for _ in 0..iteration {
            let pool = BufferPool::<OsRuntime>::new(128, 100);
            let success_count = <OsRuntime as Runtime>::AtomicI32::new(0);
            let failed_count = <OsRuntime as Runtime>::AtomicI32::new(0);

            std::thread::scope(|scope| {
                for _ in 0..150 {
                    scope.spawn(|| {
                        let success = pool
                            .alloc(
                                txid,
                                PageMeta {
                                    id: PageId::new(1).unwrap(),
                                    kind: PageKind::None,
                                    lsn: Lsn::new(1),
                                    dirty: false,
                                },
                            )
                            .is_some();
                        if success {
                            success_count.fetch_add(1);
                        } else {
                            failed_count.fetch_add(1);
                        }
                    });
                }
            });

            assert_eq!(100, success_count.load());
            assert_eq!(50, failed_count.load());
        }
    }

    #[test]
    fn test_concurrent_read_write() {
        let n = 100;
        let pool = BufferPool::<OsRuntime>::new(128, n);
        let txid = TxId::new(1).unwrap();
        for i in 0u64..n as u64 {
            let result = pool.alloc(
                txid,
                PageMeta {
                    id: PageId::new(1000 + i).unwrap(),
                    kind: PageKind::None,
                    lsn: Lsn::new(100 + i),
                    dirty: false,
                },
            );
            assert!(result.is_some());
        }

        std::thread::scope(|scope| {
            let pool = &pool;
            let mut randomizer = rand::thread_rng();
            for _ in 0..150 {
                let index = randomizer.gen_range(0..n);
                let is_read = randomizer.gen_bool(0.5);
                scope.spawn(move || {
                    if is_read {
                        let buff = pool.read(index);
                        let x = buff.buffer[0];
                        assert!(buff.buffer.iter().all(|y| *y == x));
                    } else {
                        let buff = pool.write(txid, index);
                        let x = buff.buffer[0];
                        assert!(buff.buffer.iter().all(|y| *y == x));
                        buff.buffer.fill(1);
                    }
                });
            }
        });
    }

    #[test]
    #[should_panic]
    fn test_read_write_unallocated_frame() {
        let n = 100;
        let txid = TxId::new(1).unwrap();
        loop {
            let pool = BufferPool::<OsRuntime>::new(128, n);
            std::thread::scope(|scope| {
                let pool = &pool;
                let mut randomizer = rand::thread_rng();
                for _ in 0..150 {
                    let index = randomizer.gen_range(0..n);
                    let action = randomizer.gen_range(0..3);
                    scope.spawn(move || match action {
                        0 => {
                            pool.write(txid, index);
                        }
                        1 => {
                            pool.read(index);
                        }
                        2 => {
                            pool.alloc(
                                txid,
                                PageMeta {
                                    id: PageId::new(1).unwrap(),
                                    kind: PageKind::None,
                                    lsn: Lsn::new(1),
                                    dirty: false,
                                },
                            );
                        }
                        _ => unreachable!(),
                    });
                }
            });
        }
    }
}
