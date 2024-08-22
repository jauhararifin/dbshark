use crate::id::TxId;
use crate::pager_v2::page::PageMeta;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) struct BufferPool {
    page_size: usize,
    n: usize,
    allocated: AtomicUsize,
    locks: Box<[RwLock<()>]>,
    metas: *mut MaybeUninit<PageMeta>,
    buffer: *mut u8,
}

unsafe impl std::marker::Send for BufferPool {}
unsafe impl std::marker::Sync for BufferPool {}

impl BufferPool {
    pub(crate) fn new(page_size: usize, n: usize) -> Self {
        let metas = (0..n)
            .map(|_| MaybeUninit::uninit())
            .collect::<Vec<_>>()
            .leak()
            .as_mut_ptr();
        let locks = (0..n)
            .map(|_| RwLock::new(()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            page_size,
            n,
            allocated: AtomicUsize::new(0),
            locks,
            metas,
            buffer: vec![0u8; page_size * n].leak().as_mut_ptr(),
        }
    }

    pub(crate) fn read(&self, txid: TxId, index: usize) -> ReadFrame {
        assert!(index < self.allocated.load(Ordering::SeqCst));
        assert!(index < self.n);

        let guard = self.locks[index].read();
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
            guard,
            txid,
            meta,
            buffer,
        }
    }

    pub(crate) fn write(&self, txid: TxId, index: usize) -> WriteFrame {
        assert!(index < self.allocated.load(Ordering::SeqCst));
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

        WriteFrame {
            index,
            guard,
            txid,
            meta,
            buffer,
        }
    }

    pub(crate) fn alloc(&self, txid: TxId, init: PageMeta) -> Option<WriteFrame> {
        let (guard, index) = loop {
            let allocated = self.allocated.load(Ordering::SeqCst);
            if allocated >= self.n {
                return None;
            }
            let Some(guard) = self.locks[allocated].try_write() else {
                continue;
            };
            let result = self.allocated.compare_exchange(
                allocated,
                allocated + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            if result.is_ok() {
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

impl Drop for BufferPool {
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

pub(crate) struct ReadFrame<'a> {
    pub(super) index: usize,
    pub(super) guard: RwLockReadGuard<'a, ()>,
    pub(super) txid: TxId,
    pub(super) meta: &'a PageMeta,
    pub(super) buffer: &'a [u8],
}

impl<'a> From<WriteFrame<'a>> for ReadFrame<'a> {
    fn from(value: WriteFrame<'a>) -> Self {
        Self {
            index: value.index,
            guard: RwLockWriteGuard::downgrade(value.guard),
            txid: value.txid,
            meta: value.meta,
            buffer: value.buffer,
        }
    }
}

pub(crate) struct WriteFrame<'a> {
    pub(super) index: usize,
    pub(super) guard: RwLockWriteGuard<'a, ()>,
    pub(super) txid: TxId,
    pub(super) meta: &'a mut PageMeta,
    pub(super) buffer: &'a mut [u8],
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{Lsn, PageId};
    use crate::pager_v2::page::PageKind;
    use rand::Rng;
    use std::sync::atomic::AtomicI32;

    #[test]
    fn test_allocation() {
        let iteration = 100;
        let txid = TxId::new(1).unwrap();
        for _ in 0..iteration {
            let pool = BufferPool::new(128, 100);
            let success_count = AtomicI32::new(0);
            let failed_count = AtomicI32::new(0);

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
                            success_count.fetch_add(1, Ordering::SeqCst);
                        } else {
                            failed_count.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                }
            });

            assert_eq!(100, success_count.into_inner());
            assert_eq!(50, failed_count.into_inner());
        }
    }

    #[test]
    fn test_concurrent_read_write() {
        let n = 100;
        let pool = BufferPool::new(128, n);
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
                        let buff = pool.read(txid, index);
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
            let pool = BufferPool::new(128, n);
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
                            pool.read(txid, index);
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
