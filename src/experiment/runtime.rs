use std::fs;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;

pub(crate) type RwMutexReadGuard<'a, R, T> =
    <<R as Runtime>::RwMutex<T> as RwMutex<T>>::ReadGuard<'a>;
pub(crate) type RwMutexWriteGuard<'a, R, T> =
    <<R as Runtime>::RwMutex<T> as RwMutex<T>>::WriteGuard<'a>;
pub(crate) type Guard<'a, R, T> = <<R as Runtime>::Mutex<T> as Mutex<T>>::Guard<'a>;

pub(crate) trait Runtime: 'static {
    type Timer: Timer;
    type TimerHandle: TimerHandle;

    type Mutex<T: Send + Sync>: Mutex<T>;

    type RwMutex<T: Send + Sync>: RwMutex<T>;

    type JoinHandle: JoinHandle;

    type File: File;

    type AtomicUsize: Atomic<usize>;
    type AtomicU8: Atomic<u8>;
    type AtomicU16: Atomic<u16>;
    type AtomicU32: Atomic<u32>;
    type AtomicU64: Atomic<u64>;
    type AtomicIsize: Atomic<isize>;
    type AtomicI8: Atomic<i8>;
    type AtomicI16: Atomic<i16>;
    type AtomicI32: Atomic<i32>;
    type AtomicI64: Atomic<i64>;

    fn spawn(f: impl FnOnce() + Send + 'static) -> Self::JoinHandle;

    fn park();

    fn timer(duration: std::time::Duration) -> (Self::Timer, Self::TimerHandle);

    fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()>;
}

pub(crate) trait Timer: Send {
    fn wait(&mut self) -> bool;
}

pub(crate) trait TimerHandle: Clone {
    fn trigger(&self);
    fn close(&self);
}

pub(crate) trait Mutex<T: Send + Sync>: Send + Sync {
    type Guard<'a>: DerefMut<Target = T>
    where
        Self: 'a;

    fn new(data: T) -> Self;
    fn lock(& self) -> Self::Guard<'_>;
    fn try_lock(& self) -> Option<Self::Guard<'_>>;
}

pub(crate) trait RwMutex<T: Send + Sync>: Send + Sync {
    type ReadGuard<'a>: Deref<Target = T> + From<Self::WriteGuard<'a>>
    where
        Self: 'a;
    type WriteGuard<'a>: DerefMut<Target = T>
    where
        Self: 'a;

    fn new(data: T) -> Self;

    fn read(&self) -> Self::ReadGuard<'_>;

    fn write(&self) -> Self::WriteGuard<'_>;

    fn try_write(&self) -> Option<Self::WriteGuard<'_>>;
}

pub(crate) trait JoinHandle {
    fn join(self);
}

pub(crate) trait File: Sized + Send + Sync {
    fn open(path: impl AsRef<Path>) -> io::Result<Self>;

    fn metadata(&self) -> io::Result<fs::Metadata>;

    fn seek(&mut self, position: io::SeekFrom) -> io::Result<()>;

    fn read(&mut self, buff: &mut [u8]) -> io::Result<usize>;

    fn read_exact(&mut self, buff: &mut [u8]) -> io::Result<()>;

    fn write_all(&mut self, buff: &[u8]) -> io::Result<()>;

    fn sync(&mut self) -> io::Result<()>;

    fn truncate(&mut self, size: u64) -> io::Result<()>;
}

pub(crate) trait Atomic<T> {
    fn new(value: T) -> Self;
    fn load(&self) -> T;
    fn store(&self, value: T);
    fn compare_and_exchange(&self, old: T, new: T) -> bool;
    fn fetch_add(&self, delta: T) -> T;
}
