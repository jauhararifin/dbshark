use std::future::Future;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;

pub trait Runtime: 'static {
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

    async fn spawn<F>(name: &'static str, f: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static;

    fn timer(duration: std::time::Duration) -> (Self::Timer, Self::TimerHandle);

    async fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()>;
}

pub trait Timer: Send {
    async fn wait(&mut self) -> bool;
}

pub trait TimerHandle: Send + Sync {
    async fn trigger(&self);
}

pub trait Mutex<T: Send + Sync>: Send + Sync {
    type Guard<'a>: DerefMut<Target = T>
    where
        Self: 'a;

    fn new(data: T) -> Self;
    async fn lock(&self) -> Self::Guard<'_>;
    async fn try_lock(&self) -> Option<Self::Guard<'_>>;
}

pub trait MutexGuard<T: Send + Sync>: DerefMut<Target = T> {
    async fn unlock(self);
}

pub trait RwMutex<T: Send + Sync>: Send + Sync {
    type ReadGuard<'a>: Deref<Target = T> + From<Self::WriteGuard<'a>>
    where
        Self: 'a;
    type WriteGuard<'a>: DerefMut<Target = T>
    where
        Self: 'a;

    fn new(data: T) -> Self;

    async fn read(&self) -> Self::ReadGuard<'_>;

    async fn write(&self) -> Self::WriteGuard<'_>;

    async fn try_write(&self) -> Option<Self::WriteGuard<'_>>;
}

pub trait RwMutexReadGuard<'a, T> {
    async fn unlock(self);
}

pub trait JoinHandle {
    async fn join(self);
}

pub trait File: Sized + Send + Sync {
    async fn open(path: impl AsRef<Path>) -> io::Result<Self>;

    async fn is_file(&self) -> io::Result<bool>;

    async fn len(&self) -> io::Result<u64>;

    async fn seek(&mut self, position: io::SeekFrom) -> io::Result<()>;

    async fn read(&mut self, buff: &mut [u8]) -> io::Result<usize>;

    async fn read_exact(&mut self, buff: &mut [u8]) -> io::Result<()>;

    async fn write_all(&mut self, buff: &[u8]) -> io::Result<()>;

    async fn sync(&mut self) -> io::Result<()>;

    async fn truncate(&mut self, size: u64) -> io::Result<()>;
}

pub trait Atomic<T>: Send + Sync {
    fn new(value: T) -> Self;
    async fn load(&self) -> T;
    async fn compare_and_exchange(&self, old: T, new: T) -> bool;
    async fn fetch_add(&self, delta: T) -> T;
}
