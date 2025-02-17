use std::future::Future;
use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;

pub trait Runtime: Send + Sync + 'static {
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

    fn spawn<F>(name: &'static str, f: F) -> impl Future<Output = Self::JoinHandle>
    where
        F: Future<Output = ()> + Send + 'static;

    fn timer(duration: std::time::Duration) -> (Self::Timer, Self::TimerHandle);

    fn create_dir_all<P: AsRef<Path>>(path: P) -> impl Future<Output = io::Result<()>>;
}

pub trait Timer: Send {
    fn wait(&mut self) -> impl Future<Output = bool> + Send;
}

pub trait TimerHandle: Send + Sync {
    fn trigger(&self) -> impl Future<Output = ()> + Send;
}

pub trait Mutex<T: Send + Sync>: Send + Sync {
    type Guard<'a>: DerefMut<Target = T> + Send
    where
        Self: 'a;

    fn new(data: T) -> Self;

    fn lock(&self) -> impl Future<Output = Self::Guard<'_>> + Send;

    fn try_lock(&self) -> impl Future<Output = Option<Self::Guard<'_>>> + Send;

    fn into_inner(self) -> impl Future<Output = T> + Send;
}

pub trait MutexGuard<T: Send + Sync>: DerefMut<Target = T> {
    fn unlock(self) -> impl Future<Output = ()>;
}

pub trait RwMutex<T: Send + Sync>: Send + Sync {
    type ReadGuard<'a>: RwMutexReadGuard<'a, T> + From<Self::WriteGuard<'a>>
    where
        Self: 'a;
    type WriteGuard<'a>: RwMutexWriteGuard<'a, T>
    where
        Self: 'a;

    fn new(data: T) -> Self;

    fn read(&self) -> impl Future<Output = Self::ReadGuard<'_>> + Send;

    fn write(&self) -> impl Future<Output = Self::WriteGuard<'_>> + Send;

    fn try_write(&self) -> impl Future<Output = Option<Self::WriteGuard<'_>>>;
}

pub trait RwMutexReadGuard<'a, T>: Deref<Target = T> + Send {
    // TODO: maybe we don't have to go this far?
    // or here is another idea
    // in tokio, just do the same logic for drop and unlock
    // but in simulation, dropping without unlocking should fail
    fn unlock(self) -> impl Future<Output = ()>;
}

pub trait RwMutexWriteGuard<'a, T>: DerefMut<Target = T> + Send {
    fn unlock(self) -> impl Future<Output = ()>;
}

pub trait JoinHandle: Send + Sync {
    fn join(self) -> impl Future<Output = ()>;
}

pub trait File: Sized + Send + Sync {
    fn open(path: &Path) -> impl Future<Output = io::Result<Self>> + Send;

    fn is_file(&self) -> impl Future<Output = io::Result<bool>> + Send;

    fn len(&self) -> impl Future<Output = io::Result<u64>> + Send;

    fn seek(&mut self, position: io::SeekFrom) -> impl Future<Output = io::Result<()>> + Send;

    fn read(&mut self, buff: &mut [u8]) -> impl Future<Output = io::Result<usize>> + Send;

    fn read_exact(&mut self, buff: &mut [u8]) -> impl Future<Output = io::Result<()>> + Send;

    fn write_all(&mut self, buff: &[u8]) -> impl Future<Output = io::Result<()>> + Send;

    fn sync(&mut self) -> impl Future<Output = io::Result<()>> + Send;

    fn truncate(&mut self, size: u64) -> impl Future<Output = io::Result<()>> + Send;

    fn close(self) -> impl Future<Output = std::io::Result<()>> + Send;
}

pub trait Atomic<T>: Send + Sync {
    fn new(value: T) -> Self;
    fn load(&self) -> impl Future<Output = T> + Send;
    fn compare_and_exchange(&self, old: T, new: T) -> impl Future<Output = bool> + Send;
    fn fetch_add(&self, delta: T) -> impl Future<Output = T> + Send;
}
