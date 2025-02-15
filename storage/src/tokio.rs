use crate::runtime::{
    File, JoinHandle, Mutex, MutexGuard, Runtime, RwMutex, RwMutexReadGuard, Timer, TimerHandle,
};
use parking_lot;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use tokio;

pub struct TokioRuntime;

impl Runtime for TokioRuntime {
    type Timer = TokioTimer;
    type TimerHandle = TokioTimerHandle;

    type Mutex<T: Send + Sync> = TokioMutex<T>;
    type RwMutex<T: Send + Sync> = TokioRwMutex<T>;

    type JoinHandle = TokioJoinHandle;

    type File = TokioFile;

    type AtomicUsize = AtomicUsize;
    type AtomicU8 = AtomicU8;
    type AtomicU16 = AtomicU16;
    type AtomicU32 = AtomicU32;
    type AtomicU64 = AtomicU64;
    type AtomicIsize = AtomicIsize;
    type AtomicI8 = AtomicI8;
    type AtomicI16 = AtomicI16;
    type AtomicI32 = AtomicI32;
    type AtomicI64 = AtomicI64;

    async fn spawn<F>(_name: &'static str, f: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::task::spawn(f);
        TokioJoinHandle(handle)
    }

    fn timer(duration: std::time::Duration) -> (Self::Timer, Self::TimerHandle) {
        let (sender, receiver) = tokio::sync::mpsc::channel(5);
        let timer = TokioTimer { duration, receiver };
        let handle = TokioTimerHandle { sender };
        (timer, handle)
    }

    async fn create_dir_all<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<()> {
        tokio::fs::create_dir_all(path.as_ref()).await
    }
}

pub struct TokioTimer {
    duration: std::time::Duration,
    receiver: tokio::sync::mpsc::Receiver<()>,
}

impl Timer for TokioTimer {
    async fn wait(&mut self) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(self.duration) => {
                true
            }
            res = self.receiver.recv() => {
                res.is_some()
            }
        }
    }
}

#[derive(Clone)]
pub struct TokioTimerHandle {
    sender: tokio::sync::mpsc::Sender<()>,
}

impl TimerHandle for TokioTimerHandle {
    async fn trigger(&self) {
        let _ = self.sender.send(()).await;
    }
}

pub struct TokioMutex<T> {
    inner: parking_lot::Mutex<T>,
}

impl<T: Send + Sync> Mutex<T> for TokioMutex<T> {
    type Guard<'a> = TokioGuard<'a, T>
    where
        Self: 'a;

    #[inline]
    fn new(data: T) -> Self {
        Self {
            inner: parking_lot::Mutex::new(data),
        }
    }

    #[inline]
    async fn lock(&self) -> Self::Guard<'_> {
        TokioGuard {
            inner: Some(self.inner.lock()),
        }
    }

    #[inline]
    async fn try_lock(&self) -> Option<Self::Guard<'_>> {
        self.inner
            .try_lock()
            .map(|guard| TokioGuard { inner: Some(guard) })
    }
}

pub struct TokioGuard<'a, T> {
    inner: Option<parking_lot::MutexGuard<'a, T>>,
}

impl<'a, T> Deref for TokioGuard<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap().deref()
    }
}

impl<'a, T> DerefMut for TokioGuard<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap().deref_mut()
    }
}

impl<'a, T: Send + Sync> MutexGuard<T> for TokioGuard<'a, T> {
    #[inline]
    async fn unlock(mut self) {
        self.inner.take();
    }
}

impl<'a, T> Drop for TokioGuard<'a, T> {
    #[inline]
    fn drop(&mut self) {
        if self.inner.is_none() {
            panic!("mutex guard is dropped without unlock")
        }
    }
}

pub struct TokioRwMutex<T> {
    inner: Option<parking_lot::RwLock<T>>,
}

impl<T: Send + Sync> RwMutex<T> for TokioRwMutex<T> {
    type ReadGuard<'a> = TokioReadGuard<'a, T>
    where
        Self: 'a;
    type WriteGuard<'a> = TokioWriteGuard<'a,T>
    where
        Self: 'a;

    #[inline]
    fn new(data: T) -> Self {
        Self {
            inner: Some(parking_lot::RwLock::new(data)),
        }
    }

    #[inline]
    async fn read(&self) -> Self::ReadGuard<'_> {
        TokioReadGuard {
            inner: Some(self.inner.as_ref().unwrap().read()),
        }
    }

    #[inline]
    async fn write(&self) -> Self::WriteGuard<'_> {
        TokioWriteGuard {
            inner: Some(self.inner.as_ref().unwrap().write()),
        }
    }

    #[inline]
    async fn try_write(&self) -> Option<Self::WriteGuard<'_>> {
        let guard = self.inner.as_ref().unwrap().try_write()?;
        Some(TokioWriteGuard { inner: Some(guard) })
    }
}

pub struct TokioReadGuard<'a, T> {
    inner: Option<parking_lot::RwLockReadGuard<'a, T>>,
}

impl<'a, T> From<TokioWriteGuard<'a, T>> for TokioReadGuard<'a, T> {
    #[inline]
    fn from(mut value: TokioWriteGuard<'a, T>) -> Self {
        Self {
            inner: Some(parking_lot::RwLockWriteGuard::downgrade(
                value.inner.take().unwrap(),
            )),
        }
    }
}

impl<'a, T> Deref for TokioReadGuard<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap().deref()
    }
}

impl<'a, T: Send + Sync> RwMutexReadGuard<'a, T> for TokioReadGuard<'a, T> {
    #[inline]
    async fn unlock(mut self) {
        self.inner.take();
    }
}

impl<'a, T> Drop for TokioReadGuard<'a, T> {
    #[inline]
    fn drop(&mut self) {
        if self.inner.is_none() {
            panic!("rwmutex read guard is dropped without unlock")
        }
    }
}

pub struct TokioWriteGuard<'a, T> {
    inner: Option<parking_lot::RwLockWriteGuard<'a, T>>,
}

impl<'a, T> Deref for TokioWriteGuard<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap().deref()
    }
}

impl<'a, T> DerefMut for TokioWriteGuard<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap().deref_mut()
    }
}

pub struct TokioJoinHandle(tokio::task::JoinHandle<()>);

impl JoinHandle for TokioJoinHandle {
    #[inline]
    async fn join(self) {
        self.0.await.expect("thread should not panic")
    }
}

pub struct TokioFile(tokio::fs::File);

impl File for TokioFile {
    #[inline]
    async fn open(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let f = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())
            .await?;
        lock(&f)?;
        Ok(Self(f))
    }

    #[inline]
    async fn is_file(&self) -> std::io::Result<bool> {
        Ok(self.0.metadata().await?.is_file())
    }

    #[inline]
    async fn len(&self) -> std::io::Result<u64> {
        Ok(self.0.metadata().await?.len())
    }

    #[inline]
    async fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<()> {
        use tokio::io::AsyncSeekExt;
        self.0.seek(position).await?;
        Ok(())
    }

    #[inline]
    async fn read(&mut self, buff: &mut [u8]) -> std::io::Result<usize> {
        use tokio::io::AsyncReadExt;
        self.0.read(buff).await
    }

    #[inline]
    async fn read_exact(&mut self, buff: &mut [u8]) -> std::io::Result<()> {
        use tokio::io::AsyncReadExt;
        self.0.read_exact(buff).await?;
        Ok(())
    }

    #[inline]
    async fn write_all(&mut self, buff: &[u8]) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        self.0.write_all(buff).await?;
        Ok(())
    }

    #[inline]
    async fn sync(&mut self) -> std::io::Result<()> {
        self.0.sync_all().await
    }

    #[inline]
    async fn truncate(&mut self, size: u64) -> std::io::Result<()> {
        self.0.set_len(size).await
    }
}

#[cfg(unix)]
fn lock(f: &tokio::fs::File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    use syscalls::{syscall2, Sysno};
    let fd = f.as_raw_fd();
    const LOCK_EX: usize = 0x2;
    let result = unsafe { syscall2(Sysno::flock, fd as usize, LOCK_EX) };
    if let Err(err) = result {
        Err(std::io::Error::from_raw_os_error(err.into_raw()))
    } else {
        Ok(())
    }
}

macro_rules! impl_atomic {
    ($name:ident, $ty:ident) => {
        pub struct $name(atomic::$name);

        impl crate::runtime::Atomic<$ty> for $name {
            #[inline]
            fn new(value: $ty) -> Self {
                Self(atomic::$name::new(value))
            }

            #[inline]
            async fn load(&self) -> $ty {
                self.0.load(atomic::Ordering::SeqCst)
            }

            #[inline]
            async fn compare_and_exchange(&self, old: $ty, new: $ty) -> bool {
                self.0
                    .compare_exchange(old, new, atomic::Ordering::SeqCst, atomic::Ordering::SeqCst)
                    .is_ok()
            }

            #[inline]
            async fn fetch_add(&self, delta: $ty) -> $ty {
                self.0.fetch_add(delta, atomic::Ordering::SeqCst)
            }
        }
    };
}

use std::sync::atomic;
impl_atomic!(AtomicUsize, usize);
impl_atomic!(AtomicU8, u8);
impl_atomic!(AtomicU16, u16);
impl_atomic!(AtomicU32, u32);
impl_atomic!(AtomicU64, u64);
impl_atomic!(AtomicIsize, isize);
impl_atomic!(AtomicI8, i8);
impl_atomic!(AtomicI16, i16);
impl_atomic!(AtomicI32, i32);
impl_atomic!(AtomicI64, i64);
