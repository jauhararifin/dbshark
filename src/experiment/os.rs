use super::runtime;
use std::io::{Read, Seek, Write};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub(crate) struct OsRuntime;

impl runtime::Runtime for OsRuntime {
    type Timer = OsTimer;
    type TimerHandle = OsTimerHandle;

    type Mutex<T: Send + Sync> = OsMutex<T>;
    type RwMutex<T: Send + Sync> = OsRwMutex<T>;

    type JoinHandle = OsJoinHandle;

    type File = OsFile;

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

    fn spawn(f: impl FnOnce() + Send + 'static) -> Self::JoinHandle {
        let handle = std::thread::spawn(f);
        OsJoinHandle(handle)
    }

    fn park() {
        // no-op
    }

    fn timer(duration: std::time::Duration) -> (Self::Timer, Self::TimerHandle) {
        let cond = Arc::new(parking_lot::Condvar::new());
        let m = Arc::new(parking_lot::Mutex::new(TimerState {
            trigger: 0,
            closed: false,
            last_run: std::time::Instant::now(),
        }));

        (
            OsTimer {
                duration,
                cond: cond.clone(),
                m: m.clone(),
            },
            OsTimerHandle(Arc::new(OsTimerHandleInternal { cond, m })),
        )
    }

    fn create_dir_all<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<()> {
        std::fs::create_dir_all(path.as_ref())
    }
}

pub(crate) struct OsTimer {
    duration: std::time::Duration,
    cond: Arc<parking_lot::Condvar>,
    m: Arc<parking_lot::Mutex<TimerState>>,
}

struct TimerState {
    trigger: usize,
    closed: bool,
    last_run: std::time::Instant,
}

impl runtime::Timer for OsTimer {
    fn wait(&mut self) -> bool {
        let mut state = self.m.lock();
        if state.closed {
            return false;
        }
        if state.trigger > 0 {
            state.trigger -= 1;
            state.last_run = std::time::Instant::now();
            return true;
        }

        let elapsed = state.last_run.elapsed();
        if elapsed > self.duration {
            state.last_run = std::time::Instant::now();
            return true;
        }

        let result = self.cond.wait_for(&mut state, self.duration - elapsed);

        if state.closed {
            return false;
        }
        if state.trigger > 0 {
            state.trigger -= 1;
            state.last_run = std::time::Instant::now();
            return true;
        }

        assert!(result.timed_out());
        state.last_run = std::time::Instant::now();
        true
    }
}

pub(crate) struct OsTimerHandle(Arc<OsTimerHandleInternal>);

struct OsTimerHandleInternal {
    cond: Arc<parking_lot::Condvar>,
    m: Arc<parking_lot::Mutex<TimerState>>,
}

impl Clone for OsTimerHandle {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl runtime::TimerHandle for OsTimerHandle {
    fn trigger(&self) {
        self.0.m.lock().trigger += 1;
        self.0.cond.notify_one();
    }
}

impl Drop for OsTimerHandleInternal {
    fn drop(&mut self) {
        self.m.lock().closed = true;
        self.cond.notify_one();
    }
}

pub(crate) struct OsMutex<T: Send + Sync>(parking_lot::Mutex<T>);

impl<T: Send + Sync> runtime::Mutex<T> for OsMutex<T> {
    type Guard<'a> = OsMutexGuard<'a, T>
    where
        Self: 'a;

    fn new(data: T) -> Self {
        Self(parking_lot::Mutex::new(data))
    }

    fn lock(&self) -> Self::Guard<'_> {
        OsMutexGuard(self.0.lock())
    }

    fn try_lock(&self) -> Option<Self::Guard<'_>> {
        self.0.try_lock().map(OsMutexGuard)
    }
}

pub(crate) struct OsMutexGuard<'a, T>(parking_lot::MutexGuard<'a, T>);

impl<'a, T> Deref for OsMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a, T> DerefMut for OsMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

pub(crate) struct OsRwMutex<T: Send + Sync>(parking_lot::RwLock<T>);

impl<T: Send + Sync> runtime::RwMutex<T> for OsRwMutex<T> {
    type ReadGuard<'a> = OsRwMutexReadGuard<'a,T>
    where
        T: 'a;
    type WriteGuard<'a> = OsRwMutexWriteGuard<'a,T>
    where
        T: 'a;

    fn new(data: T) -> Self {
        Self(parking_lot::RwLock::new(data))
    }

    fn read(&self) -> Self::ReadGuard<'_> {
        OsRwMutexReadGuard(self.0.read())
    }

    fn write(&self) -> Self::WriteGuard<'_> {
        OsRwMutexWriteGuard(self.0.write())
    }

    fn try_write(&self) -> Option<Self::WriteGuard<'_>> {
        self.0.try_write().map(OsRwMutexWriteGuard)
    }
}

pub(crate) struct OsRwMutexReadGuard<'a, T>(parking_lot::RwLockReadGuard<'a, T>);

impl<'a, T> Deref for OsRwMutexReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a, T> From<OsRwMutexWriteGuard<'a, T>> for OsRwMutexReadGuard<'a, T> {
    fn from(value: OsRwMutexWriteGuard<'a, T>) -> Self {
        Self(parking_lot::RwLockWriteGuard::downgrade(value.0))
    }
}

pub(crate) struct OsRwMutexWriteGuard<'a, T>(parking_lot::RwLockWriteGuard<'a, T>);

impl<'a, T> Deref for OsRwMutexWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a, T> DerefMut for OsRwMutexWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

pub(crate) struct OsJoinHandle(std::thread::JoinHandle<()>);

impl runtime::JoinHandle for OsJoinHandle {
    fn join(self) {
        self.0.join().expect("thread should not panic")
    }
}

pub(crate) struct OsFile(std::fs::File);

impl runtime::File for OsFile {
    #[inline]
    fn open(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())?;
        lock(&f)?;
        Ok(Self(f))
    }

    #[inline]
    fn is_file(&self) -> std::io::Result<bool> {
        Ok(self.0.metadata()?.is_file())
    }

    #[inline]
    fn len(&self) -> std::io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    #[inline]
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<()> {
        self.0.seek(position)?;
        Ok(())
    }

    #[inline]
    fn read(&mut self, buff: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buff)
    }

    #[inline]
    fn read_exact(&mut self, buff: &mut [u8]) -> std::io::Result<()> {
        self.0.read_exact(buff)
    }

    #[inline]
    fn write_all(&mut self, buff: &[u8]) -> std::io::Result<()> {
        self.0.write_all(buff)?;
        Ok(())
    }

    #[inline]
    fn sync(&mut self) -> std::io::Result<()> {
        self.0.sync_all()
    }

    #[inline]
    fn truncate(&mut self, size: u64) -> std::io::Result<()> {
        self.0.set_len(size)
    }
}

#[cfg(unix)]
fn lock(f: &std::fs::File) -> std::io::Result<()> {
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
        pub(crate) struct $name(atomic::$name);

        impl runtime::Atomic<$ty> for $name {
            #[inline]
            fn new(value: $ty) -> Self {
                Self(atomic::$name::new(value))
            }

            #[inline]
            fn load(&self) -> $ty {
                self.0.load(atomic::Ordering::SeqCst)
            }

            #[inline]
            fn compare_and_exchange(&self, old: $ty, new: $ty) -> bool {
                self.0
                    .compare_exchange(old, new, atomic::Ordering::SeqCst, atomic::Ordering::SeqCst)
                    .is_ok()
            }

            fn fetch_add(&self, delta: $ty) -> $ty {
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
