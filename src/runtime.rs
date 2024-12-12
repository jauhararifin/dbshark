//! Deterministic simulation testing.
//!
//! # The need for simulation
//!
//! Finding bugs and debugging complex concurrent programs is difficult. There are a bunch of cases
//! that hard to trigger in normal condition such as context switch and disk failures. On top of
//! that, being able to trigger the bug alone is not enough to help us find the root cause. Often,
//! we need to modify our code to help us debug the problem. The problem is, when you run the code
//! again, even though the bug might be triggered, the program state is different from the last
//! time you run, and you need to start debugging again from the beginning.
//!
//! To make it easier to debug, every possible source undeterminitism will be abstracted away with
//! a trait. Things such as locks, thread spawn, context switch, atomic operation, timer, and file
//! system should be accessed using a trait. In normal condition, we should just use the actual
//! primitives provided by OS or library like parking_lot. But, during testing, we can use some
//! kind of simulated runtime. The simulated runtime is used to simulate our program
//! deterministically. Ideally, when you run the same simulation with the same seed, you should get
//! the same result with exact same state. However, our implementation is not 100% deterministic,
//! and not 100% reflects what can happen in the practice. There are some limitations. Checks the
//! [`simulation`] module for more information.
//!
//! [`simulation`]: crate::simulation

use std::io;
use std::ops::{Deref, DerefMut};
use std::path::Path;

pub type RwMutexReadGuard<'a, R, T> = <<R as Runtime>::RwMutex<T> as RwMutex<T>>::ReadGuard<'a>;
pub type RwMutexWriteGuard<'a, R, T> = <<R as Runtime>::RwMutex<T> as RwMutex<T>>::WriteGuard<'a>;

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

    fn spawn(name: &'static str, f: impl FnOnce() + Send + 'static) -> Self::JoinHandle;

    fn park();

    fn timer(duration: std::time::Duration) -> (Self::Timer, Self::TimerHandle);

    fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()>;
}

pub trait Timer: Send {
    fn wait(&mut self) -> bool;
}

pub trait TimerHandle: Send + Sync + Clone {
    fn trigger(&self);
}

pub trait Mutex<T: Send + Sync>: Send + Sync {
    type Guard<'a>: DerefMut<Target = T>
    where
        Self: 'a;

    fn new(data: T) -> Self;
    fn lock(&self) -> Self::Guard<'_>;
    fn try_lock(&self) -> Option<Self::Guard<'_>>;
}

pub trait RwMutex<T: Send + Sync>: Send + Sync {
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

pub trait JoinHandle {
    fn join(self);
}

pub trait File: Sized + Send + Sync {
    fn open(path: impl AsRef<Path>) -> io::Result<Self>;

    fn is_file(&self) -> io::Result<bool>;

    fn len(&self) -> io::Result<u64>;

    fn seek(&mut self, position: io::SeekFrom) -> io::Result<()>;

    fn read(&mut self, buff: &mut [u8]) -> io::Result<usize>;

    fn read_exact(&mut self, buff: &mut [u8]) -> io::Result<()>;

    fn write_all(&mut self, buff: &[u8]) -> io::Result<()>;

    fn sync(&mut self) -> io::Result<()>;

    fn truncate(&mut self, size: u64) -> io::Result<()>;
}

pub trait Atomic<T>: Send + Sync {
    fn new(value: T) -> Self;
    fn load(&self) -> T;
    fn compare_and_exchange(&self, old: T, new: T) -> bool;
    fn fetch_add(&self, delta: T) -> T;
}
