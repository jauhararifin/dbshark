use super::runtime;
use super::runtime::Runtime;
use indexmap::IndexSet;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

thread_local! {
    static RUNTIME: RefCell<Option<SimulatedRuntime>> = RefCell::default();
    static THREAD_ID: Cell<usize> = Cell::new(1);
    static THREAD_WAITER: RefCell<Option<std::sync::mpsc::Receiver<()>>> = RefCell::default();
}

pub(crate) struct SimulatedRuntime {
    internal: Arc<parking_lot::Mutex<Internal>>,
}

struct Internal {
    thread_id: usize,
    ready: Vec<usize>,
    active_threads: HashSet<usize>,
    panicked_threads: Vec<usize>,
    waker: HashMap<usize, std::sync::mpsc::SyncSender<()>>,
    // joining maps from joined thread to the list joining threads.
    // k -> v0, v1, v2... means v0, v1, and v2 waiting for
    // k to finish
    joining: HashMap<usize, IndexSet<usize>>,
    rng: StdRng,
}

impl Runtime for SimulatedRuntime {
    type Timer = SimulatedTimer;
    type TimerHandle = SimulatedTimerHandle;

    type Mutex<T: Send + Sync> = SimulatedMutex<T>;
    type RwMutex<T: Send + Sync> = SimulatedRwMutex<T>;

    type JoinHandle = SimulatedJoinHandle;

    type File = SimulatedFile;

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
        Self::park();

        let thread_id = RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            r.thread_id += 1;
            r.thread_id
        });
        log::trace!(thread_id; "spawning_thread");

        let (trigger, waiter) = std::sync::mpsc::sync_channel::<()>(1);
        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            r.waker.insert(thread_id, trigger);
            r.active_threads.insert(thread_id);
            r.ready.push(thread_id);
        });

        let cloned_runtime =
            RUNTIME.with_borrow(|r| r.as_ref().expect("runtime should be valid").clone());

        std::thread::spawn(move || {
            waiter.recv().expect("waiting a thread should never fail");
            RUNTIME.set(Some(cloned_runtime));
            THREAD_ID.set(thread_id);
            THREAD_WAITER.set(Some(waiter));

            log::trace!(thread_id; "thread_started");

            Self::park();
            let cleanup = SpawnCleanup { thread_id };
            f();
            drop(cleanup);
        });

        Self::park();

        SimulatedJoinHandle(thread_id)
    }

    fn park() {
        let thread_id = THREAD_ID.get();
        log::trace!(thread_id; "thread_parked");

        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            r.ready.push(thread_id);
        });

        resume_any();

        THREAD_WAITER.with_borrow(|w| {
            w.as_ref()
                .expect("waiter should exists")
                .recv()
                .expect("waiting a thread should never fail")
        });
        log::trace!(thread_id=THREAD_ID.get(); "thread_resumed");
    }

    fn timer(duration: std::time::Duration) -> (Self::Timer, Self::TimerHandle) {
        todo!();
    }

    fn create_dir_all<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<()> {
        todo!();
    }
}

struct SpawnCleanup {
    thread_id: usize,
}

impl Drop for SpawnCleanup {
    fn drop(&mut self) {
        let is_panic = std::thread::panicking();
        if is_panic {
            RUNTIME.with_borrow(|r| {
                let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
                r.panicked_threads.push(self.thread_id);
            });
        }

        SimulatedRuntime::park();

        log::trace!(thread_id=self.thread_id; "thread_finished");
        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            r.active_threads.remove(&self.thread_id);
            if let Some(s) = r.joining.remove(&self.thread_id) {
                for t in s {
                    r.ready.push(t);
                }
            }
        });

        resume_any().expect("all thread are sleeping, a sign of deadlock");
    }
}

impl SimulatedRuntime {
    pub(crate) fn run(seed: u64, f: impl FnOnce() + Send + 'static) {
        let r = Self {
            internal: Arc::new(parking_lot::Mutex::<Internal>::new(Internal {
                thread_id: 1,
                ready: vec![],
                active_threads: HashSet::default(),
                panicked_threads: Vec::default(),
                waker: HashMap::default(),
                joining: HashMap::default(),
                rng: rand::rngs::StdRng::seed_from_u64(seed),
            })),
        };

        let thread_id = 1;
        log::trace!(thread_id; "spawn_root");

        let (trigger, waiter) = std::sync::mpsc::sync_channel::<()>(1);
        THREAD_WAITER.set(Some(waiter));
        {
            let mut r = r.internal.lock();
            r.waker.insert(thread_id, trigger);
            r.active_threads.insert(thread_id);
        }

        RUNTIME.set(Some(r));

        let cleanup = RunCleanup { thread_id };
        f();
        drop(cleanup);
    }
}

struct RunCleanup {
    thread_id: usize,
}

impl Drop for RunCleanup {
    fn drop(&mut self) {
        let is_panic = std::thread::panicking();
        if is_panic {
            RUNTIME.with_borrow(|r| {
                let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
                r.panicked_threads.push(self.thread_id);
            });
        }

        let runtime = RUNTIME.take().unwrap();
        let mut r = runtime.internal.lock();
        r.active_threads.remove(&self.thread_id);
        if let Some(s) = r.joining.remove(&self.thread_id) {
            for t in s {
                r.ready.push(t);
            }
        }
        drop(r);

        loop {
            let r = runtime.internal.lock();
            if r.active_threads.is_empty() {
                break;
            }
            drop(r);
            resume_any().expect("all thread are sleeping, a sign of deadlock");
        }

        let r = runtime.internal.lock();
        for panicked_thread_id in &r.panicked_threads {
            panic!("thread {panicked_thread_id} got panicked");
        }

        log::trace!("simulation_finished");
    }
}

fn resume_any() -> Option<usize> {
    let (id, waker) = RUNTIME.with_borrow(|r| {
        let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
        if r.ready.is_empty() {
            return None;
        }
        let resuming_thread_index = r.rng.next_u64() as usize % r.ready.len();

        let resuming_thread_id = r.ready.remove(resuming_thread_index);
        log::trace!(thread_id=resuming_thread_id; "thread_resuming");
        let waker = r
            .waker
            .get(&resuming_thread_id)
            .expect("waker should exists");
        Some((resuming_thread_id, waker.clone()))
    })?;

    waker
        .try_send(())
        .expect("resuming thread should always successfull");

    Some(id)
}

impl Clone for SimulatedRuntime {
    fn clone(&self) -> Self {
        Self {
            internal: self.internal.clone(),
        }
    }
}

impl Drop for SimulatedRuntime {
    fn drop(&mut self) {
        // TODO: we should check that everything is ok. no pending thread etc
    }
}

pub(crate) struct RuntimeGuard;

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        RUNTIME.set(None);
    }
}

pub(crate) struct SimulatedTimer {}

impl runtime::Timer for SimulatedTimer {
    fn wait(&mut self) -> bool {
        todo!();
    }
}

pub(crate) struct SimulatedTimerHandle {}

impl Clone for SimulatedTimerHandle {
    fn clone(&self) -> Self {
        todo!();
    }
}

impl runtime::TimerHandle for SimulatedTimerHandle {
    fn trigger(&self) {
        todo!();
    }

    fn close(&self) {
        todo!();
    }
}

pub(crate) struct SimulatedMutex<T: Send + Sync>(std::marker::PhantomData<T>);

impl<T: Send + Sync> runtime::Mutex<T> for SimulatedMutex<T> {
    type Guard<'a> = SimulatedMutexGuard<'a, T>
    where
        Self: 'a;

    fn new(data: T) -> Self {
        todo!();
    }

    fn lock(&self) -> Self::Guard<'_> {
        todo!();
    }

    fn try_lock(&self) -> Option<Self::Guard<'_>> {
        todo!();
    }
}

pub(crate) struct SimulatedMutexGuard<'a, T>(std::marker::PhantomData<&'a T>);

impl<'a, T> Deref for SimulatedMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        todo!();
    }
}

impl<'a, T> DerefMut for SimulatedMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        todo!();
    }
}

pub(crate) struct SimulatedRwMutex<T: Send + Sync>(std::marker::PhantomData<T>);

impl<T: Send + Sync> runtime::RwMutex<T> for SimulatedRwMutex<T> {
    type ReadGuard<'a> = SimulatedRwMutexReadGuard<'a,T>
    where
        T: 'a;
    type WriteGuard<'a> = SimulatedRwMutexWriteGuard<'a,T>
    where
        T: 'a;

    fn new(data: T) -> Self {
        todo!();
    }

    fn read(&self) -> Self::ReadGuard<'_> {
        todo!();
    }

    fn write(&self) -> Self::WriteGuard<'_> {
        todo!();
    }

    fn try_write(&self) -> Option<Self::WriteGuard<'_>> {
        todo!();
    }
}

pub(crate) struct SimulatedRwMutexReadGuard<'a, T>(std::marker::PhantomData<&'a T>);

impl<'a, T> Deref for SimulatedRwMutexReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        todo!();
    }
}

impl<'a, T> From<SimulatedRwMutexWriteGuard<'a, T>> for SimulatedRwMutexReadGuard<'a, T> {
    fn from(value: SimulatedRwMutexWriteGuard<'a, T>) -> Self {
        todo!()
    }
}

pub(crate) struct SimulatedRwMutexWriteGuard<'a, T>(std::marker::PhantomData<&'a T>);

impl<'a, T> Deref for SimulatedRwMutexWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        todo!();
    }
}

impl<'a, T> DerefMut for SimulatedRwMutexWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        todo!();
    }
}

pub(crate) struct SimulatedJoinHandle(usize);

impl runtime::JoinHandle for SimulatedJoinHandle {
    fn join(self) {
        log::trace!(source=THREAD_ID.get(), target=self.0;"join");
        SimulatedRuntime::park();

        let thread_id = self.0;
        let already_exit = RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            if !r.active_threads.contains(&self.0) {
                return true;
            }

            let this_thread = THREAD_ID.get();
            r.joining.entry(thread_id).or_default().insert(this_thread);
            false
        });

        if already_exit {
            log::trace!(source=THREAD_ID.get(), target=self.0; "join_target_already_exit");
            return;
        }

        resume_any().expect("all thread are sleeping, a sign of deadlock");

        THREAD_WAITER.with_borrow(|w| {
            w.as_ref()
                .expect("waiter should exists")
                .recv()
                .expect("waiting a thread should never fail")
        });
    }
}

pub(crate) struct SimulatedFile;

impl runtime::File for SimulatedFile {
    #[inline]
    fn open(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        todo!();
    }

    #[inline]
    fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        todo!();
    }

    #[inline]
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<()> {
        todo!();
    }

    #[inline]
    fn read(&mut self, buff: &mut [u8]) -> std::io::Result<usize> {
        todo!();
    }

    #[inline]
    fn read_exact(&mut self, buff: &mut [u8]) -> std::io::Result<()> {
        todo!();
    }

    #[inline]
    fn write_all(&mut self, buff: &[u8]) -> std::io::Result<()> {
        todo!();
    }

    #[inline]
    fn sync(&mut self) -> std::io::Result<()> {
        todo!();
    }

    #[inline]
    fn truncate(&mut self, size: u64) -> std::io::Result<()> {
        todo!();
    }
}

macro_rules! impl_atomic {
    ($name:ident, $ty:ident) => {
        pub(crate) struct $name(parking_lot::Mutex<$ty>);
        impl runtime::Atomic<$ty> for $name {
            #[inline]
            fn new(value: $ty) -> Self {
                Self(parking_lot::Mutex::new(value))
            }

            #[inline]
            fn load(&self) -> $ty {
                SimulatedRuntime::park();
                let val = *self.0.lock();
                SimulatedRuntime::park();
                val
            }

            #[inline]
            fn store(&self, value: $ty) {
                SimulatedRuntime::park();
                *self.0.lock() = value;
                SimulatedRuntime::park();
            }

            #[inline]
            fn compare_and_exchange(&self, old: $ty, new: $ty) -> bool {
                SimulatedRuntime::park();
                let mut val = self.0.lock();
                let result = if *val == old {
                    *val = new;
                    true
                } else {
                    false
                };
                drop(val);
                SimulatedRuntime::park();
                result
            }

            fn fetch_add(&self, delta: $ty) -> $ty {
                SimulatedRuntime::park();
                let mut val = self.0.lock();
                let old = *val;
                *val += delta;
                drop(val);
                SimulatedRuntime::park();
                old
            }
        }
    };
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experiment::runtime::*;

    use std::sync::Once;
    static INIT: Once = Once::new();
    fn setup() {
        INIT.call_once(|| {
            env_logger::init();
        });
    }

    #[test]
    fn test_1() {
        setup();
        SimulatedRuntime::run(0, || {
            let handle = SimulatedRuntime::spawn(|| {});
            handle.join();
        });
    }

    #[test]
    fn test_2() {
        setup();
        SimulatedRuntime::run(0, || {
            let a = Arc::new(AtomicI32::new(0));
            let x = a.clone();
            let handle1 = SimulatedRuntime::spawn(move || {
                for _ in 0..10000 {
                    x.fetch_add(20);
                }
            });
            let x = a.clone();
            let handle2 = SimulatedRuntime::spawn(move || {
                for _ in 0..10000 {
                    x.fetch_add(-10);
                }
            });
            handle1.join();
            handle2.join();

            assert_eq!(100000, a.load());
        });
    }
}
