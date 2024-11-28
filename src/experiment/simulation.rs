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
    static THREAD_ID: Cell<ThreadId> = Cell::default();
    static THREAD_WAITER: RefCell<Option<std::sync::mpsc::Receiver<()>>> = RefCell::default();
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct ThreadId(usize);

impl ThreadId {
    fn next(&mut self) -> ThreadId {
        self.0 += 1;
        *self
    }
}

impl log::kv::ToValue for ThreadId {
    fn to_value(&self) -> log::kv::Value {
        log::kv::Value::from_display(self)
    }
}

impl std::fmt::Display for ThreadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "thread#{}", self.0)
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct MutexId(usize);

impl MutexId {
    fn next(&mut self) -> MutexId {
        self.0 += 1;
        *self
    }
}

impl log::kv::ToValue for MutexId {
    fn to_value(&self) -> log::kv::Value {
        log::kv::Value::from_display(self)
    }
}

impl std::fmt::Display for MutexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mutex#{}", self.0)
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct RwMutexId(usize);

impl RwMutexId {
    fn next(&mut self) -> RwMutexId {
        self.0 += 1;
        *self
    }
}

impl log::kv::ToValue for RwMutexId {
    fn to_value(&self) -> log::kv::Value {
        log::kv::Value::from_display(self)
    }
}

impl std::fmt::Display for RwMutexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rwmutex#{}", self.0)
    }
}

pub(crate) struct SimulatedRuntime {
    internal: Arc<parking_lot::Mutex<Internal>>,
}

struct Internal {
    thread_id: ThreadId,
    ready: Vec<ThreadId>,
    active_threads: HashSet<ThreadId>,
    panicked_threads: Vec<ThreadId>,
    waker: HashMap<ThreadId, std::sync::mpsc::SyncSender<()>>,

    // joining maps from joined thread to the list joining threads.
    // k -> v0, v1, v2... means v0, v1, and v2 waiting for
    // k to finish
    joining: HashMap<ThreadId, IndexSet<ThreadId>>,
    mutex_id: MutexId,
    mutex_wait: HashMap<MutexId, IndexSet<ThreadId>>,
    rwmutex_id: RwMutexId,
    rwmutex_read_wait: HashMap<RwMutexId, IndexSet<ThreadId>>,
    rwmutex_write_wait: HashMap<RwMutexId, IndexSet<ThreadId>>,

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
            r.thread_id.next()
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

        switch();
        log::trace!(thread_id; "thread_resumed");
    }

    fn timer(_duration: std::time::Duration) -> (Self::Timer, Self::TimerHandle) {
        todo!();
    }

    fn create_dir_all<P: AsRef<std::path::Path>>(_path: P) -> std::io::Result<()> {
        Ok(())
    }
}

struct SpawnCleanup {
    thread_id: ThreadId,
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

        park();

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
    #[allow(unused)]
    pub(crate) fn run(seed: u64, f: impl FnOnce() + Send + 'static) {
        let r = Self {
            internal: Arc::new(parking_lot::Mutex::<Internal>::new(Internal {
                thread_id: ThreadId(1),
                ready: vec![],
                active_threads: HashSet::default(),
                panicked_threads: Vec::default(),
                waker: HashMap::default(),
                joining: HashMap::default(),
                mutex_id: MutexId::default(),
                mutex_wait: HashMap::default(),
                rwmutex_id: RwMutexId::default(),
                rwmutex_read_wait: HashMap::default(),
                rwmutex_write_wait: HashMap::default(),
                rng: rand::rngs::StdRng::seed_from_u64(seed),
            })),
        };

        let thread_id = ThreadId(1);
        THREAD_ID.set(thread_id);
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
    thread_id: ThreadId,
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

fn resume_any() -> Option<ThreadId> {
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
}

pub(crate) struct SimulatedMutex<T: Send + Sync> {
    id: MutexId,
    locker: parking_lot::Mutex<Option<ThreadId>>,
    value: parking_lot::Mutex<T>,
}

impl<T: Send + Sync> runtime::Mutex<T> for SimulatedMutex<T> {
    type Guard<'a> = SimulatedMutexGuard<'a, T>
    where
        Self: 'a;

    fn new(data: T) -> Self {
        let mutex_id = RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            r.mutex_id.next()
        });

        Self {
            id: mutex_id,
            locker: parking_lot::Mutex::new(None),
            value: parking_lot::Mutex::new(data),
        }
    }

    fn lock(&self) -> Self::Guard<'_> {
        park();

        loop {
            let current = THREAD_ID.get();
            let mut locker = self.locker.lock();
            if let Some(blocker) = *locker {
                drop(locker);
                log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id,blocker; "mutex_acquiring_blocked");
                enqueue_mutex(self.id);
                switch();
            } else {
                *locker = Some(current);
                break;
            }
        }

        park();

        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id; "mutex_acquired");
        SimulatedMutexGuard {
            id: self.id,
            locker: &self.locker,
            guard: self.value.lock(),
        }
    }

    fn try_lock(&self) -> Option<Self::Guard<'_>> {
        todo!();
    }
}

pub(crate) struct SimulatedMutexGuard<'a, T: Send + Sync> {
    id: MutexId,
    locker: &'a parking_lot::Mutex<Option<ThreadId>>,
    guard: parking_lot::MutexGuard<'a, T>,
}

impl<'a, T: Send + Sync> Deref for SimulatedMutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<'a, T: Send + Sync> DerefMut for SimulatedMutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

impl<'a, T: Send + Sync> Drop for SimulatedMutexGuard<'a, T> {
    fn drop(&mut self) {
        park();

        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id; "mutex_released");
        {
            *self.locker.lock() = None;
        }
        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            if let Some(s) = r.mutex_wait.remove(&self.id) {
                for t in s {
                    r.ready.push(t);
                }
            }
        });
    }
}

pub(crate) struct SimulatedRwMutex<T: Send + Sync> {
    id: RwMutexId,
    locker: parking_lot::Mutex<RwMutexState>,
    value: parking_lot::RwLock<T>,
}

enum RwMutexState {
    // It's in read state. reading this rwmutex shouldn't be blocked
    Read(usize),
    // It's in read state, but a writer is trying to write it.
    // To avoid starvation, any further read is blocked until this
    // writer get a chance to write
    ReadBlocked(usize),
    // It's in write state, no other thread should be able to lock
    Write(ThreadId),
    // It's unlocked
    Unlocked,
}

impl<T: Send + Sync> runtime::RwMutex<T> for SimulatedRwMutex<T> {
    type ReadGuard<'a> = SimulatedRwMutexReadGuard<'a,T>
    where
        T: 'a;
    type WriteGuard<'a> = SimulatedRwMutexWriteGuard<'a,T>
    where
        T: 'a;

    fn new(data: T) -> Self {
        let rwmutex_id = RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            r.rwmutex_id.next()
        });

        Self {
            id: rwmutex_id,
            locker: parking_lot::Mutex::new(RwMutexState::Unlocked),
            value: parking_lot::RwLock::new(data),
        }
    }

    fn read(&self) -> Self::ReadGuard<'_> {
        park();

        loop {
            let current = THREAD_ID.get();
            let mut locker = self.locker.lock();
            match *locker {
                RwMutexState::Read(n) => {
                    *locker = RwMutexState::Read(n + 1);
                    break;
                }
                RwMutexState::ReadBlocked(_) => {
                    drop(locker);
                    log::trace!(thread_id=current,rwmutex_id=self.id; "rwmutex_read_acquiring_blocked_1");
                    enqueue_rwmutex_for_read(self.id);
                    switch();
                }
                RwMutexState::Write(writer) => {
                    drop(locker);
                    log::trace!(thread_id=current,rwmutex_id=self.id,writer; "rwmutex_read_acquiring_blocked_2");
                    enqueue_rwmutex_for_read(self.id);
                    switch();
                }
                RwMutexState::Unlocked => {
                    *locker = RwMutexState::Read(1);
                    break;
                }
            }
        }

        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id; "rwmutex_read_acquired");

        park();
        SimulatedRwMutexReadGuard {
            id: self.id,
            locker: &self.locker,
            guard: self.value.read(),
        }
    }

    fn write(&self) -> Self::WriteGuard<'_> {
        park();

        loop {
            let current = THREAD_ID.get();
            let mut locker = self.locker.lock();
            match *locker {
                RwMutexState::Read(n) => {
                    log::trace!(thread_id=current,rwmutex_id=self.id; "rwmutex_write_acquiring_blocked_1");
                    *locker = RwMutexState::ReadBlocked(n);
                    drop(locker);
                    enqueue_rwmutex_for_write(self.id);
                    switch();
                }
                RwMutexState::ReadBlocked(_) | RwMutexState::Write(_) => {
                    log::trace!(thread_id=current,rwmutex_id=self.id; "rwmutex_write_acquiring_blocked_2");
                    drop(locker);
                    enqueue_rwmutex_for_write(self.id);
                    switch();
                }
                RwMutexState::Unlocked => {
                    *locker = RwMutexState::Write(current);
                    break;
                }
            }
        }

        park();

        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id; "rwmutex_write_acquired");
        SimulatedRwMutexWriteGuard {
            id: self.id,
            locker: &self.locker,
            guard: self.value.write(),
        }
    }

    fn try_write(&self) -> Option<Self::WriteGuard<'_>> {
        park();

        let current = THREAD_ID.get();
        let mut locker = self.locker.lock();
        match *locker {
            RwMutexState::Read(_) | RwMutexState::ReadBlocked(_) | RwMutexState::Write(_) => {
                return None;
            }
            RwMutexState::Unlocked => {
                *locker = RwMutexState::Write(current);
            }
        }

        park();

        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id; "rwmutex_try_write_acquired");
        Some(SimulatedRwMutexWriteGuard {
            id: self.id,
            locker: &self.locker,
            guard: self.value.write(),
        })
    }
}

pub(crate) struct SimulatedRwMutexReadGuard<'a, T> {
    id: RwMutexId,
    locker: &'a parking_lot::Mutex<RwMutexState>,
    guard: parking_lot::RwLockReadGuard<'a, T>,
}

impl<'a, T> Drop for SimulatedRwMutexReadGuard<'a, T> {
    fn drop(&mut self) {
        park();

        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id; "rwmutex_read_released");
        let mut state = self.locker.lock();
        let is_last_reader = {
            match *state {
                RwMutexState::Read(n) => {
                    *state = RwMutexState::Read(n - 1);
                    n == 1
                }
                RwMutexState::ReadBlocked(n) => {
                    *state = RwMutexState::ReadBlocked(n - 1);
                    n == 1
                }
                _ => unreachable!("cannot release non acquired rw mutex"),
            }
        };

        if is_last_reader {
            *state = RwMutexState::Unlocked;
            release_all_rwmutex_waiter(self.id);
        }
    }
}

impl<'a, T> Deref for SimulatedRwMutexReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<'a, T> From<SimulatedRwMutexWriteGuard<'a, T>> for SimulatedRwMutexReadGuard<'a, T> {
    fn from(_value: SimulatedRwMutexWriteGuard<'a, T>) -> Self {
        todo!()
    }
}

pub(crate) struct SimulatedRwMutexWriteGuard<'a, T> {
    id: RwMutexId,
    locker: &'a parking_lot::Mutex<RwMutexState>,
    guard: parking_lot::RwLockWriteGuard<'a, T>,
}

impl<'a, T> Drop for SimulatedRwMutexWriteGuard<'a, T> {
    fn drop(&mut self) {
        park();

        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id; "rwmutex_write_released");
        *self.locker.lock() = RwMutexState::Unlocked;
        release_all_rwmutex_waiter(self.id);
    }
}

fn release_all_rwmutex_waiter(rwmutex_id: RwMutexId) {
    RUNTIME.with_borrow(|r| {
        let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
        if let Some(s) = r.rwmutex_write_wait.remove(&rwmutex_id) {
            for t in s {
                r.ready.push(t);
            }
        }
        if let Some(s) = r.rwmutex_read_wait.remove(&rwmutex_id) {
            for t in s {
                r.ready.push(t);
            }
        }
    });
}

impl<'a, T> Deref for SimulatedRwMutexWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<'a, T> DerefMut for SimulatedRwMutexWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

pub(crate) struct SimulatedJoinHandle(ThreadId);

impl runtime::JoinHandle for SimulatedJoinHandle {
    fn join(self) {
        park();

        log::trace!(source=THREAD_ID.get(), target=self.0;"join");

        let thread_id = self.0;
        let already_exit = RUNTIME.with_borrow(|r| {
            let r = r.as_ref().expect("runtime should be valid").internal.lock();
            !r.active_threads.contains(&self.0)
        });

        if already_exit {
            log::trace!(source=THREAD_ID.get(), target=self.0; "join_target_already_exit");
        } else {
            enqueue_join(thread_id);
            switch();
        }
    }
}

fn switch() {
    resume_any().expect("all thread are sleeping, a sign of deadlock");
    sleep();
}

fn sleep() {
    THREAD_WAITER.with_borrow(|w| {
        w.as_ref()
            .expect("waiter should exists")
            .recv()
            .expect("waiting a thread should never fail")
    });
}

fn park() {
    SimulatedRuntime::park();
}

fn enqueue_join(thread_id: ThreadId) {
    RUNTIME.with_borrow(|r| {
        let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
        let this_thread = THREAD_ID.get();
        r.joining.entry(thread_id).or_default().insert(this_thread);
    });
}

fn enqueue_mutex(mutex_id: MutexId) {
    RUNTIME.with_borrow(|r| {
        let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
        let this_thread = THREAD_ID.get();
        r.mutex_wait
            .entry(mutex_id)
            .or_default()
            .insert(this_thread);
    });
}

fn enqueue_rwmutex_for_read(rwmutex_id: RwMutexId) {
    RUNTIME.with_borrow(|r| {
        let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
        let this_thread = THREAD_ID.get();
        r.rwmutex_read_wait
            .entry(rwmutex_id)
            .or_default()
            .insert(this_thread);
    });
}

fn enqueue_rwmutex_for_write(rwmutex_id: RwMutexId) {
    RUNTIME.with_borrow(|r| {
        let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
        let this_thread = THREAD_ID.get();
        r.rwmutex_write_wait
            .entry(rwmutex_id)
            .or_default()
            .insert(this_thread);
    });
}

pub(crate) struct SimulatedFile;

impl runtime::File for SimulatedFile {
    #[inline]
    fn open(_path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        todo!();
    }

    #[inline]
    fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        todo!();
    }

    #[inline]
    fn seek(&mut self, _position: std::io::SeekFrom) -> std::io::Result<()> {
        todo!();
    }

    #[inline]
    fn read(&mut self, _buff: &mut [u8]) -> std::io::Result<usize> {
        todo!();
    }

    #[inline]
    fn read_exact(&mut self, _buff: &mut [u8]) -> std::io::Result<()> {
        todo!();
    }

    #[inline]
    fn write_all(&mut self, _buff: &[u8]) -> std::io::Result<()> {
        todo!();
    }

    #[inline]
    fn sync(&mut self) -> std::io::Result<()> {
        todo!();
    }

    #[inline]
    fn truncate(&mut self, _size: u64) -> std::io::Result<()> {
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
                park();
                let val = *self.0.lock();
                park();
                val
            }

            #[inline]
            fn compare_and_exchange(&self, old: $ty, new: $ty) -> bool {
                park();
                let mut val = self.0.lock();
                let result = if *val == old {
                    *val = new;
                    true
                } else {
                    false
                };
                drop(val);
                park();
                result
            }

            fn fetch_add(&self, delta: $ty) -> $ty {
                park();
                let mut val = self.0.lock();
                let old = *val;
                *val += delta;
                drop(val);
                park();
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
    fn test_simple_join() {
        setup();
        SimulatedRuntime::run(0, || {
            let handle = SimulatedRuntime::spawn(|| {});
            handle.join();
        });
    }

    #[test]
    fn test_simple_atomic() {
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

    #[test]
    fn test_simple_mutex() {
        setup();
        SimulatedRuntime::run(0, || {
            let a = Arc::new(SimulatedMutex::new(0));
            let x = a.clone();
            let handle1 = SimulatedRuntime::spawn(move || {
                for _ in 0..10000 {
                    park();
                    let mut y = x.lock();
                    park();
                    let new_y = *y + 20;
                    park();
                    *y = new_y;
                    park();
                }
            });

            let x = a.clone();
            let handle2 = SimulatedRuntime::spawn(move || {
                for _ in 0..10000 {
                    park();
                    let mut y = x.lock();
                    park();
                    let new_y = *y - 10;
                    park();
                    *y = new_y;
                    park();
                }
            });

            let x = a.clone();
            let handle3 = SimulatedRuntime::spawn(move || {
                for _ in 0..10000 {
                    park();
                    let mut y = x.lock();
                    park();
                    let new_y = *y + 1;
                    park();
                    *y = new_y;
                    park();
                }
            });

            handle1.join();
            handle2.join();
            handle3.join();

            assert_eq!(110_000, *a.lock());
        });
    }

    #[test]
    fn test_simple_rwmutex() {
        setup();
        SimulatedRuntime::run(0, || {
            let a = Arc::new(SimulatedRwMutex::new((0, 0)));

            let mut joins = vec![];
            for _ in 0..100 {
                let x = a.clone();
                let handle = SimulatedRuntime::spawn(move || {
                    for _ in 0..100 {
                        park();
                        let g = x.read();
                        park();
                        let (p, q) = *g;
                        park();
                        assert!(p == q);
                        park();
                    }
                });
                joins.push(handle);
            }

            use rand::SeedableRng;

            for seed in 0..15 {
                let x = a.clone();
                let mut rng = StdRng::seed_from_u64(seed as u64);
                let handle = SimulatedRuntime::spawn(move || {
                    for _ in 0..100 {
                        park();
                        let mut g = x.write();
                        park();
                        let y = rng.next_u32() as i32;
                        park();
                        *g = (y, y);
                        park();
                    }
                });
                joins.push(handle);
            }

            for h in joins {
                h.join();
            }
        });
    }
}
