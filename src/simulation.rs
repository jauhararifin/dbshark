use super::runtime;
use super::runtime::Runtime;
use indexmap::IndexSet;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, RngCore, SeedableRng};
use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;

// TODO: make a better crash simulation.
// Currently, we simulate crash scenario by panicking. When a crash is triggered, all threads
// throw panics, and the simulator will just exit and report the the program is crash. However,
// the behavior of crashing might not be the behavior you want to simulate. Normally, when a
// process is crash, it just stopped immediately, no cleanup. Think about what happen when there
// is a power outage, your whole computer just down, no cleanup. This simulation, doesn't do that.
// Instead, the Drop functions will still be called. As a result, some cleanup might be executed
// by the simulation.
// In order to do better simulation, you might want to make sure that your Drop function doesn't
// do any cleanup magic that can affect the next run. For example, don't try to flush your file
// or write something to a file. As long as your cleanup function only happen on the memory
// without side effect that can be perceived by the OS, everything should be fine.

thread_local! {
    static RUNTIME: RefCell<Option<SimulatedRuntime>> = RefCell::default();
    static THREAD_ID: Cell<ThreadId> = Cell::default();
    static THREAD_WAITER: RefCell<Option<std::sync::mpsc::Receiver<bool>>> = RefCell::default();
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
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

pub struct SimulatedRuntime {
    internal: Arc<parking_lot::Mutex<Internal>>,
}

struct Internal {
    thread_id: ThreadId,
    is_crashing: bool,
    is_deadlock: bool,
    main_trigger: Option<std::sync::mpsc::SyncSender<()>>,

    ticks: usize,

    // TODO: maybe we don't need active_threads. Instead, we can assume
    // anything in the waker are active;
    active_threads: HashSet<ThreadId>,
    panicked_threads: Vec<ThreadId>,

    thread_waker: HashMap<ThreadId, std::sync::mpsc::SyncSender<bool>>,
    thread_handle: HashMap<ThreadId, std::thread::JoinHandle<()>>,

    ready: Vec<ThreadId>,
    joining: HashMap<ThreadId, IndexSet<ThreadId>>,
    mutex_id: MutexId,
    mutex_wait: HashMap<MutexId, IndexSet<ThreadId>>,
    rwmutex_id: RwMutexId,
    rwmutex_read_wait: HashMap<RwMutexId, IndexSet<ThreadId>>,
    rwmutex_write_wait: HashMap<RwMutexId, IndexSet<ThreadId>>,
    timer_wait: BTreeSet<(usize, ThreadId)>,
    thread_timer: HashMap<ThreadId, usize>,

    files: HashMap<PathBuf, FileInternal>,

    rng: StdRng,
    ticks_per_milli: usize,
}

struct FileInternal {
    is_opened: bool,

    persisted_content: Vec<u8>,
    buffered_content: Vec<u8>,
    changes: Vec<(usize, usize)>,

    cursor: usize,
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

    fn spawn(name: &'static str, f: impl FnOnce() + Send + 'static) -> Self::JoinHandle {
        park();
        let result = RUNTIME.with_borrow(|r| {
            let r = r.as_ref().expect("runtime should be valid");
            r.spawn_internal(name, f)
        });
        park();
        result
    }

    fn park() {
        let is_panic = std::thread::panicking();
        if is_panic {
            return;
        }

        let thread_id = THREAD_ID.get();
        log::trace!(thread_id; "thread_parked");

        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            r.ready.push(thread_id);
        });

        switch();
        log::trace!(thread_id; "thread_resumed");
    }

    fn timer(duration: std::time::Duration) -> (Self::Timer, Self::TimerHandle) {
        let ticks_per_milli =
            RUNTIME.with_borrow(|r| r.as_ref().unwrap().internal.lock().ticks_per_milli);
        let state = Arc::new(parking_lot::Mutex::new(TimerState::Idle));
        (
            SimulatedTimer {
                last_ticked: usize::MIN,
                duration: duration.as_millis() as usize * ticks_per_milli,
                state: state.clone(),
            },
            SimulatedTimerHandle(Arc::new(SimulatedTimerHandleInternal { state })),
        )
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
        // TODO: try to simulate park here. Currently, we don't park here because parking
        // might crash if other thread trigger crash simulation. Crashing inside drop can't be
        // handled yet.
        // park();

        log::trace!(thread_id=self.thread_id; "thread_finished");
        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            r.active_threads.remove(&self.thread_id);
            r.thread_waker.remove(&self.thread_id);
            r.thread_handle.remove(&self.thread_id);
            if let Some(s) = r.joining.remove(&self.thread_id) {
                for t in s {
                    r.ready.push(t);
                }
            }
        });

        let is_crash = RUNTIME.with_borrow(|r| {
            r.as_ref()
                .expect("runtime should be valid")
                .internal
                .lock()
                .is_crashing
        });
        if is_crash {
            RUNTIME.with_borrow(|r| {
                let r = r.as_ref().expect("runtime should be valid").internal.lock();
                let trigger = r
                    .main_trigger
                    .as_ref()
                    .expect("main trigger should be valid");

                // it is possible that the main thread already triggered to start the cleanup
                // if there are multiple threads running when the program simulated to crash
                let _ = trigger.try_send(());
            });
            return;
        }

        let is_panic = std::thread::panicking();
        if is_panic {
            RUNTIME.with_borrow(|r| {
                let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
                r.panicked_threads.push(self.thread_id);
                log::error!(thread_id=self.thread_id; "thread_panicked");
            });
        }

        resume_any();
    }
}

impl SimulatedRuntime {
    #[allow(unused)]
    pub fn new(seed: u64) -> Self {
        Self {
            internal: Arc::new(parking_lot::Mutex::<Internal>::new(Internal {
                thread_id: ThreadId(0),
                is_crashing: false,
                is_deadlock: false,
                main_trigger: None,
                ticks: 0,
                ready: vec![],
                active_threads: HashSet::default(),
                panicked_threads: Vec::default(),
                thread_waker: HashMap::default(),
                thread_handle: HashMap::default(),
                joining: HashMap::default(),
                mutex_id: MutexId::default(),
                mutex_wait: HashMap::default(),
                rwmutex_id: RwMutexId::default(),
                rwmutex_read_wait: HashMap::default(),
                rwmutex_write_wait: HashMap::default(),
                timer_wait: BTreeSet::default(),
                thread_timer: HashMap::default(),
                files: HashMap::default(),
                rng: rand::rngs::StdRng::seed_from_u64(seed),
                ticks_per_milli: 10,
            })),
        }
    }

    pub fn run(&mut self, f: impl FnOnce() + Send + 'static) -> RunResult {
        let original_panic_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |panic_info| {
            let Some(s) = panic_info.payload().downcast_ref::<&str>() else {
                return original_panic_hook(panic_info);
            };
            if *s == "simulated_crash" {
                return;
            }
            original_panic_hook(panic_info)
        }));

        let (trigger, waiter) = std::sync::mpsc::sync_channel::<()>(1);
        {
            let mut r = self.internal.lock();
            log::trace!(
                starting_thread_id=r.thread_id,
                starting_mutex_id=r.mutex_id,
                starting_twmutex_id=r.rwmutex_id;
                "simulation_started"
            );
            r.main_trigger = Some(trigger);
        }

        RUNTIME.set(Some(self.clone()));
        self.spawn_internal("simulation_orchestrator", f);
        resume_any();

        waiter
            .recv()
            .expect("waiting main thread should never fail");

        let mut r = self.internal.lock();

        let mut active_threads = String::default();
        for s in r.active_threads.iter().map(|x| format!("{},", x.0)) {
            active_threads.push_str(&s);
        }
        log::trace!(active_threads,count=r.active_threads.len();"main_thread_start_cleanup");

        let is_crashing = r.is_crashing;
        let is_deadlock = r.is_deadlock;
        let wakers = std::mem::take(&mut r.thread_waker);
        let joiners = std::mem::take(&mut r.thread_handle);
        let active_threads = std::mem::take(&mut r.active_threads);
        let panicked_threads = std::mem::take(&mut r.panicked_threads);
        drop(r);

        let result = RunResult {
            is_simulated_crashing: is_crashing,
            failing_threads: panicked_threads.clone(),
            is_deadlock,
            unfinished_threads: active_threads.iter().cloned().collect(),
        };

        if is_crashing {
            for (_, waker) in wakers {
                waker
                    .send(true)
                    .expect("canceling thread should always successfull");
            }

            for (_, joiner) in joiners {
                let _ = joiner.join();
            }
        } else if !active_threads.is_empty() {
            for t in &active_threads {
                wakers.get(t).unwrap().try_send(false).unwrap();
            }
            assert!(is_deadlock);
            panic!(
                "some of the thread are not finished yet {:?}",
                active_threads
            );
        }

        let _ = std::panic::take_hook();

        for panicked_thread_id in &panicked_threads {
            panic!("thread {panicked_thread_id} got panicked");
        }

        log::trace!("simulation_finished");

        let is_failing = result.is_failed();
        if is_failing {
            let mut unfinished_threads = String::default();
            for s in result
                .unfinished_threads
                .iter()
                .map(|x| format!("{},", x.0))
            {
                unfinished_threads.push_str(&s);
            }

            let mut failing_threads = String::default();
            for s in result.failing_threads.iter().map(|x| format!("{},", x.0)) {
                failing_threads.push_str(&s);
            }
            log::error!(failing_threads, unfinished_threads, deadlock=result.is_deadlock;"simulation_failed");
        } else {
            log::info!(is_crashed=result.is_simulated_crashing;"simulation_success");
        }

        let mut r = self.internal.lock();
        r.is_crashing = false;
        r.is_deadlock = false;
        r.main_trigger = None;
        r.ticks = 0;
        r.ready = vec![];
        r.active_threads = HashSet::default();
        r.panicked_threads = Vec::default();
        r.thread_waker = HashMap::default();
        r.thread_handle = HashMap::default();
        r.joining = HashMap::default();
        //r.mutex_id = MutexId::default();
        r.mutex_wait = HashMap::default();
        //r.rwmutex_id = RwMutexId::default();
        r.rwmutex_read_wait = HashMap::default();
        r.rwmutex_write_wait = HashMap::default();
        r.timer_wait = BTreeSet::default();
        r.thread_timer = HashMap::default();

        result
    }

    fn spawn_internal(
        &self,
        name: &'static str,
        f: impl FnOnce() + Send + 'static,
    ) -> SimulatedJoinHandle {
        let (trigger, waiter) = std::sync::mpsc::sync_channel::<bool>(1);
        RUNTIME.with_borrow(|r| {
            let r = r.as_ref().expect("runtime should be valid");
            let cloned_runtime = r.clone();
            let mut r = r.internal.lock();

            let thread_id = r.thread_id.next();
            log::trace!(name,thread_id; "spawning_thread");

            r.thread_waker.insert(thread_id, trigger);
            r.active_threads.insert(thread_id);
            r.ready.push(thread_id);

            let handle = std::thread::spawn(move || {
                RUNTIME.set(Some(cloned_runtime));
                THREAD_ID.set(thread_id);
                THREAD_WAITER.set(Some(waiter));

                let cleanup = SpawnCleanup { thread_id };
                sleep();
                log::trace!(thread_id; "thread_started");
                f();
                drop(cleanup);
            });
            r.thread_handle.insert(thread_id, handle);

            SimulatedJoinHandle(thread_id)
        })
    }
}

#[derive(Debug)]
pub struct RunResult {
    is_simulated_crashing: bool,
    failing_threads: Vec<ThreadId>,
    is_deadlock: bool,
    unfinished_threads: Vec<ThreadId>,
}

impl RunResult {
    pub fn is_failed(&self) -> bool {
        !self.is_simulated_crashing
            && (self.is_deadlock
                || !self.failing_threads.is_empty()
                || !self.unfinished_threads.is_empty())
    }
}

fn resume_any() {
    RUNTIME.with_borrow(|r| {
        let mut r = r.as_ref().expect("runtime should be valid").internal.lock();

        // set threads that are awaken by the timer to ready
        while let Some((tick, thread_id)) = r.timer_wait.first().cloned() {
            if r.ticks >= tick {
                r.timer_wait.pop_first().unwrap();
                r.thread_timer.remove(&thread_id);
                r.ready.push(thread_id);
            } else {
                break;
            }
        }

        if !r.ready.is_empty() {
            let resuming_thread_index = r.rng.next_u64() as usize % r.ready.len();
            let resuming_thread_id = r.ready.remove(resuming_thread_index);
            log::trace!(thread_id=resuming_thread_id; "thread_resuming");
            r.ticks += 1;
            let waker = r
                .thread_waker
                .get(&resuming_thread_id)
                .expect("waker should exists");
            waker
                .try_send(r.is_crashing)
                .expect("resuming thread should always successfull");
            return;
        }

        if !r.timer_wait.is_empty() {
            // if nothing is ready, just wake up the first thread in
            // the timer waiting list and update the ticks
            // this is like jumping into the future.
            let (target, thread_id) = r.timer_wait.pop_first().expect("nothing is ready");
            log::trace!(thread_id; "timer_resuming");
            r.thread_timer.remove(&thread_id);
            r.ticks = target + 1;
            let waker = r
                .thread_waker
                .get(&thread_id)
                .expect("waker should exists")
                .clone();
            waker
                .try_send(r.is_crashing)
                .expect("resuming thread should always successfull");
            return;
        }

        log::trace!("no_more_thread_to_resume");
        r.is_deadlock = !r.active_threads.is_empty();
        let trigger = r
            .main_trigger
            .as_ref()
            .expect("main trigger should be valid");
        let _ = trigger.try_send(());
    });
}

impl Clone for SimulatedRuntime {
    fn clone(&self) -> Self {
        Self {
            internal: self.internal.clone(),
        }
    }
}

pub struct SimulatedTimer {
    last_ticked: usize,
    duration: usize,
    state: Arc<parking_lot::Mutex<TimerState>>,
}

enum TimerState {
    Idle,
    Wait(ThreadId),
    Triggered(usize),
    Closing,
}

impl runtime::Timer for SimulatedTimer {
    fn wait(&mut self) -> bool {
        park();

        let thread_id = THREAD_ID.get();

        loop {
            let mut state = self.state.lock();
            let current = get_tick();
            match *state {
                TimerState::Idle => {
                    *state = TimerState::Wait(thread_id);
                }
                TimerState::Wait(t) => {
                    assert!(t == thread_id);

                    let elapsed = current - self.last_ticked;
                    if elapsed >= self.duration {
                        self.last_ticked = current;
                        *state = TimerState::Idle;
                        return true;
                    }
                    drop(state);

                    RUNTIME.with_borrow(|r| {
                        let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
                        let next_tick = self.last_ticked + self.duration;
                        r.timer_wait.insert((next_tick, thread_id));
                        r.thread_timer.insert(thread_id, next_tick);
                    });
                    switch();
                }
                TimerState::Triggered(n) => {
                    assert!(n > 0);
                    if n == 1 {
                        *state = TimerState::Idle;
                    } else {
                        *state = TimerState::Triggered(n - 1);
                    }
                    drop(state);

                    return true;
                }
                TimerState::Closing => {
                    drop(state);
                    return false;
                }
            }
        }
    }
}

pub struct SimulatedTimerHandle(Arc<SimulatedTimerHandleInternal>);

struct SimulatedTimerHandleInternal {
    state: Arc<parking_lot::Mutex<TimerState>>,
}

impl Clone for SimulatedTimerHandle {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl runtime::TimerHandle for SimulatedTimerHandle {
    fn trigger(&self) {
        log::trace!("simulated_timer_handle_dropped");
        park();

        let mut state = self.0.state.lock();
        match *state {
            TimerState::Idle => {
                *state = TimerState::Triggered(1);
            }
            TimerState::Wait(thread_id) => {
                log::trace!(triggered_by=THREAD_ID.get(), thread_id;"trigger_waiting_timer");
                *state = TimerState::Triggered(1);
                RUNTIME.with_borrow(|r| {
                    let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
                    let tick = r
                        .thread_timer
                        .remove(&thread_id)
                        .expect("the waiting thread should exists");
                    r.timer_wait.remove(&(tick, thread_id));
                    r.ready.push(thread_id);
                });
            }
            TimerState::Triggered(n) => {
                *state = TimerState::Triggered(n + 1);
            }
            TimerState::Closing => {}
        }
    }
}

impl Drop for SimulatedTimerHandleInternal {
    fn drop(&mut self) {
        // TODO: try to simulate park here. Currently, we don't park here because parking
        // might crash if other thread trigger crash simulation. Crashing inside drop can't be
        // handled yet.
        // park();

        *self.state.lock() = TimerState::Closing;
    }
}

pub struct SimulatedMutex<T: Send + Sync> {
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

    #[track_caller]
    fn lock(&self) -> Self::Guard<'_> {
        let loc = format!("{}", std::panic::Location::caller());
        let is_panic = std::thread::panicking();

        park();

        loop {
            let current = THREAD_ID.get();
            let mut locker = self.locker.lock();
            if let Some(blocker) = *locker {
                drop(locker);
                log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id,blocker,is_panic,loc; "mutex_acquiring_blocked");
                enqueue_mutex(self.id);
                switch();
            } else {
                *locker = Some(current);
                break;
            }
        }
        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id,is_panic,loc; "mutex_acquired");

        let guard = SimulatedMutexGuard {
            id: self.id,
            locker: &self.locker,
            guard: self.value.lock(),
        };

        // WARNING: it is imporant to put park after the guard is constructed to make sure that the
        // guard will be dropped when the program crash while the thread is parked.
        park();
        guard
    }

    #[track_caller]
    fn try_lock(&self) -> Option<Self::Guard<'_>> {
        let loc = format!("{}", std::panic::Location::caller());

        park();

        {
            let current = THREAD_ID.get();
            let mut locker = self.locker.lock();
            if let Some(blocker) = *locker {
                log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id,blocker,loc; "mutex_try_acquiring_failed");
                return None;
            } else {
                *locker = Some(current);
            }
        }
        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id,loc; "mutex_try_acquired");

        let result = Some(SimulatedMutexGuard {
            id: self.id,
            locker: &self.locker,
            guard: self.value.lock(),
        });

        // WARNING: it is imporant to put park after the guard is constructed to make sure that the
        // guard will be dropped when the program crash while the thread is parked.
        park();

        result
    }
}

pub struct SimulatedMutexGuard<'a, T: Send + Sync> {
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
        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id; "mutex_released");

        // TODO: try to simulate park here. Currently, we don't park here because parking
        // might crash if other thread trigger crash simulation. Crashing inside drop can't be
        // handled yet.
        // park();

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

pub struct SimulatedRwMutex<T: Send + Sync> {
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

    #[track_caller]
    fn read(&self) -> Self::ReadGuard<'_> {
        park();
        let current = THREAD_ID.get();
        let loc = format!("{}", std::panic::Location::caller());

        loop {
            let mut locker = self.locker.lock();
            match *locker {
                RwMutexState::Read(n) => {
                    *locker = RwMutexState::Read(n + 1);
                    break;
                }
                RwMutexState::ReadBlocked(_) => {
                    drop(locker);
                    log::trace!(thread_id=current,rwmutex_id=self.id,loc; "rwmutex_read_acquiring_blocked_1");
                    enqueue_rwmutex_for_read(self.id);
                    switch();
                }
                RwMutexState::Write(writer) => {
                    drop(locker);
                    log::trace!(thread_id=current,rwmutex_id=self.id,writer,loc; "rwmutex_read_acquiring_blocked_2");
                    enqueue_rwmutex_for_read(self.id);
                    switch();
                }
                RwMutexState::Unlocked => {
                    *locker = RwMutexState::Read(1);
                    break;
                }
            }
        }

        log::trace!(thread_id=current,mutex_id=self.id,loc; "rwmutex_read_acquired");

        let guard = SimulatedRwMutexReadGuard {
            id: self.id,
            locker: &self.locker,
            guard: self.value.read(),
        };

        // WARNING: it is imporant to put park after the guard is constructed to make sure that the
        // guard will be dropped when the program crash while the thread is parked.
        park();
        guard
    }

    #[track_caller]
    fn write(&self) -> Self::WriteGuard<'_> {
        park();

        let loc = format!("{}", std::panic::Location::caller());

        loop {
            let current = THREAD_ID.get();
            let mut locker = self.locker.lock();
            match *locker {
                RwMutexState::Read(n) => {
                    log::trace!(thread_id=current,rwmutex_id=self.id,loc; "rwmutex_write_acquiring_blocked_1");
                    *locker = RwMutexState::ReadBlocked(n);
                    drop(locker);
                    enqueue_rwmutex_for_write(self.id);
                    switch();
                }
                RwMutexState::ReadBlocked(_) | RwMutexState::Write(_) => {
                    log::trace!(thread_id=current,rwmutex_id=self.id,loc; "rwmutex_write_acquiring_blocked_2");
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

        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id,loc; "rwmutex_write_acquired");

        let guard = SimulatedRwMutexWriteGuard {
            id: self.id,
            locker: &self.locker,
            guard: Some(self.value.write()),
        };

        // WARNING: it is imporant to put park after the guard is constructed to make sure that the
        // guard will be dropped when the program crash while the thread is parked.
        park();
        guard
    }

    #[track_caller]
    fn try_write(&self) -> Option<Self::WriteGuard<'_>> {
        park();

        let loc = format!("{}", std::panic::Location::caller());

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

        log::trace!(thread_id=THREAD_ID.get(),mutex_id=self.id,loc; "rwmutex_try_write_acquired");
        let result = Some(SimulatedRwMutexWriteGuard {
            id: self.id,
            locker: &self.locker,
            guard: Some(self.value.write()),
        });

        // WARNING: it is imporant to put park after the guard is constructed to make sure that the
        // guard will be dropped when the program crash while the thread is parked.
        park();
        result
    }
}

pub struct SimulatedRwMutexReadGuard<'a, T> {
    id: RwMutexId,
    locker: &'a parking_lot::Mutex<RwMutexState>,
    guard: parking_lot::RwLockReadGuard<'a, T>,
}

impl<'a, T> Drop for SimulatedRwMutexReadGuard<'a, T> {
    fn drop(&mut self) {
        // TODO: try to simulate park here. Currently, we don't park here because parking
        // might crash if other thread trigger crash simulation. Crashing inside drop can't be
        // handled yet.
        // park();

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
    fn from(mut value: SimulatedRwMutexWriteGuard<'a, T>) -> Self {
        park();

        {
            let thread_id = THREAD_ID.get();
            log::trace!(thread_id,rwmutex_id=value.id; "rwmutex_write_downgrade");
            let mut state = value.locker.lock();
            assert!(matches!(*state, RwMutexState::Write(..)));
            *state = RwMutexState::Read(1);
        }

        release_all_rwmutex_waiter(value.id);

        let guard = parking_lot::RwLockWriteGuard::downgrade(value.guard.take().unwrap());
        SimulatedRwMutexReadGuard {
            id: value.id,
            locker: value.locker,
            guard,
        }
    }
}

pub struct SimulatedRwMutexWriteGuard<'a, T> {
    id: RwMutexId,
    locker: &'a parking_lot::Mutex<RwMutexState>,
    guard: Option<parking_lot::RwLockWriteGuard<'a, T>>,
}

impl<'a, T> Drop for SimulatedRwMutexWriteGuard<'a, T> {
    fn drop(&mut self) {
        if self.guard.is_none() {
            return;
        }

        // TODO: try to simulate park here. Currently, we don't park here because parking
        // might crash if other thread trigger crash simulation. Crashing inside drop can't be
        // handled yet.
        // park();

        log::trace!(thread_id=THREAD_ID.get(),rwmutex_id=self.id; "rwmutex_write_released");
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
        self.guard.as_ref().unwrap().deref()
    }
}

impl<'a, T> DerefMut for SimulatedRwMutexWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().unwrap().deref_mut()
    }
}

pub struct SimulatedJoinHandle(ThreadId);

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
    resume_any();
    sleep();
}

fn get_tick() -> usize {
    RUNTIME.with_borrow(|r| {
        let r = r.as_ref().expect("runtime should be valid").internal.lock();
        r.ticks
    })
}

fn sleep() {
    let is_crashing = THREAD_WAITER.with_borrow(|w| {
        w.as_ref()
            .expect("waiter should exists")
            .recv()
            .expect("waiting a thread should never fail")
    });

    if is_crashing {
        log::trace!(thread_id=THREAD_ID.get(); "thread_aborted_due_to_crash");
        panic!("simulated_crash");
    }
}

fn park() {
    SimulatedRuntime::park();
}

fn crash() {
    log::trace!(thread_id=THREAD_ID.get(); "simulated_crash_triggered");
    RUNTIME.with_borrow(|r| {
        let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
        r.is_crashing = true;
    });
    panic!("simulated_crash");
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

pub struct SimulatedFile {
    path: PathBuf,
}

impl Drop for SimulatedFile {
    fn drop(&mut self) {
        // TODO: try to simulate park here. Currently, we don't park here because parking
        // might crash if other thread trigger crash simulation. Crashing inside drop can't be
        // handled yet.
        // park();

        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let file = r.files.get_mut(&self.path).expect("file is missing");
            assert!(file.is_opened);
            file.is_opened = false;
            file.buffered_content = file.persisted_content.clone();
            file.changes.clear();
        });
    }
}

impl runtime::File for SimulatedFile {
    #[inline]
    fn open(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        // TODO: maybe need to simulate various io error when opening file, like permission denied.
        // or just return generic error.
        Ok(RUNTIME.with_borrow(|r| -> SimulatedFile {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let entry = r.files.entry(path.as_ref().to_path_buf());
            let file = entry.or_insert(FileInternal {
                is_opened: false,
                persisted_content: vec![],
                buffered_content: vec![],
                changes: vec![],
                cursor: 0,
            });

            if file.is_opened {
                panic!("file is already opened");
            }
            file.is_opened = true;
            file.cursor = 0;
            file.changes = vec![];
            file.buffered_content = file.persisted_content.clone();

            SimulatedFile {
                path: path.as_ref().to_path_buf(),
            }
        }))
    }

    fn is_file(&self) -> std::io::Result<bool> {
        // TODO: maybe need to simulate these cases:
        // - non regular file
        // - io errors
        Ok(true)
    }

    fn len(&self) -> std::io::Result<u64> {
        // TODO: simulate error case
        // TODO: think about the error case. currently, when `truncate` returns,
        // we assume everything is success even though we don't call fsync.
        Ok(RUNTIME.with_borrow(|r| {
            let r = r.as_ref().expect("runtime should be valid").internal.lock();
            r.files
                .get(&self.path)
                .expect("the file should exists")
                .buffered_content
                .len() as u64
        }))
    }

    #[inline]
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<()> {
        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let file = r.files.get_mut(&self.path).expect("the file should exists");
            let mut current = file.cursor;
            match position {
                std::io::SeekFrom::Start(v) => current = v as usize,
                std::io::SeekFrom::End(v) => current = file.buffered_content.len() + v as usize,
                std::io::SeekFrom::Current(v) => current = (current as i64 + v) as usize,
            }
            file.cursor = current;
        });
        Ok(())
    }

    #[inline]
    fn read(&mut self, buff: &mut [u8]) -> std::io::Result<usize> {
        Ok(RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let file = r.files.get_mut(&self.path).expect("the file should exists");

            let mut size = buff.len();
            if file.cursor + buff.len() > file.buffered_content.len() {
                size = file.buffered_content.len() - file.cursor;
            }

            for i in 0..size {
                buff[i] = file.buffered_content[file.cursor + i];
            }
            file.cursor += size;

            size
        }))
    }

    #[inline]
    fn read_exact(&mut self, buff: &mut [u8]) -> std::io::Result<()> {
        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let file = r.files.get_mut(&self.path).expect("the file should exists");

            if file.cursor + buff.len() > file.buffered_content.len() {
                todo!("read beyond file length, should this error or panic?");
            }
            for i in 0..buff.len() {
                buff[i] = file.buffered_content[file.cursor + i];
            }
            file.cursor += buff.len();
        });
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buff: &[u8]) -> std::io::Result<()> {
        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let file = r.files.get_mut(&self.path).expect("the file should exists");
            while file.cursor + buff.len() > file.buffered_content.len() {
                file.buffered_content.push(0);
            }
            for (i, b) in buff.iter().cloned().enumerate() {
                file.buffered_content[file.cursor + i] = b;
            }
            file.changes.push((file.cursor, buff.len()));
            file.cursor += buff.len();
        });
        Ok(())
    }

    #[inline]
    fn sync(&mut self) -> std::io::Result<()> {
        let will_crash = RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let r = r.deref_mut();
            r.rng.gen_bool(0.1)
        });

        log::trace!(will_crash; "sync_file");

        if !will_crash {
            RUNTIME.with_borrow(|r| {
                let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
                let r = r.deref_mut();
                let file = r.files.get_mut(&self.path).expect("the file should exists");
                file.persisted_content = file.buffered_content.clone();
                file.changes.clear();
            });
            return Ok(());
        }

        let mut changes = RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let r = r.deref_mut();
            let file = r.files.get_mut(&self.path).expect("the file should exists");
            let mut changes = Vec::default();
            for (offset, size) in file.changes.drain(..) {
                for i in offset..offset + size {
                    changes.push((i, file.buffered_content[i]));
                }
            }
            changes.shuffle(&mut r.rng);
            changes
        });

        let changes_to_apply = RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let r = r.deref_mut();
            r.rng.gen_range(0..=changes.len())
        });

        for (i, b) in changes.drain(..changes_to_apply) {
            let should_park = RUNTIME.with_borrow(|r| {
                let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
                let r = r.deref_mut();
                let file = r.files.get_mut(&self.path).expect("the file should exists");

                log::trace!(i, b; "sync_file_1");
                while file.persisted_content.len() <= i {
                    file.persisted_content.push(0);
                }
                file.persisted_content[i] = b;
                r.rng.gen_bool(0.1)
            });

            if should_park {
                park();
            }
        }

        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let r = r.deref_mut();
            let file = r.files.get_mut(&self.path).expect("the file should exists");
            file.buffered_content = file.persisted_content.clone();
            file.changes.clear();
            file.cursor = 0;
        });

        crash();
        Ok(())
    }

    #[inline]
    fn truncate(&mut self, size: u64) -> std::io::Result<()> {
        RUNTIME.with_borrow(|r| {
            let mut r = r.as_ref().expect("runtime should be valid").internal.lock();
            let file = r.files.get_mut(&self.path).expect("the file should exists");
            if size as usize > file.buffered_content.len() {
                let to_add = size as usize - file.buffered_content.len();
                file.buffered_content
                    .extend(std::iter::repeat(0).take(to_add));
                // TODO: should the file.changes recorded?
            } else {
                file.buffered_content.truncate(size as usize);
                // TODO: should the file.changes recorded?
            }
        });
        Ok(())
    }
}

macro_rules! impl_atomic {
    ($name:ident, $ty:ident) => {
        pub struct $name(parking_lot::Mutex<$ty>);
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
    use crate::runtime::*;

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
        let mut runtime = SimulatedRuntime::new(0);
        runtime.run(|| {
            let handle = SimulatedRuntime::spawn("t2", || {});
            handle.join();
        });
    }

    #[test]
    fn test_simple_atomic() {
        setup();
        let mut runtime = SimulatedRuntime::new(0);
        runtime.run(|| {
            let a = Arc::new(AtomicI32::new(0));
            let x = a.clone();
            let handle1 = SimulatedRuntime::spawn("t2", move || {
                for _ in 0..10000 {
                    x.fetch_add(20);
                }
            });
            let x = a.clone();
            let handle2 = SimulatedRuntime::spawn("t3", move || {
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
        let mut runtime = SimulatedRuntime::new(0);
        runtime.run(|| {
            let a = Arc::new(SimulatedMutex::new(0));
            let x = a.clone();
            let handle1 = SimulatedRuntime::spawn("t2", move || {
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
            let handle2 = SimulatedRuntime::spawn("t3", move || {
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
            let handle3 = SimulatedRuntime::spawn("t4", move || {
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
        let mut runtime = SimulatedRuntime::new(0);
        runtime.run(|| {
            let a = Arc::new(SimulatedRwMutex::new((0, 0)));

            let mut joins = vec![];
            for _ in 0..100 {
                let x = a.clone();
                let handle = SimulatedRuntime::spawn("t2", move || {
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
                let handle = SimulatedRuntime::spawn("t3", move || {
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

    #[test]
    fn test_simple_timer() {
        setup();
        let mut runtime = SimulatedRuntime::new(0);
        runtime.run(|| {
            let counter = AtomicUsize::new(0);
            let timer_count = Arc::new(AtomicUsize::new(0));
            let (mut timer, timer_handle) =
                SimulatedRuntime::timer(std::time::Duration::from_millis(100));

            let h1 = SimulatedRuntime::spawn("t2", move || {
                let timer_handle = timer_handle;
                for i in 0..10000 {
                    if i == 1234 {
                        timer_handle.trigger();
                    }
                    counter.fetch_add(1);
                }
            });

            let c = timer_count.clone();
            let h2 = SimulatedRuntime::spawn("t3", move || {
                while timer.wait() {
                    c.fetch_add(1);
                }
            });

            h1.join();
            h2.join();

            assert_eq!(21, timer_count.load());
        });
    }

    #[test]
    fn test_writing_file() {
        setup();
        let mut runtime = SimulatedRuntime::new(0);
        let rng: Arc<parking_lot::Mutex<StdRng>> = Arc::new(parking_lot::Mutex::new(
            rand::rngs::StdRng::seed_from_u64(0),
        ));
        let n = 10u64;

        let success = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        use std::sync::atomic::Ordering::SeqCst;

        for _ in 0..100 {
            let rng = rng.clone();
            let success = success.clone();
            let failed = failed.clone();
            runtime.run(move || {
                let mut f = SimulatedFile::open("dummy").expect("cannot open file");
                let size = f.len().unwrap();
                assert!(size <= n);
                if size == n {
                    let mut buff = vec![0u8; n as usize];
                    f.read_exact(&mut buff).unwrap();
                    let is_fail = buff[1..].iter().any(|b| *b != buff[0]);
                    if is_fail {
                        failed.store(true, SeqCst);
                    } else {
                        success.store(true, SeqCst);
                    }
                }

                let v = rng.lock().next_u64() as u8;
                f.seek(std::io::SeekFrom::Start(0)).unwrap();
                f.write_all(vec![v; n as usize].as_slice())
                    .expect("cannot write bytes");

                f.sync().unwrap();
            });
        }

        assert!(success.load(SeqCst) && failed.load(SeqCst))
    }

    #[test]
    fn test_atomically_writing_file() {
        setup();
        let mut runtime = SimulatedRuntime::new(0);
        let rng: Arc<parking_lot::Mutex<StdRng>> = Arc::new(parking_lot::Mutex::new(
            rand::rngs::StdRng::seed_from_u64(0),
        ));
        let n = 10u64;

        for _ in 0..100 {
            let rng = rng.clone();
            runtime.run(move || {
                let mut f1 = SimulatedFile::open("dummy_1").expect("cannot open file");
                let mut f2 = SimulatedFile::open("dummy_2").expect("cannot open file");

                if f2.len().unwrap() == n + 8 {
                    let mut buff = vec![0u8; n as usize + 8];
                    f2.read_exact(&mut buff).unwrap();
                    let calculated_checksum = crc64::crc64(0x1d0f, &buff[..n as usize]);
                    let checksum = u64::from_be_bytes(buff[n as usize..].try_into().unwrap());
                    if calculated_checksum == checksum {
                        f1.seek(std::io::SeekFrom::Start(0)).unwrap();
                        f1.write_all(&buff[..n as usize]).unwrap();
                    }
                }

                let size = f1.len().unwrap();
                assert!(size == 0 || size == n);
                if size == n {
                    let mut buff = vec![0u8; n as usize];
                    f1.seek(std::io::SeekFrom::Start(0)).unwrap();
                    f1.read_exact(&mut buff).unwrap();
                    assert!(buff[1..].iter().all(|b| *b == buff[0]));
                }

                let v = rng.lock().next_u64() as u8;
                let to_write = vec![v; n as usize];

                let checksum = crc64::crc64(0x1d0f, &to_write);
                let checksum = checksum.to_be_bytes();

                f2.seek(std::io::SeekFrom::Start(0)).unwrap();
                f2.write_all(&to_write).unwrap();
                f2.write_all(&checksum).unwrap();
                f2.sync().unwrap();

                f1.seek(std::io::SeekFrom::Start(0)).unwrap();
                f1.write_all(&to_write).expect("cannot write bytes");
                f1.sync().unwrap();
            });
        }
    }
}
