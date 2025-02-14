use indexmap::IndexSet;
use rand::{rngs::StdRng, SeedableRng};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct SimulatedRuntime {
    internal: Arc<parking_lot::Mutex<SimulatedRuntimeInternal>>,
}

impl SimulatedRuntime {
    pub fn new(seed: u64) -> Self {
        Self {
            internal: Arc::new(parking_lot::Mutex::new(SimulatedRuntimeInternal {
                thread_id: ThreadId::default(),
                rng: rand::rngs::StdRng::seed_from_u64(seed),
                ticks_per_milli: 10,
                ticks: 0,
                active_threads: HashMap::default(),
                thread_handles: HashMap::default(),
                ready_threads: Vec::default(),
                joining_threads: HashMap::default(),
                mutex_id: MutexId::default(),
                mutex_blocked_threads: HashMap::default(),
                rwmutex_id: RwMutexId::default(),
                rwmutex_read_blocked_threads: HashMap::default(),
                rwmutex_write_blocked_threads: HashMap::default(),
                timer_waiting_threads: BTreeSet::default(),
                files: HashMap::default(),
            })),
        }
    }

    pub fn run(&mut self, f: impl FnOnce() + Send + 'static) {
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

        let r = self.internal.lock();
        log::info!(
            starting_thread_id=r.thread_id,
            starting_mutex_id=r.mutex_id,
            starting_twmutex_id=r.rwmutex_id;
            "simulation_started"
        );
        drop(r);

        RUNTIME.set(Some(self.clone()));
        self.spawn_internal("main_thread", f);

        loop {
            // check if there is any active thread at all
            // set timer waiter to ready
            // find ready thread
            // wake up any timer waiter
            // othwerwise, there is a deadlock
        }

        self.resume_one();

        let r = self.internal.lock();
        let mut active_threads = String::default();
        for thread_id in r.active_threads.keys() {
            if !active_threads.is_empty() {
                active_threads.push(',');
            }
            active_threads.push_str(&format!("{}", thread_id));
        }
        log::trace!(active_threads,count=r.active_threads.len();"main_thread_start_cleanup");
        drop(r);

        todo!();
    }

    fn spawn_internal(
        &self,
        name: &'static str,
        f: impl FnOnce() + Send + 'static,
    ) -> SimulatedJoinHandle {
        let (waker, sleeper) = new_waking();
        RUNTIME.with_borrow(|r| {
            let r = r.as_ref().expect("runtime should be valid");
            let cloned_runtime = r.clone();
            let mut r = r.internal.lock();
            let thread_id = r.thread_id;
            r.thread_id.0 += 1;

            log::trace!(name,thread_id; "spawning_thread");

            r.active_threads.insert(thread_id, waker);
            r.ready_threads.push(thread_id);

            let handle = std::thread::spawn(move || {
                RUNTIME.set(Some(cloned_runtime));
                THREAD_ID.set(thread_id);
                THREAD_SLEEPER.set(Some(sleeper));

                log::trace!(thread_id; "thread_started");
                sleep();
                f();
            });
            r.thread_handles.insert(thread_id, handle);

            SimulatedJoinHandle { thread_id }
        })
    }

    fn resume_one(&self) {
        todo!();
    }
}

fn sleep() {
    THREAD_SLEEPER.with_borrow(|w| w.as_ref().expect("sleeper should exists").sleep());
}

struct SimulatedRuntimeInternal {
    thread_id: ThreadId,
    rng: StdRng,

    ticks_per_milli: usize,
    ticks: usize,

    active_threads: HashMap<ThreadId, Waker>,
    thread_handles: HashMap<ThreadId, std::thread::JoinHandle<()>>,

    ready_threads: Vec<ThreadId>,

    // map from thread id to all the threads that join it.
    joining_threads: HashMap<ThreadId, IndexSet<ThreadId>>,

    mutex_id: MutexId,
    // map from mutex id to all the threads that trying to lock it but blocked
    mutex_blocked_threads: HashMap<MutexId, IndexSet<ThreadId>>,

    rwmutex_id: RwMutexId,
    // map from rwmutex id to all the threads that trying to acquire shared lock but blocked
    rwmutex_read_blocked_threads: HashMap<RwMutexId, IndexSet<ThreadId>>,
    // map from rwmutex id to all the threads that trying to acquire exclusive lock but blocked
    rwmutex_write_blocked_threads: HashMap<RwMutexId, IndexSet<ThreadId>>,

    // a collection of thread id and when it should be awaker
    timer_waiting_threads: BTreeSet<(usize, ThreadId)>,

    files: HashMap<Arc<Path>, File>,
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
struct ThreadId(usize);

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

enum WakeState {
    Normal,
    Crashing,
}

pub struct File;

thread_local! {
    static RUNTIME: RefCell<Option<SimulatedRuntime>> = RefCell::default();
    static THREAD_ID: Cell<ThreadId> = Cell::default();
    static THREAD_SLEEPER: RefCell<Option<Sleeper>> = RefCell::default();
}

fn new_waking() -> (Waker, Sleeper) {
    let (trigger, waiter) = std::sync::mpsc::sync_channel::<()>(1);
    (Waker(trigger), Sleeper(waiter))
}

struct Waker(std::sync::mpsc::SyncSender<()>);

impl Waker {
    fn wake(&self) {
        self.0.send(()).unwrap();
    }
}

struct Sleeper(std::sync::mpsc::Receiver<()>);

impl Sleeper {
    fn sleep(&self) {
        self.0.recv().unwrap();
    }
}

fn park() {
    todo!();
}

struct SimulatedJoinHandle {
    thread_id: ThreadId,
}
