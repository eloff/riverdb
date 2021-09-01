use std::cell::{Cell};
#[cfg(not(test))]
use std::sync::atomic::AtomicUsize;
#[cfg(not(test))]
use std::sync::atomic::Ordering::Relaxed;

// faster than xorshift128+ and better quality (see https://github.com/lemire/testingRNG)
use nanorand::{WyRand, Rng};

use crate::riverdb::common::fast_modulo32;



thread_local! {
    static CURRENT_WORKER: Cell<*mut Worker> = Cell::new(std::ptr::null_mut());
}

static mut ALL_WORKERS: &[Worker] = &[];

/// Worker represents a Worker thread and serves as a thread-local storage
/// for all the resources the worker thread accesses. This includes
/// the glommio runtime, random number generators, and
/// sharded data structures. It corresponds 1-to-1 with tokio worker threads.
pub struct Worker {
    pub id: u32,
    rng: WyRand,
}

pub unsafe fn init_workers(num_workers: u32) {
    let workers: Vec<_> = (0..num_workers).map(|i| Worker::new(i+1)).collect();
    ALL_WORKERS = &*workers.leak();
}

// All Worker methods take &mut self, because there should never be more than one reference to Worker.
// That's mostly true if you don't hold references to a Worker across await points. Otherwise
// another task on the same tokio runtime can run and get a Worker reference while the first is
// suspended in await. The one place we do break this rule, and is undefined behavior, is to
// iterate over all workers with a shared reference. We only use sync::atomics in that case,
// and only do read-only access, so it's very unlikely LLVM can generate invalid code for that.
// This is used when collecting statistics, for one.
impl Worker {
    pub fn new(id: u32) -> Self {
        Worker {
            id,
            rng: WyRand::new(),
        }
    }

    /// get returns a mutable Worker reference to the thread-local Worker.
    /// panics if not called on one of the original tokio worker threads.
    /// If there's already a reference (e.g. you're holding a reference across an await point)
    /// then this will panic. Keep in mind also that some destructors may call get_worker
    /// to free memory or a resource, so holding this across nested scopes may panic.
    /// That can be resolved by dropping this first.
    pub fn get() -> &'static mut Worker {
        Self::try_get().expect("not a worker thread")
    }

    pub fn try_get() -> Option<&'static mut Worker> {
        #[cfg(test)]
        unsafe {
            let p = ALL_WORKERS.get(0).unwrap() as *const Worker as *mut Worker;
            Some(&mut *p)
        }
        #[cfg(not(test))]
        {
            static NEXT_WORKER: AtomicUsize = AtomicUsize::new(0);
            CURRENT_WORKER.with(|ctx| {
                // Safety: ALL_WORKERS has been initialized before this function is called
                // Casting &Worker to *mut Worker is allowed, but dereferencing that as a &mut Worker
                // is undefined behavior. We could eliminate the need for that, but it would just
                // shift the undefined behavior to a different part of the program. Fundamentally
                // what we're doing here is unsafe in Rust, but would be permitted in C. There's no
                // way around it without moving the code to C. I think Rust is just overly strict
                // here and that LLVM, as a C compiler, will generate sound code. Time will tell.
                unsafe {
                    let mut p = ctx.get();
                    if p.is_null() {
                        // Grab an unallocated worker from ALL_WORKERS
                        if NEXT_WORKER.load(Relaxed) < ALL_WORKERS.len() {
                            let worker = ALL_WORKERS.get_unchecked(NEXT_WORKER.fetch_add(1, Relaxed));
                            p = worker as *const Worker as *mut Worker;
                            ctx.set(p);
                        } else {
                            return None;
                        }
                    }
                    Some(&mut *(p as *mut Worker))
                }
            })
        }
    }

    pub fn rand32(&mut self) -> u32 {
        self.rng.generate()
    }

    pub fn uniform_rand32(&mut self, max: u32) -> u32 {
        fast_modulo32(self.rng.generate(), max)
    }
}