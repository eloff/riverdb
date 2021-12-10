use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

use tokio::time::{interval, Instant, Duration};
use crate::riverdb::config::COARSE_CLOCK_GRANULARITY_SECONDS;

/// A global, shared atomic clock that is advanced by calling update_coarse_monotonic_clock.
static COARSE_CLOCK: AtomicU32 = AtomicU32::new(0);

/// Return the current value of the clock. Roughly accurate to COARSE_CLOCK_GRANULARITY_SECONDS.
/// It provides a less accurate but more efficient monotonic time value that fits in 32 bits.
pub fn coarse_monotonic_now() -> u32 {
    COARSE_CLOCK.load(Relaxed)
}

/// Update the stored value for the clock.
/// To be called periodically no more often than once per second.
fn update_coarse_monotonic_clock() {
    static mut START: Option<Instant> = None;

    // Safety: only one thread calls this at a time
    unsafe {
        match START {
            Some(start) => {
                let now = start.elapsed().as_secs() as u32;
                COARSE_CLOCK.store(now, Relaxed);
            },
            None => {
                START = Some(Instant::now());
            }
        }
    }
}

/// An infinite async task that updates the clock every COARSE_CLOCK_GRANULARITY_SECONDS seconds.
pub async fn coarse_monotonic_clock_updater() {
    let mut interval = interval(Duration::from_secs(COARSE_CLOCK_GRANULARITY_SECONDS));
    loop {
        interval.tick().await;
        update_coarse_monotonic_clock();
    }
}