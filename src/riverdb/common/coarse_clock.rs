use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

use tokio::time::{interval, Instant, Duration};
use crate::riverdb::config::COARSE_CLOCK_GRANULARITY_SECONDS;

static COARSE_CLOCK: AtomicU32 = AtomicU32::new(0);

pub fn coarse_monotonic_now() -> u32 {
    COARSE_CLOCK.load(Relaxed)
}

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

pub async fn coarse_monotonic_clock_updater() {
    let mut interval = interval(Duration::from_secs(COARSE_CLOCK_GRANULARITY_SECONDS));
    loop {
        interval.tick().await;
        update_coarse_monotonic_clock();
    }
}