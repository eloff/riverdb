mod riverdb;

use std::thread;

use tokio::net::TcpListener;
use tracing_subscriber::FmtSubscriber;
use tracing::{info, info_span, Level};

use riverdb::worker::Worker;



fn main() {
    // TODO start a watchdog process (that won't die when this process dies!)
    // which monitors this process and restarts it with the same command line arguments if it dies.
    // If we intentionally shut it down, we kill the watchdog here first before exiting.

    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let _span = info_span!("startup");

    let num_workers = 2u32; // num_cpus::get(); // TODO take this from config

    let reuseport = false;

    // If reuseport is false, we need a worker to create a TcpListener to share between
    // all the workers, which is why we create one worker outside of the loop like this.
    let mut worker = Worker::new().expect("could not create worker");
    let mut listener = None;
    if !reuseport {
        info!("create shared listener socket");
        listener = Some(worker.listener(reuseport, true).expect("could not create tcp listener"))
    }

    info!("starting workers");
    for i in 1..num_workers {
        thread::spawn(move || {
            info!(worker_id = i, "started worker thread");
            worker.run_forever(listener.clone(), i);
        });
        worker = Worker::new().expect("could not create worker");
    }

    info!(worker_id = num_workers, "started worker");
    worker.run_forever(listener, num_workers);
}
