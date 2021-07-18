#![allow(unused_imports)]
#![allow(unused_variables)]

mod riverdb;

use std::thread;

use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tracing_subscriber::FmtSubscriber;
use tracing::{info, info_span, Level};

use crate::riverdb::worker::Worker;
use crate::riverdb::config::{conf, load_config};
use crate::riverdb::pg::PostgresService;
use crate::riverdb::worker::init_workers;


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

    let _span = info_span!("startup").entered();

    let conf = load_config().expect("could not load config");

    // This is unsafe to call after the server starts. It's safe here.
    unsafe {
        init_workers(conf.num_workers);
    }

    let tokio = Builder::new_multi_thread()
        .worker_threads(conf.num_workers as usize)
        .enable_all()
        // Eagerly assign a thread-local worker to each original tokio worker thread
        // (this is a no-op later for additional tokio threads for blocking tasks)
        .on_thread_start(|| { Worker::try_get(); })
        .build()
        .expect("could not create tokio runtime");

    // TODO catch panics and gracefully shutdown the process
    // The most common cause of a panic will be OOM, and that's best dealt with by
    // restarting gracefully to eliminate any memory fragmentation.
    // The next most common causes would be bugs and hardware errors. Neither of those
    // necessarily leave the system in a good state, so restarting is the best we can hope for.
    // std::panic::set_hook();

    tokio.block_on(async move {
        let mut handles = Vec::new();
        // If reuseport is false, we create a single TcpListener.
        // Otherwise we create one per tokio worker. This reduces contention sharing accepted
        // sockets between worker threads (less work stealing) and reduces kernel lock contention
        // in accept. The downside is it won't error if you assign a port that is in use.
        // (hopefully these end up distributed nicely across tokio worker threads,
        // but I don't see a way to control that.)
        let num_listeners = if conf.reuseport { conf.num_workers } else { 1 };

        // Postgres service
        if conf.postgres.port != 0 {
            handles.push(tokio::spawn(async move {
                let service = PostgresService::new(
                    conf.postgres_listen_address(),
                    conf.postgres.max_connections,
                    conf.postgres.idle_timeout_seconds,
                    conf.reuseport);
                service.run().await
            }));
        }

        // // HTTP service
        // if conf.http_port != 0 {
        //     handles.push(tokio::spawn(async {
        //         let service = HttpService::new(conf.http_listen_address(), conf.reuseport);
        //         service.run().await
        //     }));
        // }
        //
        // // HTTPS service
        // if conf.https_port != 0 {
        //     handles.push(tokio::spawn(async {
        //         let service = HttpsService::new(conf.https_listen_address(), conf.reuseport);
        //         service.run().await
        //     }));
        // }

        // Wait for all listener tasks to shutdown
        for handle in handles.drain(..) {
            handle.await;
        }
    });

    // TODO wait for shutdown to complete
}
