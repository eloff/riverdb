//#![cfg(not(feature = "main"))]

pub mod riverdb;
#[cfg(test)]
mod tests;

pub use crate::riverdb::*;

use std::io;

use tokio::runtime::{Runtime, Builder};
use tracing_subscriber::FmtSubscriber;
use tracing::{info_span, Level};

use crate::riverdb::worker::Worker;
use crate::riverdb::config::{Settings, load_config};
use crate::riverdb::pg::PostgresService;
use crate::riverdb::worker::init_workers;
use crate::riverdb::common::{Result, coarse_monotonic_clock_updater};


pub fn init_tracing(max_level: Level) {
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(max_level)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");
}

/// Load the configuration settings from riverdb.yaml
/// See riverdb::config::load_config for more info.
pub fn init_settings() -> Result<&'static Settings> {
    load_config("riverdb.yaml")
}

pub fn init_runtime(conf: &'static Settings) -> io::Result<Runtime> {
    // This is unsafe to call after the server starts. It's safe here.
    unsafe {
        init_workers(conf.num_workers);
    }

    Builder::new_multi_thread()
        .worker_threads(conf.num_workers as usize)
        .enable_all()
        // Eagerly assign a thread-local worker to each original tokio worker thread
        // (this is a no-op later for additional tokio threads for blocking tasks)
        .on_thread_start(|| { Worker::try_get(); })
        .build()
}

pub fn run_servers(conf: &'static Settings, tokio: &Runtime) {
    tokio.block_on(async move {
        // Update the coarse monotonic clock on a periodic basis
        tokio::spawn(coarse_monotonic_clock_updater());

        let mut handles = Vec::new();
        // If reuseport is false, we create a single TcpListener.
        // Otherwise we create one per tokio worker. This reduces contention sharing accepted
        // sockets between worker threads (less work stealing) and reduces kernel lock contention
        // in accept. The downside is it won't error if you assign a port that is in use.
        // (hopefully these end up distributed nicely across tokio worker threads,
        // but I don't see a way to control that.)
        let _num_listeners = if conf.reuseport { conf.num_workers } else { 1 };

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
            handle.await.expect("join failed");
        }
    });
}