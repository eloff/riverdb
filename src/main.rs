//#![cfg(feature = "main")]
#![allow(unused_doc_comments)]

pub mod riverdb;

use tracing::{info_span, Level};

use ::riverdb::{init_tracing, init_settings, init_runtime, run_servers};

fn main() {
    // TODO start a watchdog process (that won't die when this process dies!)
    // which monitors this process and restarts it with the same command line arguments if it dies.
    // If we intentionally shut it down, we kill the watchdog here first before exiting.

    init_tracing(Level::TRACE);

    let _span = info_span!("startup").entered();

    let conf = init_settings().expect("could not load config");

    let tokio = init_runtime(conf).expect("could not create tokio runtime");

    // TODO catch panics and gracefully shutdown the process
    // The most common cause of a panic will be OOM, and that's best dealt with by
    // restarting gracefully to eliminate any memory fragmentation.
    // The next most common causes would be bugs and hardware errors. Neither of those
    // necessarily leave the system in a good state, so restarting is the best we can hope for.
    // std::panic::set_hook();

    run_servers(conf, &tokio);

    // TODO wait for shutdown to complete
}
