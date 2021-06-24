#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::io;

use tokio::runtime::{Runtime, Builder, EnterGuard};
use tokio::net::{TcpListener, TcpSocket};
use tracing::{debug, error, info_span, Level};

use crate::riverdb::pg::PostgresSession;
use crate::riverdb::common::{Result, Error};
use std::net::{SocketAddr, IpAddr};


/// Worker represents a Worker thread and serves as a thread-local storage
/// for all the resources the worker thread accesses. This includes
/// the tokio and glommio runtimes, random number generators, and
/// sharded data structures.
pub struct Worker {
    tokio: Runtime,
    worker_id: u32,
}

impl Worker {
    pub fn new() -> Result<Worker> {
        let tokio = Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Worker {
            tokio,
            worker_id: 0,
        })
    }

    pub fn listener(&self, reuseport: bool, enter_tokio: bool) -> Result<&'static TcpListener> {
        let mut _guard = None;
        if enter_tokio {
            _guard = Some(self.tokio.enter());
        }
        let addr = "127.0.0.1:5433".parse()?;
        let sock = TcpSocket::new_v4()?;
        if cfg!(unix) {
            if reuseport {
                sock.set_reuseport(true)?;
            }
            // If we're on linux, set TCP_DEFER_ACCEPT
            // The client always sends the first data after connecting.
            if std::env::consts::OS == "linux" {
                unsafe {
                    let optval: libc::c_int = 1;
                    let ret = libc::setsockopt(
                        sock.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::TCP_DEFER_ACCEPT,
                        &optval as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&optval) as libc::socklen_t);
                    if ret != 0 {
                        return Err(Error::from(io::Error::last_os_error()));
                    }
                }
            }
        }
        sock.bind(addr)?;
        Ok(&*Box::leak(Box::new(sock.listen(1024)?)))
    }

    pub fn run_forever(mut self, postgres_listener: Option<&'static TcpListener>, worker_id: u32) {
        // If worker.run fails, create a new Worker and call run again
        // TODO catch panics and gracefully shutdown the process
        // The most common cause of a panic will be OOM, and that's best dealt with by
        // restarting gracefully to eliminate any memory fragmentation.
        // The next most common causes would be bugs and hardware errors. Neither of those
        // necessarily leave the system in a good state, so restarting is the best we can hope for.
        loop {
            self.run(postgres_listener, worker_id);
            self = match Worker::new() {
                Ok(worker) => worker,
                Err(e) => {
                    error!(%e, worker_id, "cannot create worker");
                    // TODO graceful shutdown
                    std::process::exit(-1);
                },
            };
        }
    }

    fn run(&mut self, postgres_listener: Option<&'static TcpListener>, worker_id: u32) {
        self.worker_id = worker_id;

        let _guard = self.tokio.enter();
        // If we didn't get passed a listener, create a sharded listener using SO_REUSEPORT
        let listener = match postgres_listener {
            Some(listener) => listener,
            // panic is what we want here, we'll catch it in the caller and shutdown
            None => self.listener(true, false).expect("could not create tcp listener"),
        };

        if let Err(e) = self.tokio.block_on(async move {
            accept_loop(worker_id, listener).await
        }) {
            error!(%e, worker_id, "fatal error in accept_loop");
        }
    }
}

async fn accept_loop(worker_id: u32, listener: &TcpListener) -> Result<()> {
    let _span = info_span!("accept_loop", worker_id);
    let tokio = tokio::runtime::Handle::current();
    loop {
        let sock = match listener.accept().await {
            Ok((sock, remote_addr)) => {
                debug!(fd = sock.as_raw_fd(), %remote_addr, "accept postgres connection");
                sock
            },
            Err(e) => {
                if cfg!(unix) && std::env::consts::OS == "linux" {
                    // Return an error only if it's not one of several known recoverable errors.
                    match e.raw_os_error().unwrap_or(0) {
                        libc::ECONNABORTED |
                        libc::EMFILE |
                        libc::ENFILE |
                        libc::ENOBUFS |
                        libc::ENOMEM |
                        libc::EPROTO => (),
                        _ => return Err(Error::from(e)),
                    }
                }
                // Log the error
                error!(%e, "accept error");
                continue;
            },
        };
        tokio.spawn(async move {
            if let(Err(e)) = PostgresSession::new(sock).run().await {
                error!(%e, "postgres session error");
            }
        });
    }
}