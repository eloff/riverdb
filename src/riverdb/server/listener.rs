use std::io;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::future::Future;

use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tracing::{debug, error, info_span};

use crate::riverdb::{Error, Result};
use crate::riverdb::config::LISTEN_BACKLOG;


pub struct Listener {
    pub address: String,
    listener: TcpListener,
}

impl Listener {
    pub fn new(address: String, reuseport: bool) -> Result<Self> {
        let addr = address.parse()?;
        let sock = TcpSocket::new_v4()?;
        #[cfg(unix)]
        {
            if reuseport {
                sock.set_reuseport(true)?;
            }
            // If we're on linux, set TCP_DEFER_ACCEPT
            // The client always sends the first data after connecting.
            #[cfg(target_os = "linux")]
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
        sock.bind(addr)?;
        let listener = sock.listen(LISTEN_BACKLOG)?;
        Ok(Self {
            address,
            listener,
        })
    }

    pub async fn accept(&self) -> Option<TcpStream>
    {
        loop {
            match self.listener.accept().await {
                Ok((sock, remote_addr)) => {
                    debug!(fd = sock.as_raw_fd(), %remote_addr, server = %self.address.as_str(), "accept connection");
                    return Some(sock);
                },
                Err(e) => {
                    if cfg!(unix) && std::env::consts::OS == "linux" {
                        // Return an error only if it's not one of several known recoverable errors.
                        match e.raw_os_error().unwrap_or(0) {
                            libc::ECONNABORTED |
                            libc::EMFILE | // process file-descriptor limit
                            libc::ENFILE | // system wide file-descriptor limit
                            libc::ENOBUFS | // out of memory
                            libc::ENOMEM | // out of memory
                            libc::EPROTO | // protocol error
                            libc::EINTR => {
                                error!(%e, "accept error");
                                continue;
                            }, // interrupt
                            libc::EBADF => return None, // socket closed, we want to ignore this during shutdown. TODO check if !shutdown and panic.
                            _ => panic!("unrecoverable error on {}: {}", self.address.as_str(), Error::from(e)),
                        }
                    }
                },
            }
        }
    }
}

