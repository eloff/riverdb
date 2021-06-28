use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd};

use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::{UnixStream};

use crate::riverdb::common::{Result, Error};

pub(crate) enum TransportStream {
    TcpStream(TcpStream),
    #[cfg(unix)]
    UnixSocket(UnixStream),
}

impl TransportStream {
    pub fn new_tcp(stream: TcpStream) -> Self {
        TransportStream::TcpStream(stream)
    }

    #[cfg(unix)]
    pub fn new_unix(unix_socket: UnixStream) -> Self {
        TransportStream::UnixSocket(unix_socket)
    }

    pub fn is_unix(&self) -> bool {
        match self {
            TransportStream::UnixSocket(..) => true,
            _ => false,
        }
    }

    pub async fn readable(&self) -> Result<()> {
        match self {
            TransportStream::TcpStream(s) => s.readable().await.map_err(Error::from),
            #[cfg(unix)]
            TransportStream::UnixSocket(s) => unimplemented!(),
        }
    }

    pub async fn writable(&self) -> Result<()> {
        match self {
            TransportStream::TcpStream(s) => s.writable().await.map_err(Error::from),
            #[cfg(unix)]
            TransportStream::UnixSocket(s) => unimplemented!(),
        }
    }

    pub fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        match self {
            TransportStream::TcpStream(s) => s.try_read(buf).map_err(Error::from),
            #[cfg(unix)]
            TransportStream::UnixSocket(s) => unimplemented!(),
        }
    }

    pub fn try_write(&self, buf: &[u8]) -> Result<usize> {
        match self {
            TransportStream::TcpStream(s) => s.try_write(buf).map_err(Error::from),
            #[cfg(unix)]
            TransportStream::UnixSocket(s) => unimplemented!(),
        }
    }
}

pub(crate) struct TransportStreamReader<'a>(pub &'a TransportStream);

impl<'a> Read for TransportStreamReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.0 {
            TransportStream::TcpStream(s) => {
                let mut ss = unsafe {
                    std::net::TcpStream::from_raw_fd(s.as_raw_fd())
                };
                ss.read(buf)
            },
            #[cfg(unix)]
            TransportStream::UnixSocket(s) => unimplemented!(),
        }
    }
}

pub(crate) struct TransportStreamWriter<'a>(pub &'a TransportStream);

impl<'a> Write for TransportStreamWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.0 {
            TransportStream::TcpStream(s) => {
                let mut ss = unsafe {
                    std::net::TcpStream::from_raw_fd(s.as_raw_fd())
                };
                ss.write(buf)
            },
            #[cfg(unix)]
            TransportStream::UnixSocket(s) => unimplemented!(),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.0 {
            TransportStream::TcpStream(s) => {
                let mut ss = unsafe {
                    std::net::TcpStream::from_raw_fd(s.as_raw_fd())
                };
                ss.flush()
            },
            #[cfg(unix)]
            TransportStream::UnixSocket(s) => unimplemented!(),
        }
    }
}