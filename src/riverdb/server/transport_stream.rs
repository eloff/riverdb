use std::io;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::marker::PhantomData;

use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::{UnixStream};
use tokio::io::{Interest, Ready};

use crate::riverdb::{Error, Result};


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

    pub async fn ready(&self, interest: Interest) -> Result<Ready> {
        match self {
            TransportStream::TcpStream(s) => s.ready(interest).await.map_err(Error::from),
            #[cfg(unix)]
            TransportStream::UnixSocket(s) => s.ready(interest).await.map_err(Error::from),
        }
    }

    pub fn try_read(&self, buf: &mut [u8]) -> Result<usize> {
        convert_io_result(match self {
            TransportStream::TcpStream(s) => s.try_read(buf),
            #[cfg(unix)]
            TransportStream::UnixSocket(_s) => unimplemented!(),
        })
    }

    pub fn try_write(&self, buf: &[u8]) -> Result<usize> {
        convert_io_result(match self {
            TransportStream::TcpStream(s) => s.try_write(buf),
            #[cfg(unix)]
            TransportStream::UnixSocket(_s) => unimplemented!(),
        })
    }

    pub fn close(&self) {
        let raw_fd = match self {
            TransportStream::TcpStream(s) => s.as_raw_fd(),
            #[cfg(unix)]
            TransportStream::UnixSocket(s) => s.as_raw_fd(),
        };
        unsafe {
            libc::close(raw_fd);
        }
    }
}

/// convert_result converts an io::Result from read/write to a Result
/// where WouldBlock errors are converted to Ok(0) and Ok(0) is converted to Error::closed().
pub(crate) fn convert_io_result(result: io::Result<usize>) -> Result<usize> {
    match result {
        Err(e) => {
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(0);
            }
            Err(Error::from(e))
        },
        Ok(0) => {
            Err(Error::closed()) // EOF
        },
        Ok(n) => Ok(n),
    }
}

enum Stream {
    Empty,
    Tcp(std::net::TcpStream),
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
}

impl Default for Stream {
    fn default() -> Self {
        Stream::Empty
    }
}

pub(crate) struct StreamReaderWriter<'a>{
    stream: Stream,
    _phantom: PhantomData<&'a TransportStream>
}

impl<'a> StreamReaderWriter<'a> {
    pub fn new(transport: &'a TransportStream) -> Self {
        StreamReaderWriter {
            stream: match transport {
                TransportStream::TcpStream(s) => unsafe {
                    Stream::Tcp(std::net::TcpStream::from_raw_fd(s.as_raw_fd()))
                },
                TransportStream::UnixSocket(s) => unsafe {
                    Stream::Unix(std::os::unix::net::UnixStream::from_raw_fd(s.as_raw_fd()))
                },
            },
            _phantom: PhantomData,
        }
    }
}

impl<'a> Drop for StreamReaderWriter<'a> {
    /// drop here just moves the underlying socket out so dropping it does not close it
    fn drop(&mut self) {
        std::mem::forget(std::mem::take(&mut self.stream));
    }
}

impl<'a> Read for StreamReaderWriter<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match &mut self.stream {
            Stream::Tcp(s) => s.read(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.read(buf),
            _ => unreachable!(),
        }
    }
}

impl<'a> Write for StreamReaderWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match &mut self.stream {
            Stream::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            Stream::Unix(s) => s.write(buf),
            _ => unreachable!(),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.stream {
            Stream::Tcp(s) => s.flush(),
            #[cfg(unix)]
            Stream::Unix(s) => s.flush(),
            _ => unreachable!(),
        }
    }
}