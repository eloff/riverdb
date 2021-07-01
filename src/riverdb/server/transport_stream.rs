use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
use std::marker::PhantomData;

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
        std::mem::ManuallyDrop::new(std::mem::take(&mut self.stream));
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