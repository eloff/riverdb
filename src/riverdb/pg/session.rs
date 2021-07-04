use std::sync::atomic::{AtomicU32, AtomicPtr, AtomicBool};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release};
use std::sync::{Arc, Mutex};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::collections::VecDeque;
use std::io::IoSlice;

use tokio::net::TcpStream;
use tokio::io::{Interest, Ready};
use bytes::{Bytes, BytesMut, BufMut, Buf};
use rustls::Connection;

use crate::riverdb::server::{Transport};
use crate::riverdb::common::{Result, Error};

pub enum SessionSide {
    Client,
    Backend
}

pub struct Session {
    /// client_stream is a possibly uninitialized Transport, may check if client_id != 0 first
    client_stream: UnsafeCell<MaybeUninit<Transport>>,
    /// client_id is set once and then read-only. If not used, it's 0.
    pub client_id: AtomicU32,
    /// backend_id is set once and then read-only. If not used, it's 0.
    pub backend_id: AtomicU32,
    pub client_has_send_backlog: AtomicBool,
    pub backend_has_send_backlog: AtomicBool,
    pub client_send_backlog: Mutex<VecDeque<Bytes>>,
    /// backend_stream is a possibly uninitialized Transport, may check if backend_id != 0 first
    backend_stream: UnsafeCell<MaybeUninit<Transport>>,
    pub backend_send_backlog: Mutex<VecDeque<Bytes>>,
    /// client_last-active is a course-grained monotonic clock that is advanced when data is received from the client
    pub client_last_active: AtomicU32,
    /// backend_last_active-active is a course-grained monotonic clock that is advanced when data is received from the backend
    pub backend_last_active: AtomicU32,
}

impl Session {
    pub fn new() -> Arc<Self> {
        Arc::new(Self{
            client_stream: UnsafeCell::new(MaybeUninit::uninit()),
            client_id: Default::default(),
            backend_id: Default::default(),
            client_has_send_backlog: Default::default(),
            backend_has_send_backlog: Default::default(),
            client_send_backlog: Mutex::new(Default::default()),
            backend_stream: UnsafeCell::new(MaybeUninit::uninit()),
            backend_send_backlog: Mutex::new(Default::default()),
            client_last_active: Default::default(),
            backend_last_active: Default::default()
        })
    }

    pub fn new_with_client(stream: Transport, conn_id: u32) -> Arc<Self> {
        let s = Self::new();
        unsafe {
            s.set_client(stream, conn_id);
        }
        s
    }

    pub fn new_with_backend(stream: Transport, conn_id: u32) -> Arc<Self> {
        let s = Self::new();
        unsafe {
            s.set_backend(stream, conn_id);
        }
        s
    }

    pub unsafe fn set_client(&self, stream: Transport, conn_id: u32) {
        assert_eq!(self.client_id.load(Relaxed), 0);
        *(&mut *self.client_stream.get()).as_mut_ptr() = stream;
        self.client_id.store(conn_id, Release);
    }

    pub unsafe fn set_backend(&self, stream: Transport, conn_id: u32) {
        assert_eq!(self.backend_id.load(Relaxed), 0);
        *(&mut *self.backend_stream.get()).as_mut_ptr() = stream;
        self.backend_id.store(conn_id, Release);
    }

    /// client unsafely returns a reference to the Transport for the client-facing connection.
    /// This is safe if you know it's been initialized, e.g. from ClientConn or ClientSend.
    pub unsafe fn client(&self) -> &Transport {
        &*(&*self.client_stream.get()).as_ptr()
    }

    /// backend unsafely returns a reference to the Transport for the backend-facing connection.
    /// This is safe if you know it's been initialized, e.g. from BackendConn or BackendSend.
    pub unsafe fn backend(&self) -> &Transport {
        &*(&*self.backend_stream.get()).as_ptr()
    }

    /// get_client returns Some(&Transport) if the client-facing connection has been initialized.
    pub fn get_client(&self) -> Option<&Transport> {
        if self.client_id.load(Acquire) != 0 {
            Some(unsafe { self.client() })
        } else {
            None
        }
    }

    /// get_backend returns Some(&Transport) if the client-facing connection has been initialized.
    pub fn get_backend(&self) -> Option<&Transport> {
        if self.backend_id.load(Acquire) != 0 {
            Some(unsafe { self.backend() })
        } else {
            None
        }
    }

    /// client_read_and_send_backlog reads from client() and appends the data to buf (not overwriting existing data)
    /// and optionally writes() pending data to backend(). It returns the number of bytes read and written,
    /// at least one of the return values will be non-zero if an no error occurred. If either underlying
    /// stream is closed (or half-closed) this will return Error::closed, and subsequent calls will return the same.
    pub async fn client_read_and_send_backlog(&self, buf: &mut BytesMut) -> Result<(usize, usize)> {
        read_and_flush_backlog(
            buf,
            unsafe { self.client() },
            &self.backend_send_backlog, 
            &self.backend_has_send_backlog,
            self.get_backend(),
        ).await
    }

    /// backend_read_and_send_backlog reads from backend() and appends the data to buf (not overwriting existing data)
    /// and optionally writes() pending data to client(). It returns the number of bytes read and written,
    /// at least one of the return values will be non-zero if an no error occurred. If either underlying
    /// stream is closed (or half-closed) this will return Error::closed, and subsequent calls will return the same.
    pub async fn backend_read_and_send_backlog(&self, buf: &mut BytesMut) -> Result<(usize, usize)> {
        read_and_flush_backlog(
            buf,
            unsafe { self.backend() },
            &self.client_send_backlog,
            &self.client_has_send_backlog,
            self.get_client(),
        ).await
    }

    // backend_send writes all the bytes in buf to backend() without blocking or buffers it
    // (without copying) to send later. Takes ownership of buf in all cases.
    pub fn backend_send(&self, buf: Bytes) -> Result<()> {
        backlog_send(buf, &self.backend_send_backlog, &self.backend_has_send_backlog, self.get_backend())
    }

    // client_send writes all the bytes in buf to client() without blocking or buffers it
    // (without copying) to send later. Takes ownership of buf in all cases.
    pub fn client_send(&self, buf: Bytes) -> Result<()> {
        backlog_send(buf, &self.client_send_backlog, &self.client_has_send_backlog, self.get_client())
    }
}

/// read_and_flush_backlog reads from transport and optionally flushes pending data from backlog to maybe_send_transport.
/// these two steps are combined in a single task to reduce synchronization and scheduling overhead.
async fn read_and_flush_backlog(
    buf: &mut BytesMut,
    transport: &Transport,
    backlog: &Mutex<VecDeque<Bytes>>,
    has_backlog: &AtomicBool,
    maybe_send_transport: Option<&Transport>
) -> Result<(usize, usize)> {
    if buf.remaining_mut() == 0 {
        return Ok((0, 0));
    }

    // Check if we need to write data to maybe_send_transport
    let mut interest = Interest::READABLE;
    let flush = maybe_send_transport.is_some() && has_backlog.load(Relaxed);
    if flush {
        interest.add(Interest::WRITABLE);
    } else if let Some(backend) = maybe_send_transport {
        // If backend.is_tls(), then it may have data buffered internally too
        if backend.wants_write() {
            interest.add(Interest::WRITABLE);
        }
    }

    // Note that once something is ready, it stays ready (this method returns instantly)
    // until it's reset by encountering a WouldBlock error. From mio examples, this
    // seems to apply even if we've never attempted to read or write on the socket.
    let ready = if transport.wants_read() {
        // We already have buffered plaintext data waiting on our TLS session, just read it
        Ready::READABLE
    } else {
        transport.ready(interest).await.map_err(Error::from)?
    };

    let read_bytes = if ready.is_readable() {
        try_read(buf, transport)?
    } else {
        0
    };

    let write_bytes = if ready.is_writable() {
        let backend = maybe_send_transport.unwrap();
        flush_backlog(backlog, has_backlog, backend)?
    } else {
        0
    };

    return Ok((read_bytes, write_bytes))
}

fn backlog_send(mut buf: Bytes, backlog: &Mutex<VecDeque<Bytes>>, has_backlog: &AtomicBool, transport: Option<&Transport>) -> Result<()> {
    // We always have to acquire the mutex, otherwise, even if the backlog appears empty,
    // we can't be certain another thread won't try to write the backlog and overlap write()
    // calls with us here. Essentially the backlog mutex must always be held when writing
    // so that the logical writes are atomic and ordered correctly.
    let mut backlog = backlog.lock().map_err(Error::from)?;
    if backlog.is_empty() {
        if let Some(s) = transport {
            // If the backlog is empty, maybe we can write this to the socket
            let n = s.try_write(buf.chunk())?;
            if n < buf.remaining() {
                buf.advance(n);
            } else {
                return Ok(());
            }
        }
    }
    backlog.push_back(buf);
    // Relaxed because the mutex release below is a global barrier
    has_backlog.store(true, Relaxed);
    Ok(())
}

fn flush_backlog(backlog: &Mutex<VecDeque<Bytes>>, has_backlog: &AtomicBool, transport: &Transport) -> Result<usize> {
    let mut write_bytes = 0;
    let mut backlog = backlog.lock().map_err(Error::from)?;
    loop {
        // If !backend.is_tls() && backlog.len() > 1 we may want to use try_write_vectored
        // However, that's not worth the effort yet, and it should be completely pointless once we're
        // using io_uring through mio. I'm betting on the latter eventually making it unnecessary.
        if let Some(bytes) = backlog.front_mut() {
            let n = transport.try_write(bytes.chunk())?;
            write_bytes += n;
            if n == 0 {
                break;
            } else if n < bytes.remaining() {
                bytes.advance(n);
            } else {
                // n == bytes.remaining()
                backlog.pop_front();
            }
        } else {
            // Relaxed because the mutex release below is a global barrier
            has_backlog.store(false, Relaxed);
            break;
        }
    }
    Ok(write_bytes)
}

/// try_read attempts to read some bytes without blocking from transport into buf.
/// appends to buf, does not overwrite existing data.
fn try_read(buf: &mut BytesMut, transport: &Transport) -> Result<usize> {
    let mut read_bytes = 0;
    let maybe_uninit = buf.chunk_mut();
    let bytes = unsafe {
        std::slice::from_raw_parts_mut(maybe_uninit.as_mut_ptr(), maybe_uninit.len())
    };
    let mut n = transport.try_read(&mut bytes[buf.len()..])?;
    read_bytes += n;
    if n > 0 && n < bytes.len() {
        // If we read some data, but didn't fill buffer, reading again should return 0 (WouldBlock)
        // We don't have to try again here, it will happen anyway on the next call.
        // However, doing it here is more efficient as we skip all the code between invocations.
        // Reading until WouldBlock rearms the READABLE interest, so ready will block until more data arrives.
        n = transport.try_read(&mut bytes[n..])?;
        read_bytes += n;
    }
    unsafe { buf.set_len(buf.len() + read_bytes); }
    Ok(read_bytes)
}

