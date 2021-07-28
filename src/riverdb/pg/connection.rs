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

use crate::riverdb::server;
use crate::riverdb::server::Transport;
use crate::riverdb::{Error, Result};
use crate::riverdb::common::bytes_to_slice_mut;
use crate::riverdb::pg::{BackendConn, ClientConn, ConnectionPool};


pub type Backlog = Mutex<VecDeque<Bytes>>;

pub trait Connection: server::Connection {
    fn has_backlog(&self) -> bool;
    fn set_has_backlog(&self, value: bool);
    fn backlog(&self) -> &Mutex<VecDeque<Bytes>>;
    fn transport(&self) -> &Transport;
    fn is_closed(&self) -> bool;

    fn is_tls(&self) -> bool {
        self.transport().is_tls()
    }

    /// write_or_buffer writes all the bytes in buf to sender without blocking or buffers it
    /// (without copying) to send later. Takes ownership of buf in all cases.
    fn write_or_buffer(&self, mut buf: Bytes) -> Result<()> {
        // We always have to acquire the mutex, even if the backlog appears empty, otherwise
        // we can't be certain another thread won't try to write the backlog and overlap write()
        // calls with us here. Essentially the backlog mutex must always be held when writing
        // so that the logical writes are atomic and ordered correctly.
        let mut backlog = self.backlog().lock().map_err(Error::from)?;
        if backlog.is_empty() {
            // If the backlog is empty, maybe we can write this to the socket
            let n = self.transport().try_write(buf.chunk())?;
            if n < buf.remaining() {
                buf.advance(n);
            } else {
                return Ok(());
            }
        }
        // Else we have data buffered pending because the socket is not ready for writing, add buf to the end.

        // TODO there is no unsplit on Bytes https://github.com/tokio-rs/bytes/issues/503
        // if let Some(last) = backlog.back() {
        //     // If the last buffer and this one are actually contiguous, then combine them instead of adding them separately.
        //     // MessageParser often produces a run of contiguous messages, and recombining them here will mean fewer syscalls to write().
        //
        // }

        backlog.push_back(buf);
        self.set_has_backlog(true);
        Ok(())
    }

    /// try_write_backlog tries to write some bytes from the backlog to the transport.
    /// Call when the underlying transport is ready for writing. Returns the number of bytes written.
    fn try_write_backlog(&self) -> Result<usize> {
        let mut write_bytes = 0;
        if !self.has_backlog() {
            return Ok(write_bytes);
        }

        let mut backlog = self.backlog().lock().map_err(Error::from)?;
        loop {
            // If !backend.is_tls() && backlog.len() > 1 we may want to use try_write_vectored
            // However, that's not worth the effort yet, and it should be completely pointless once we're
            // using io_uring through mio. I'm betting on the latter eventually making it unnecessary.
            if let Some(bytes) = backlog.front_mut() {
                let n = self.transport().try_write(bytes.chunk())?;
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
                self.set_has_backlog(false);
                break;
            }
        }
        Ok(write_bytes)
    }

    /// try_read attempts to read some bytes without blocking from transport into buf.
    /// appends to buf, does not overwrite existing data.
    fn try_read(&self, buf: &mut BytesMut) -> Result<usize> {
        let mut read_bytes = 0;
        let start = buf.len();
        // Safety: safe because we don't attempt to read from any possibly uninitialized bytes
        let bytes = unsafe { bytes_to_slice_mut(buf) };
        let mut n = self.transport().try_read(&mut bytes[start..])?;
        read_bytes += n;
        if n > 0 && n < bytes.len() {
            // If we read some data, but didn't fill buffer, reading again should return 0 (WouldBlock)
            // We don't have to try again here, it will happen anyway on the next call.
            // However, doing it here is more efficient as we skip all the code between invocations.
            // Reading until WouldBlock rearms the READABLE interest, so ready will block until more data arrives.
            n = self.transport().try_read(&mut bytes[n..])?;
            read_bytes += n;
        }
        // Safety: we only advance len by the amount of bytes that the OS read into buf
        unsafe { buf.set_len(buf.len() + read_bytes); }
        Ok(read_bytes)
    }
}


/// read_and_flush_backlog reads from transport and optionally flushes pending data for sender
/// these two steps are combined in a single task to reduce synchronization and scheduling overhead.
/// This is a free-standing function and not part of the Connection trait because traits don't
/// support async functions yet, and the async_trait crate boxes the returned future.
pub(crate) async fn read_and_flush_backlog<R: Connection, W: Connection>(
    connection: &R,
    buf: &mut BytesMut,
    sender: Option<&W>,
) -> Result<(usize, usize)> {
    if buf.remaining_mut() == 0 {
        return Ok((0, 0));
    }

    // Check if we need to write data to maybe_send_transport
    let mut interest = Interest::READABLE;
    let flush = sender.is_some() && sender.unwrap().has_backlog();
    if flush {
        interest.add(Interest::WRITABLE);
    } else if let Some(sender) = sender {
        // If sender.is_tls(), then it may have data buffered internally too
        if sender.transport().wants_write() {
            interest.add(Interest::WRITABLE);
        }
    }

    // Note that once something is ready, it stays ready (this method returns instantly)
    // until it's reset by encountering a WouldBlock error. From mio examples, this
    // seems to apply even if we've never attempted to read or write on the socket.
    let ready = if connection.transport().wants_read() {
        // We already have buffered plaintext data waiting on our TLS session, just read it
        Ready::READABLE
    } else {
        connection.transport().ready(interest).await.map_err(Error::from)?
    };

    let read_bytes = if ready.is_readable() {
        connection.try_read(buf)?
    } else {
        0
    };

    let write_bytes = if sender.is_some() && ready.is_writable() {
        sender.unwrap().try_write_backlog()?
    } else {
        0
    };

    return Ok((read_bytes, write_bytes))
}
