use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{Relaxed};
use std::sync::{Mutex, MutexGuard};
use std::collections::VecDeque;

use tokio::io::{Interest, Ready};
use bytes::{Bytes, BytesMut, Buf};
use tracing::debug;

use crate::riverdb::server;
use crate::riverdb::server::Transport;
use crate::riverdb::{Error, Result};
use crate::riverdb::common::{bytes_to_slice_mut, unsplit_bytes, bytes_are_contiguous};
use crate::riverdb::pg::protocol::{Tag, Messages, MessageParser};

pub type Backlog = Mutex<VecDeque<Bytes>>;

pub struct RefcountAndFlags(AtomicU8);

impl RefcountAndFlags {
    pub const HAS_BACKLOG: u8 = 128;
    const REFCOUNT_MASK: u8 = 0x3f; // max of 64

    pub const fn new() -> Self {
        Self(AtomicU8::new(1))
    }

    pub fn refcount(&self) -> u32 {
        (self.0.load(Relaxed) & Self::REFCOUNT_MASK) as u32
    }

    pub fn incref(&self) {
        let before = self.0.fetch_add(1, Relaxed);
        assert!((before & Self::REFCOUNT_MASK) < Self::REFCOUNT_MASK);
    }

    pub fn decref(&self) -> bool {
        let before = self.0.fetch_sub(1, Relaxed);
        let prev_count = before & Self::REFCOUNT_MASK;
        assert!(prev_count > 0);
        prev_count == 1
    }

    pub fn has(&self, flag: u8) -> bool {
        (self.0.load(Relaxed) & flag) != 0
    }

    pub fn set(&self, flag: u8, value: bool) {
        if value {
            self.0.fetch_or(flag, Relaxed);
        } else {
            self.0.fetch_and(!flag, Relaxed);
        }
    }
}

pub trait Connection: server::Connection {
    /// Returns true if the backlog is non-empty.
    fn has_backlog(&self) -> bool;
    /// Set if the backlog is empty or not.
    fn set_has_backlog(&self, value: bool);
    /// Returns a reference to the backlog, wrapped in a Mutex.
    fn backlog(&self) -> &Mutex<VecDeque<Bytes>>;
    /// Returns a reference to the underlying Transport.
    fn transport(&self) -> &Transport;
    fn is_closed(&self) -> bool;
    /// Returns Ok(()) if the Message Tag may be received during the
    /// current state of the session, otherwise an error.
    fn msg_is_allowed(&self, tag: Tag) -> Result<()>;
    /// Returns true if this connection is using TLS (SSL).
    fn is_tls(&self) -> bool {
        self.transport().is_tls()
    }

    /// Writes all the bytes in buf to sender without blocking or buffers it
    /// (without copying) to send later. Takes ownership of buf in all cases.
    /// Returns the number of bytes actually written (not buffered.)
    fn write_or_buffer(&self, mut buf: Bytes) -> Result<usize> {
        // We always have to acquire the mutex, even if the backlog appears empty, otherwise
        // we can't be certain another thread won't try to write the backlog and overlap write()
        // calls with us here. Essentially the backlog mutex must always be held when writing
        // so that the logical writes are atomic and ordered correctly.
        let mut bytes_written = 0;
        let mut backlog = self.backlog().lock().map_err(Error::from)?;
        // If the backlog is empty try writing buf directly
        if backlog.is_empty() {
            // If the backlog is empty, maybe we can write this to the socket
            bytes_written = self.transport().try_write(buf.chunk())?;
            if bytes_written < buf.remaining() {
                buf.advance(bytes_written);
            } else {
                return Ok(bytes_written);
            }
        }
        // Else we have data buffered pending because the socket is not ready for writing, add buf to the end.

        // MessageParser often produces a run of contiguous messages, and recombining them here will mean fewer syscalls to write().
        if !backlog.is_empty() && bytes_are_contiguous(&buf, backlog.back().unwrap()) {
            // Safety: If buf and back() are contiguous we know they were allocated from the same buffer
            // At any rate, we just pass the buffer straight to the kernel. There's no way to miscompile this.
            // The kernel neither knows nor cares about Rust's concept of pointer provenance.
            let (merged, failed) = unsafe {
                unsplit_bytes(buf, backlog.pop_back().unwrap())
            };
            backlog.push_back(merged.unwrap());
            if let Some(other) = failed {
                backlog.push_back(other);
            }
        } else {
            backlog.push_back(buf);
        }
        self.set_has_backlog(true);

        Ok(bytes_written)
    }

    /// Tries to write some bytes from the backlog to the transport.
    /// Call when the underlying transport is ready for writing. Returns the number of bytes written.
    fn try_write_backlog(&self) -> Result<usize> {
        if !self.has_backlog() {
            return Ok(0);
        }

        let backlog = self.backlog().lock().map_err(Error::from)?;
        self.write_backlog(backlog)
    }

    /// With the given locked backlog, write as much data from it to the connection as possible.
    fn write_backlog(&self, mut backlog: MutexGuard<VecDeque<Bytes>>) -> Result<usize> {
        let mut write_bytes = 0;
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

    /// Attempts to read some bytes without blocking from transport into buf.
    /// appends to buf, does not overwrite existing data.
    fn try_read(&self, buf: &mut BytesMut) -> Result<usize> {
        let start = buf.len();
        // Safety: safe because we don't attempt to read from any possibly uninitialized bytes
        let bytes = unsafe { bytes_to_slice_mut(buf) };
        let read_bytes = self.transport().try_read(&mut bytes[start..])?;
        // Safety: we only advance len by the amount of bytes that the OS read into buf
        unsafe { buf.set_len(buf.len() + read_bytes); }
        Ok(read_bytes)
    }
}

/// Reads from transport and optionally flushes pending data for sender
/// these two steps are combined in a single task to reduce synchronization and scheduling overhead.
/// This is a free-standing function and not part of the Connection trait because traits don't
/// support async functions yet, and the async_trait crate boxes the returned future.
pub(crate) async fn read_and_flush_backlog<R: Connection, W: Connection>(
    connection: &R,
    buf: &mut BytesMut,
    sender: Option<&W>,
) -> Result<(usize, usize)> {
    if buf.capacity() == buf.len() {
        return Ok((0, 0));
    }

    // Check if we need to write data to maybe_send_transport
    let interest = Interest::READABLE;
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

/// Using the given MessageParser to accumulate and parse messages, reads bytes from receiver,
/// writes any pending backlog data to sender (if not None) and returns the parsed Messages.
/// Reads at least one Message, or returns an Error.
pub async fn parse_messages<R: Connection, W: Connection>(parser: &mut MessageParser, receiver: &R, sender: Option<&W>, first_only: bool) -> Result<Messages> {
    loop {
        read_and_flush_backlog(
            receiver,
            parser.bytes_mut(),
            sender,
        ).await?;

        loop {
            if let Some(result) = parser.next(first_only) {
                let msgs = result?;
                debug!(msgs=?&msgs, sender=?receiver, "received messages");

                return Ok(msgs);
            } else {
                // We can keep reading cheaper than calling read_and_flush_backlog again
                // Until try_read returns EWOULDBLOCK, which is Ok(0) in this case.
                // Because the docs for ready() used inside read_and_flush_backlog say:
                //   Once a readiness event occurs, the method will continue to return
                //   immediately until the readiness event is consumed by an attempt to
                //   read or write that fails with WouldBlock.
                let bytes_read = receiver.try_read(parser.bytes_mut())?;
                if bytes_read == 0 {
                    break;
                }
            }
        }
    }
}
