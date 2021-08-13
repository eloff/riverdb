use bytes::{BytesMut, Buf};
use std::num::NonZeroU32;
use std::convert::TryInto;

use rustls::Connection;

use crate::riverdb::{Error, Result};
use crate::riverdb::pg::protocol::Messages;
use crate::riverdb::config::conf;
use crate::riverdb::pg::protocol::Tag;

pub const MIN_MESSAGE_LEN: u32 = 5;

#[derive(Copy, Clone)]
pub struct Header {
    pub tag: Tag,
    pub length: NonZeroU32,
}

impl Header {
    /// Returns the parsed frame Header if successful.
    /// If there wasn't enough data for a frame header, it returns Ok(None).
    /// Else if the frame header was invalid, it returns an error.
    pub fn parse(bytes: &[u8]) -> Result<Option<Self>> {
        if (bytes.len() as u32) < MIN_MESSAGE_LEN {
            return Ok(None);
        }
        let tag = Tag::new(bytes[0])?;
        let start = if tag != Tag::UNTAGGED { 1 } else { 0 };
        let len = u32::from_be_bytes((&bytes[start..start+4]).try_into().unwrap());
        if len < 4 {
            return Err(Error::protocol_error("length of message frame cannot be < 4"));
        }
        Ok(Some(Header{
            tag,
            // Safety: we already checked len != 0 above
            length: unsafe { NonZeroU32::new_unchecked(len) },
        }))
    }

    /// Returns the length of the message frame, including the tag byte (if any)
    pub fn len(&self) -> u32 {
        let len = self.length.get();
        if self.tag == Tag::UNTAGGED {
            len
        } else {
            len + 1
        }
    }
}

pub struct MessageParser {
    data: BytesMut,
}

impl MessageParser {
    pub fn new() -> Self {
        Self {
            data: BytesMut::with_capacity(conf().recv_buffer_size as usize),
        }
    }

    /// Returns the next byte in the buffer (or None) if empty. Does not advance the read position.
    pub fn peek(&mut self) -> Option<u8> {
        self.data.first().cloned()
    }

    /// Returns the next byte in the buffer (or None) if empty and advances the read position.
    pub fn next_byte(&mut self) -> Option<u8> {
        let b = self.peek();
        self.data.advance(1);
        b
    }

    /// Parses and returns the next Message in the buffer without copying,
    /// or None if there isn't a complete message.
    pub fn next(&mut self) -> Option<Result<Messages>> {
        let mut pos = 0;
        let mut reserve_extra = 0;
        let data = self.data.chunk();
        loop {
            match Header::parse(&data[pos..]) {
                Err(e) => { return Some(Err(e)) },
                Ok(None) => { break; },
                Ok(Some(hdr)) => {
                    let msg_end = pos + hdr.len() as usize;
                    if msg_end <= self.data.len() {
                        // We have the full message. Start after this message and loop again.
                        pos = msg_end;
                        continue;
                    } else {
                        // We don't have this last message, make sure buffer is large enough for it
                        reserve_extra = msg_end - self.data.len();
                        break;
                    }
                }
            }
        }

        let result = if pos != 0 {
            let msg = Messages::new(self.data.split_to(pos as usize).freeze());
            Some(Ok(msg))
        } else {
            None
        };

        // Doing this after splitting off the parsed data lets reserve
        // allocate a new buffer without copying as much existing data.
        if reserve_extra != 0 {
            self.data.reserve(reserve_extra);
        }

        result
    }

    /// Returns a mutable reference to the underlying BytesMut buffer.
    pub fn bytes_mut(&mut self) -> &mut BytesMut {
        &mut self.data
    }
}