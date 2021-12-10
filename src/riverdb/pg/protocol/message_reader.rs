use std::convert::TryInto;

use crate::riverdb::pg::protocol::{Message};
use crate::riverdb::{Error, Result};


/// A reader for reading the content of a Postgres wire protocol message sequentially.
pub struct MessageReader<'a> {
    pub msg: &'a Message<'a>,
    pos: u32, // track position for read_xxx methods
    read_past_end: bool, // true if we tried to read past the end of the message
}

impl<'a> MessageReader<'a> {
    /// Create a new Reader borrowing from the passed Message
    pub fn new(msg: &'a Message<'a>) -> Self {
        MessageReader{
            msg,
            pos: msg.body_start() as u32,
            read_past_end: false,
        }
    }

    /// Create a new Reader from the passed Message at the given offset
    /// Same as r = new() followed by r.seek(pos).
    /// Panics if pos is out of range.
    pub fn new_at(msg: &'a Message<'a>, pos: u32) -> Self {
        assert!(pos <= msg.len());
        MessageReader{
            msg,
            pos,
            read_past_end: false,
        }
    }

    /// Return the length of the underlying message, see Message::len
    pub fn len(&self) -> u32 {
        self.msg.len()
    }

    /// Returns an Error if has_error() is true
    pub fn error(&self) -> Result<()> {
        if self.has_error() {
            Err(Error::protocol_error(format!("attempted to read past end of {:?}", self.msg)))
        } else {
            Ok(())
        }
    }

    /// Returns true if any of the read_* methods attempted to read beyond the end of the Message
    pub fn has_error(&self) -> bool {
        self.read_past_end
    }

    /// Peek at the next byte without changing the position. None if at end.
    pub fn peek(&self) -> Option<u8> {
        let pos = self.pos;
        self.msg.as_slice().get(pos as usize).cloned()
    }

    /// Reads a single byte and returns it.
    /// Returns 0 if no bytes left, use error() or has_error() to distinguish between that and an actual 0.
    pub fn read_byte(&mut self) -> u8 {
        let pos = self.pos;
        let new_pos = pos + 1;
        if new_pos > self.msg.len() {
            self.read_past_end = true;
            return 0;
        }

        // Safe because we just did the bounds check
        unsafe {
            let b = *self.msg.as_slice().get_unchecked(pos as usize);
            self.pos = new_pos;
            b
        }
    }

    /// Reads an i16 and returns it.
    /// Returns 0 if no bytes left, use error() or has_error() to distinguish between that and an actual 0.
    pub fn read_i16(&mut self) -> i16 {
        let pos = self.pos;
        let new_pos = pos + 2;
        if new_pos > self.msg.len() {
            self.read_past_end = true;
            return 0;
        }


        let bytes = &self.msg.as_slice()[pos as usize..new_pos as usize];
        self.pos = new_pos;
        i16::from_be_bytes(bytes.try_into().unwrap())
    }

    /// Reads an i32 and returns it.
    /// Returns 0 if no bytes left, use error() or has_error() to distinguish between that and an actual 0.
    pub fn read_i32(&mut self) -> i32 {
        let pos = self.pos;
        let new_pos = pos + 4;
        if new_pos > self.msg.len() {
            self.read_past_end = true;
            return 0;
        }

        let bytes = &self.msg.as_slice()[pos as usize..new_pos as usize];
        self.pos = new_pos;
        i32::from_be_bytes(bytes.try_into().unwrap())
    }

    /// Reads and returns a null-terminated utf-8 string
    pub fn read_str(&mut self) -> Result<&'a str> {
        let bytes = self.read_null_terminated_bytes()?;
        std::str::from_utf8(bytes).map_err(Error::from)
    }

    /// Reads and returns a null-terminated slice of bytes
    pub fn read_null_terminated_bytes(&mut self) -> Result<&'a [u8]> {
        let pos = self.pos;
        let bytes = &self.msg.as_slice()[pos as usize..];
        if let Some(i) = memchr::memchr(0, bytes) {
            self.pos = pos + i as u32 + 1;
            Ok(&bytes[..i])
        } else {
            self.read_past_end = true;
            Err(self.error().unwrap_err())
        }
    }

    /// Reads and returns a slice of bytes of the specified length
    pub fn read_bytes(&mut self, len: u32) -> Result<&'a [u8]> {
        let pos = self.pos;
        let new_pos = pos + len;
        self.seek(new_pos)?;

        let bytes = &self.msg.as_slice()[pos as usize..new_pos as usize];
        Ok(bytes)
    }

    /// Reads and returns the remainder of the message as a &[u8]
    pub fn read_to_end(&mut self) -> &'a [u8] {
        let end = self.len();
        let pos = self.pos;
        let bytes = &self.msg.as_slice()[pos as usize..end as usize];
        self.pos = end;
        bytes
    }

    /// Seek to pos, and returns the old position.
    /// Returns an error if out of range without changing the position.
    pub fn seek(&mut self, pos: u32) -> Result<u32> {
        if pos > self.len() {
            self.read_past_end = true;
            return Err(self.error().unwrap_err());
        }
        Ok(std::mem::replace(&mut self.pos, pos))
    }

    /// Return the current position.
    pub fn tell(&self) -> u32 {
        self.pos
    }

    /// Advances the current position by bytes.
    /// Same as seek(tell() + bytes).
    pub fn advance(&mut self, bytes: u32) -> Result<u32> {
        self.seek(self.tell() + bytes)
    }
}
