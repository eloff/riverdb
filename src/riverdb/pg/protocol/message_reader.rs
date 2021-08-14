use std::convert::TryInto;
use std::cell::Cell;
use std::mem::{swap, ManuallyDrop, transmute_copy};

use bytes::Bytes;

use crate::riverdb::pg::protocol::{Message, Tag};
use crate::riverdb::{Error, Result};
use std::ops::Deref;


pub struct MessageReader<'a> {
    pub msg: &'a Message<'a>,
    pos: Cell<u32>, // track position for read_xxx methods
    read_past_end: Cell<bool>, // true if we tried to read past the end of the message
}

impl<'a> MessageReader<'a> {
    pub fn new(msg: &'a Message<'a>) -> Self {
        MessageReader{
            msg,
            pos: Cell::new(msg.body_start() as u32),
            read_past_end: Cell::new(false),
        }
    }

    pub fn new_at(msg: &'a Message<'a>, pos: u32) -> Self {
        assert!(pos <= msg.len());
        MessageReader{
            msg,
            pos: Cell::new(pos),
            read_past_end: Cell::new(false),
        }
    }

    pub fn len(&self) -> u32 {
        self.msg.len()
    }

    /// error returns an Error if has_error() is true
    pub fn error(&self) -> Result<()> {
        if self.has_error() {
            Err(Error::protocol_error(format!("attempted to read past end of {:?}", self.msg)))
        } else {
            Ok(())
        }
    }

    /// has_error returns true if any of the read_* methods attempted to read beyond the end of the Message
    pub fn has_error(&self) -> bool {
        self.read_past_end.get()
    }

    pub fn peek(&self) -> Option<u8> {
        let pos = self.pos.get();
        self.msg.as_slice().get(pos as usize).cloned()
    }

    /// read_byte reads a single byte and returns it.
    /// Returns 0 if no bytes left, use error() or has_error() to distinguish between that and an actual 0.
    pub fn read_byte(&self) -> u8 {
        let pos = self.pos.get();
        let new_pos = pos + 1;
        if new_pos > self.msg.len() {
            self.read_past_end.set(true);
            return 0;
        }

        // Safe because we just did the bounds check
        unsafe {
            let b = *self.msg.as_slice().get_unchecked(pos as usize);
            self.pos.set(new_pos);
            b
        }
    }

    /// read_i16 reads an i16 and returns it.
    /// Returns 0 if no bytes left, use error() or has_error() to distinguish between that and an actual 0.
    pub fn read_i16(&self) -> i16 {
        let pos = self.pos.get();
        let new_pos = pos + 2;
        if new_pos > self.msg.len() {
            self.read_past_end.set(true);
            return 0;
        }


        let bytes = &self.msg.as_slice()[pos as usize..new_pos as usize];
        self.pos.set(new_pos);
        i16::from_be_bytes(bytes.try_into().unwrap())
    }

    /// read_i32 reads an i32 and returns it.
    /// Returns 0 if no bytes left, use error() or has_error() to distinguish between that and an actual 0.
    pub fn read_i32(&self) -> i32 {
        let pos = self.pos.get();
        let new_pos = pos + 4;
        if new_pos > self.msg.len() {
            self.read_past_end.set(true);
            return 0;
        }

        let bytes = &self.msg.as_slice()[pos as usize..new_pos as usize];
        self.pos.set(new_pos);
        i32::from_be_bytes(bytes.try_into().unwrap())
    }

    /// read_str reads and returns a null-terminated utf-8 string
    pub fn read_str(&self) -> Result<&'a str> {
        let bytes = self.read_null_terminated_bytes()?;
        std::str::from_utf8(bytes).map_err(Error::from)
    }

    /// read_null_terminated_bytes reads and returns a null-terminated slice of bytes
    pub fn read_null_terminated_bytes(&self) -> Result<&'a [u8]> {
        let pos = self.pos.get();
        let bytes = &self.msg.as_slice()[pos as usize..];
        if let Some(i) = memchr::memchr(0, bytes) {
            self.pos.set(pos + i as u32 + 1);
            Ok(&bytes[..i])
        } else {
            self.read_past_end.set(true);
            Err(self.error().unwrap_err())
        }
    }

    /// read_bytes reads and returns a slice of bytes of the specified length
    pub fn read_bytes(&self, len: u32) -> Result<&'a [u8]> {
        let pos = self.pos.get();
        let new_pos = pos + len;
        self.seek(new_pos)?;

        let bytes = &self.msg.as_slice()[pos as usize..new_pos as usize];
        self.pos.set(new_pos);
        Ok(bytes)
    }

    pub fn seek(&self, pos: u32) -> Result<u32> {
        if pos > self.len() {
            self.read_past_end.set(true);
            return Err(self.error().unwrap_err());
        }
        Ok(self.pos.replace(pos))
    }

    pub fn tell(&self) -> u32 {
        self.pos.get()
    }
}
