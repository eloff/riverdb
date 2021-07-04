use bytes::{Bytes, Buf};

use crate::riverdb::pg::protocol::Tag;
use crate::riverdb::pg::protocol::parser::Header;

pub struct Message {
    data: Bytes, // start of underlying buffer
    pos: u32, // track position for read_xxx methods
    read_past_end: bool, // true if we tried to read past the end of the message
}

impl Message {
    pub fn new(buf: Bytes) -> Self {
        Message{
            data: buf,
            pos: 0,
            read_past_end: false,
        }
    }

    /// tag returns the message Tag or panics if self.is_empty()
    pub fn tag(&self) -> Tag {
        Tag::new_unchecked(*self.data.get(0).expect("empty Message") as char)
    }

    /// is_empty returns true if Message was initialized with an empty buffer
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// len returns the length of the Message including optional tag byte and length frame
    pub fn len(&self) -> u32 {
        self.data.len() as u32
    }

    /// header returns the message Header or panics if self.is_empty()
    pub fn header(&self) -> Header {
        Header::parse(&self.data.chunk()[..5])
            .expect("invalid Message")
            .expect("empty Message")
    }

    /// into_bytes consumes Message and returns the underlying Bytes buffer
    pub fn into_bytes(self) -> Bytes {
        self.data
    }
}