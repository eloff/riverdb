use std::fmt;
use std::fmt::{Display, Formatter, Debug};

use bytes::{Bytes, Buf};

use crate::riverdb::pg::protocol::Tag;
use crate::riverdb::pg::protocol::message_parser::Header;


pub struct Message(Bytes);

impl Message {
    pub fn new(buf: Bytes) -> Self {
        Message(buf)
    }

    /// tag returns the message Tag or panics if self.is_empty()
    pub fn tag(&self) -> Tag {
        Tag::new_unchecked(*self.0.get(0).expect("empty Message") as char)
    }

    /// is_empty returns true if Message was initialized with an empty buffer
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// len returns the length of the Message including optional tag byte and length frame
    pub fn len(&self) -> u32 {
        self.0.len() as u32
    }

    /// header returns the message Header or panics if self.is_empty()
    pub fn header(&self) -> Header {
        Header::parse(&self.0.chunk()[..5])
            .expect("invalid Message")
            .expect("empty Message")
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.chunk()
    }

    /// into_bytes consumes Message and returns the underlying Bytes buffer
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// Is returns true if other and msg are the same because they share the same backing buffer
    /// or if is_empty() is true for both.
    pub fn is(&self, other: &Message) -> bool {
        self.0.as_ptr() == other.0.as_ptr()
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            f.write_str("Empty")
        } else {
            f.write_fmt(format_args!("{}", self.tag()))
        }
    }
}

impl Debug for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self::Display::fmt(self, f)?;
        f.write_str(" Message")
    }
}