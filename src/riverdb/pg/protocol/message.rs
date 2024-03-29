use std::fmt;
use std::fmt::{Display, Formatter, Debug};

use crate::riverdb::pg::protocol::{Tag, MessageReader};
use crate::riverdb::pg::protocol::message_parser::{Header};


/// Message represents a single PostgreSQL wire protocol message
/// It's borrowed from an owning Messages buffer which contains one or more messages.
pub struct Message<'a> {
    header: Header,
    data: &'a [u8],
    offset: usize,
}

impl<'a> Message<'a> {
    /// Create a new Message with decoded header and data starting with header at offset.
    pub fn new(header: Header, data: &'a [u8], offset: usize) -> Self {
        Message{
            header,
            data,
            offset,
        }
    }

    /// Return the Header object for this message (tag byte and length)
    pub fn header(&self) -> Header {
        self.header
    }

    /// Returns offset of this message in the parent Messages buffer
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Returns the message Tag, it does not validate if the tag byte is a know Postgres message tag
    pub fn tag(&self) -> Tag {
        self.header.tag
    }

    /// Returns the length of the message including tag and framing
    pub fn len(&self) -> u32 {
        self.header.len() as u32
    }

    pub fn body_start(&self) -> usize {
        if self.tag() == Tag::UNTAGGED { 4 } else { 5 }
    }

    /// Returns the full message including tag, length, and body.
    pub fn as_slice(&self) -> &[u8] {
        self.data
    }

    /// Returns body data of the message as a slice of bytes
    pub fn body(&self) -> &[u8] {
        &self.data[self.body_start()..]
    }

    /// Returns a MessageReader into this Message
    pub fn reader(&'a self) -> MessageReader<'a> {
        MessageReader::new(self)
    }

    /// Is returns true if other and msg are the same because they share the same backing buffer
    /// or if is_empty() is true for both.
    pub fn is(&self, other: &Message<'_>) -> bool {
        self.data.as_ptr() == other.data.as_ptr()
    }
}

impl<'a> Display for Message<'a> {
    /// Format message as "$Type Message"
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{} Message", self.tag()))
    }
}

impl<'a> Debug for Message<'a> {
    /// Format message as "$Type Message"
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self::Display::fmt(self, f)
    }
}
