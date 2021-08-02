use std::fmt;
use std::fmt::{Display, Formatter, Debug};
use std::mem::ManuallyDrop;

use bytes::{Bytes, Buf};

use crate::riverdb::Result;
use crate::riverdb::pg::protocol::{Tag, MessageReader, MessageErrorBuilder, ErrorSeverity};
use crate::riverdb::pg::protocol::message_parser::Header;
use crate::riverdb::common::unsplit_bytes;


#[derive(Clone)]
pub struct Message(Bytes);

impl Message {
    pub fn new(buf: Bytes) -> Self {
        Message(buf)
    }

    pub fn new_error(error_code: &str, error_msg: &str) -> Self {
        let mut mb = MessageErrorBuilder::new(
            ErrorSeverity::Fatal,
            error_code,
            &error_msg
        );
        mb.finish()
    }

    /// tag returns the message Tag or panics if self.is_empty()
    /// it does not validate if the tag byte is a know Postgres message tag
    /// which depends not just on if the tag byte is one of the predefined set,
    /// but if it's appearing at the correct order in the message flow.
    /// This allows for extensions to the protocol.
    pub fn tag(&self) -> Tag {
        Tag::new_unchecked(*self.0.get(0).expect("empty Message"))
    }

    /// is_empty returns true if Message was initialized with an empty buffer
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// len returns the length of all the messages in Message including framing
    pub fn len(&self) -> u32 {
        self.0.len() as u32
    }

    /// Returns the offset to the start of the message body
    pub fn body_start(&self) -> u32 {
        if self.tag() == Tag::UNTAGGED { 4 } else { 5 }
    }

    /// header returns the message Header of the first message or panics if self.is_empty()
    pub fn header(&self) -> Header {
        Header::parse(&self.0.chunk()[..5])
            .expect("invalid Message")
            .expect("empty Message")
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.chunk()
    }

    pub fn bytes(&self) -> &Bytes {
        &self.0
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

    /// If there is another message in the buffer, returns a new Message object starting
    /// at the next message. This increments the reference count of the underlying buffer,
    /// but does not copy data. Returns None if there isn't another message.
    pub fn next(&self) -> Option<Message> {
        // Note: it's tempting to try to implement Iterator with a wrapper struct
        // but that's not possible. It's really a streaming iterator and not compatible
        // without fully materializing the iterator as a Vec<Message> (at which point
        // it would be more useful to just have a method that returns a Vec<Message>.)
        // See: https://stackoverflow.com/a/30422716/152580
        if self.is_empty() {
            return None;
        }

        let len = self.header().len() as usize;
        Some(Message::new(self.0.slice(len..)))
    }

    /// If other follows directly after self in memory, this merges other into self and returns self.
    /// Otherwise returns both self and other unchanged.
    /// Safety: see note on unsplit_bytes for when this may be undefined beahvior.
    pub unsafe fn unsplit(self, other: Self) -> (Option<Self>, Option<Self>) {
        let (b1, b2) = unsplit_bytes(self.0, other.0);
        (b1.and_then(|b|Some(Self::new(b))), b2.and_then(|b| Some(Self::new(b))))
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