use std::fmt;
use std::fmt::{Display, Formatter, Debug};
use std::mem::ManuallyDrop;

use bytes::{Bytes, Buf};

use crate::riverdb::Result;
use crate::riverdb::pg::protocol::{Tag, MessageReader, MessageErrorBuilder, ErrorSeverity};
use crate::riverdb::pg::protocol::message_parser::{Header, MIN_MESSAGE_LEN};
use crate::riverdb::common::unsplit_bytes;


#[derive(Clone)]
pub struct Message(Bytes);

impl Message {
    pub fn new(buf: Bytes) -> Self {
        let len = buf.len() as u32;
        assert!(len == 0 || len >= MIN_MESSAGE_LEN);
        Message(buf)
    }

    /// Return a new Message of type Tag::ERROR_RESPONSE with the given error code and error message
    pub fn new_error(error_code: &str, error_msg: &str) -> Self {
        let mut mb = MessageErrorBuilder::new(
            ErrorSeverity::Fatal,
            error_code,
            &error_msg
        );
        mb.finish()
    }

    /// Return a new Message of type Tag::NOTICE_RESPONSE with the given error code and error message
    pub fn new_warning(error_code: &str, error_msg: &str) -> Self {
        let mut mb = MessageErrorBuilder::new(
            ErrorSeverity::Warning,
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
    pub fn header(&self) -> Result<Header> {
        Ok(Header::parse(&self.0.chunk()[..5])?.expect("empty message"))
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

    /// Returns the number of complete protocol messages in this Message. 0 if empty.
    /// This method is O(N) where N is the number of messages.
    pub fn count(&self) -> usize {
        let mut count = 0;
        let mut pos = 0;
        while pos < self.0.len() {
            if let Ok(Some(hdr)) = Header::parse(&self.0.chunk()[pos..pos+5]) {
                count += 1;
                pos += hdr.len() as usize;
            } else {
                // This is an invalid message header, ignore everything from this point forward
                break;
            }
        }
        count
    }

    /// Returns true if there is more than one protocol message in this Message.
    pub fn has_multiple_messages(&self) -> bool {
        if !self.is_empty() {
            if let Ok(hdr) = self.header() {
                if hdr.len() < self.len() {
                    return true;
                }
            }
        }
        false
    }

    /// Returns an Iterator over the protocol messages in this Message
    pub fn iter(&self) -> MessageIter {
        MessageIter(self.clone())
    }

    /// If other follows directly after self in memory, this merges other into self and returns self.
    /// Otherwise returns both self and other unchanged.
    /// Safety: see note on unsplit_bytes for when this may be undefined beahvior.
    pub unsafe fn unsplit(self, other: Self) -> (Option<Self>, Option<Self>) {
        let (b1, b2) = unsplit_bytes(self.0, other.0);
        (b1.and_then(|b|Some(Self::new(b))), b2.and_then(|b| Some(Self::new(b))))
    }
}

impl Default for Message {
    fn default() -> Self {
        Self(Bytes::new())
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

pub struct MessageIter(Message);

impl Iterator for MessageIter {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.0.is_empty() {
            if let Ok(hdr) = self.0.header() {
                let len = hdr.len();
                let msg = std::mem::replace(&mut self.0, Message::default());
                return Some(if len == self.0.len() {
                    // It's the only Message, just return it
                    msg
                } else {
                    let mut bytes = msg.into_bytes();
                    let next = bytes.split_to(len as usize);
                    self.0 = Message(bytes);
                    Message(next)
                })
            }
        }
        None
    }
}
