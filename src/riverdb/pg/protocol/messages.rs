use std::fmt;
use std::fmt::{Display, Formatter, Debug, Write};
use std::mem::ManuallyDrop;

use bytes::{Bytes, Buf};
use tracing::{error};

use crate::riverdb::Result;
use crate::riverdb::pg::protocol::{Tag, Message, MessageReader, MessageErrorBuilder, ErrorSeverity};
use crate::riverdb::pg::protocol::message_parser::{Header, MIN_MESSAGE_LEN};
use crate::riverdb::common::unsplit_bytes;


#[derive(Clone)]
pub struct Messages(Bytes);

impl Messages {
    pub fn new(buf: Bytes) -> Self {
        let len = buf.len() as u32;
        assert!(len == 0 || len >= MIN_MESSAGE_LEN);
        Messages(buf)
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

    /// Returns true if Message was initialized with an empty buffer
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of all the messages in Messages
    pub fn len(&self) -> u32 {
        self.0.len() as u32
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.chunk()
    }

    pub fn bytes(&self) -> &Bytes {
        &self.0
    }

    /// into_bytes consumes Messages and returns the underlying Bytes buffer
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// Is returns true if other and msgs are the same because they share the same backing buffer
    /// or if is_empty() is true for both.
    pub fn is(&self, other: &Self) -> bool {
        self.0.as_ptr() == other.0.as_ptr()
    }

    /// Returns the number of complete protocol messages in this Message. 0 if empty.
    /// This method is O(N) where N is the number of messages.
    pub fn count(&self) -> usize {
        let mut count = 0;
        let mut pos = 0;
        while pos < self.0.len() {
            if let Ok(Some(hdr)) = Header::parse(&self.0.chunk()[pos..]) {
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
            if let Ok(Some(hdr)) = Header::parse(self.0.chunk()) {
                if hdr.len() < self.len() {
                    return true;
                }
            }
        }
        false
    }

    /// Returns an Iterator over the protocol messages in this Message
    pub fn iter(&self, start_offset: usize) -> MessageIter {
        MessageIter::new(&self.0, start_offset)
    }

    /// Returns the first Message in Messages
    pub fn first(&self) -> Option<Message> {
        self.iter(0).next()
    }

    /// Splits self after message end, so that self contains [offset, len)
    /// and the returned Messages contains [0, offset). Zero-copy, just adjusts
    /// offsets and reference counts.
    pub fn split_to(&mut self, offset: usize) -> Self {
        assert!(offset <= self.0.len());

        Self::new(self.0.split_to(offset))
    }

    /// Return just message as a new Messages object. Zero-copy.
    pub fn split_message<'a>(&'a self, message: &'a Message<'a>) -> Self {
        Self::new(self.0.slice_ref(message.as_slice()))
    }

    /// Returns one message starting at offset. Zero-copy.
    /// If offset is at the end of self, returns an empty Messages object.
    /// Panics if a valid message doesn't start at offset.
    pub fn split_message_at(&self, offset: usize) -> Self {
        let mut b = self.0.slice(offset..);
        if b.len() != 0 {
            let hdr = Header::parse(b.chunk()).expect("expected valid message").unwrap();
            b.truncate(hdr.len() as usize);
            Self::new(b)
        } else {
            Self::default()
        }
    }

    /// If other follows directly after self in memory, this merges other into self and returns self.
    /// Otherwise returns both self and other unchanged.
    /// Safety: see note on unsplit_bytes for when this may be undefined beahvior.
    pub unsafe fn unsplit(self, other: Self) -> (Option<Self>, Option<Self>) {
        let (b1, b2) = unsplit_bytes(self.0, other.0);
        (b1.and_then(|b|Some(Self::new(b))), b2.and_then(|b| Some(Self::new(b))))
    }
}

impl Default for Messages {
    fn default() -> Self {
        Self(Bytes::new())
    }
}

impl Display for Messages {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("Messages{")?;
        let mut first = true;
        for msg in self.iter(0) {
            if !first {
                f.write_str(", ")?;
            } else {
                first = false;
            }
            Display::fmt(&msg, f)?;
        }
        f.write_char('}')
    }
}

impl Debug for Messages {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self::Display::fmt(self, f)
    }
}

pub struct MessageIter<'a> {
    messages: &'a Bytes,
    pos: usize,
}

impl<'a> MessageIter<'a> {
    fn new(messages: &'a Bytes, start_offset: usize) -> Self {
        Self{messages, pos: start_offset}
    }
}

impl<'a> Iterator for MessageIter<'a> {
    type Item = Message<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.messages.is_empty() {
            return None;
        }

        let data = &self.messages.chunk()[self.pos..];
        match Header::parse(data) {
            Ok(Some(hdr)) => {
                let start = self.pos;
                let end = hdr.len() as usize;
                self.pos += end;
                Some(Message::new(hdr,&data[..end], start))
            },
            Ok(None) => None,
            Err(e) => {
                error!(%e, "error parsing PostgreSQL protocol message");
                None
            }
        }
    }
}
