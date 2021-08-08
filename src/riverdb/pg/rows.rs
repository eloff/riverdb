use std::sync::Arc;
use std::convert::TryInto;

use tracing::{warn};
use tokio::sync::Notify;

use crate::riverdb::{Error, Result};
use crate::riverdb::pg::{BackendConn, MessageQueue};
use crate::riverdb::pg::protocol::{Message, MessageReader, Tag, RowDescription, PostgresError};
use crate::riverdb::common::change_lifetime;


const FIELD_INDEX_OUT_OF_RANGE: &str = "field index out of range";

pub struct Rows<'a> {
    message_queue: Option<&'a MessageQueue>,
    notifier: Option<&'a Notify>,
    fields: RowDescription,
    cur: Message, // the current message being processed
    raw: Vec<&'static [u8]>, // these point into cur, they're not static
    affected: i32,
}

impl<'a> Rows<'a> {
    pub fn new(message_queue: &'a MessageQueue, notifier: &'a Notify) -> Self {
        Self{
            message_queue: Some(message_queue),
            notifier: Some(notifier),
            fields: RowDescription::default(),
            cur: Message::default(),
            raw: Vec::new(),
            affected: -1,
        }
    }

    /// Returns the number of affected rows. Can only be called once next() returns false.
    /// Panics if next() has not returned false.
    pub fn affected(&self) -> i32 {
        assert!(self.affected >= 0);
        self.affected
    }

    pub fn fields(&self) -> &RowDescription { &self.fields }

    pub fn message(&self) -> &Message {
        &self.cur
    }

    pub fn take_message(&mut self) -> Message {
        self.raw.clear();
        std::mem::replace(&mut self.cur, Message::default())
    }

    pub fn get_raw(&self) -> &[&[u8]] {
        // Safety: change the fake 'static lifetime in raw to the real borrowed from self lifetime
        // because next() takes a &mut self, it can't be called until this returned shared
        // reference is out of use. We ensure the references don't outlive the buffer in msg.
        unsafe { change_lifetime(self.raw.as_slice()) }
    }

    #[inline]
    pub fn get_bytes(&self, i: usize) -> Result<&[u8]> {
        self.raw.get(i).cloned().ok_or_else(|| Error::new(FIELD_INDEX_OUT_OF_RANGE))
    }

    pub fn get_str(&self, i: usize) -> Result<&str> {
        std::str::from_utf8(self.get_bytes(i)?).map_err(Error::from)
    }

    fn get_byte_array<const SIZE: usize>(&self, i: usize) -> Result<Option<[u8; SIZE]>> {
        let bytes = self.get_bytes(i)?;
        if bytes.len() < SIZE {
            if bytes.len() == 0 {
                Ok(None)
            } else {
                let mut result: [u8; SIZE] = [0; SIZE];
                result.clone_from_slice(bytes);
                Ok(Some(result))
            }
        } else {
            Ok(Some((&bytes[bytes.len()-SIZE..]).try_into().unwrap()))
        }
    }

    pub fn get_i16(&self, i: usize) -> Result<Option<i16>> {
        match self.get_byte_array::<2>(i)? {
            None => Ok(None),
            Some(a) => Ok(Some(i16::from_be_bytes(a))),
        }
    }

    pub fn get_i32(&self, i: usize) -> Result<Option<i32>> {
        match self.get_byte_array::<4>(i)? {
            None => Ok(None),
            Some(a) => Ok(Some(i32::from_be_bytes(a))),
        }
    }

    pub fn get_i64(&self, i: usize) -> Result<Option<i64>> {
        match self.get_byte_array::<8>(i)? {
            None => Ok(None),
            Some(a) => Ok(Some(i64::from_be_bytes(a))),
        }
    }

    pub fn get_f32(&self, i: usize) -> Result<Option<f32>> {
        match self.get_byte_array::<4>(i)? {
            None => Ok(None),
            Some(a) => Ok(Some(f32::from_be_bytes(a))),
        }
    }

    pub fn get_f64(&self, i: usize) -> Result<Option<f64>> {
        match self.get_byte_array::<8>(i)? {
            None => Ok(None),
            Some(a) => Ok(Some(f64::from_be_bytes(a))),
        }
    }

    pub async fn finish(&mut self) -> Result<i32> {
        if self.affected >= 0 {
            return Ok(self.affected);
        }

        if let Some(notify) = self.notifier {
            // Wait for our turn with the message queue
            notify.notified().await;
            self.notifier = None;
        }

        self.raw = Vec::new();
        self.cur = Message::default();

        loop {
            let msg = self.message_queue.unwrap().pop().await;
            match msg.tag() {
                Tag::COMMAND_COMPLETE => {
                    let r = MessageReader::new(&msg);
                    // For all command tags that have a row count, it's the last part of the tag after a space
                    let cmd_tag = r.read_str()?;
                    if let Some(i) = cmd_tag.rfind(' ') {
                        self.affected = (&cmd_tag[i+1..]).parse::<i32>().unwrap_or(0);
                    } else {
                        self.affected = 0;
                    }
                    self.message_queue = None;
                    return Ok(self.affected);
                },
                Tag::ERROR_RESPONSE => {
                    let e = PostgresError::new(msg)?;
                    return Err(Error::from(e));
                },
                Tag::NOTICE_RESPONSE => {
                    let e = PostgresError::new(msg)?;
                    warn!(%e, "notice received while iterating over result in Rows");
                },
                _ => (),
            }
        }
    }

    pub async fn next(&mut self) -> Result<bool> {
        if let Some(notify) = self.notifier {
            // Wait for our turn with the message queue
            notify.notified().await;
            self.notifier = None;
        }

        if self.message_queue.is_none() {
            // Already iterated to completion
            return Ok(false);
        }

        loop {
            let msg = self.message_queue.unwrap().pop().await;
            match msg.tag() {
                Tag::DATA_ROW => {
                    self.raw.clear();
                    let r = MessageReader::new(&msg);
                    let num_fields = r.read_i16() as usize;
                    let bytes = msg.as_slice();
                    for _ in 0..num_fields {
                        let len = r.read_i32();
                        if len <= 0 {
                            self.raw.push(&[]); // null
                        } else {
                            let start = r.tell() as usize;
                            let data = &bytes[start..start+(len as usize)];
                            // Safety: we fake a 'static lifetime here, but we ensure the references
                            // don't outlive the buffer in msg (see call to raw.clear() at the top,
                            // and raw = Vec::new() in COMMAND_COMPLETE section below.
                            unsafe {
                                self.raw.push(change_lifetime(data));
                            }
                        }
                    }
                    self.cur = msg;
                    return Ok(true);
                },
                Tag::ROW_DESCRIPTION => {
                    self.fields = RowDescription::new(msg)?;
                    self.raw.reserve(self.fields.len());
                },
                Tag::COMMAND_COMPLETE => {
                    let r = MessageReader::new(&msg);
                    // For all command tags that have a row count, it's the last part of the tag after a space
                    let cmd_tag = r.read_str()?;
                    if let Some(i) = cmd_tag.rfind(' ') {
                        self.affected = (&cmd_tag[i+1..]).parse::<i32>().unwrap_or(0);
                    } else {
                        self.affected = 0;
                    }
                    self.raw = Vec::new();
                    self.cur = Message::default();
                    self.message_queue = None;
                    return Ok(false);
                },
                Tag::ERROR_RESPONSE => {
                    let e = PostgresError::new(msg)?;
                    return Err(Error::from(e));
                },
                Tag::NOTICE_RESPONSE => {
                    let e = PostgresError::new(msg)?;
                    warn!(%e, "notice received while iterating over result in Rows");
                },
                _ => {
                    return Err(Error::new(format!("unexpected message in result {:?}", msg.tag())));
                }
            }
        }
    }
}

impl<'a> Drop for Rows<'a> {
    fn drop(&mut self) {
        assert!(self.affected >= 0, "you MUST call Rows::next() until it returns false or an error");
    }
}



