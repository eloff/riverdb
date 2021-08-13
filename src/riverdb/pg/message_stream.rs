use std::marker::PhantomData;
use std::sync::Arc;

use tracing::{debug};

use crate::riverdb::Result;
use crate::riverdb::pg::connection::Connection;
use crate::riverdb::pg::protocol::{Messages, MessageParser};
use crate::riverdb::pg::connection::read_and_flush_backlog;

pub struct MessageStream<'a, R: Connection, W: Connection> {
    read_conn: &'a R,
    parser: MessageParser,
    _phantom: PhantomData<W>,
}

impl<'a, R: Connection, W: Connection> MessageStream<'a, R, W> {
    pub fn new(read_conn: &'a R) -> Self {
        Self{
            read_conn,
            parser: MessageParser::new(),
            _phantom: PhantomData,
        }
    }

    pub async fn next(&mut self, sender: Option<&W>) -> Result<Messages> {
        loop {
            if let Some(result) = self.parser.next() {
                let msgs = result?;
                debug!(msgs=?&msgs, sender=?self.read_conn, "received messages");

                return Ok(msgs);
            } else {
                read_and_flush_backlog(
                    self.read_conn,
                    self.parser.bytes_mut(),
                    sender,
                ).await?;
            }
        }
    }
}

