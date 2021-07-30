use std::marker::PhantomData;
use std::sync::Arc;

use tracing::{debug};

use crate::riverdb::Result;
use crate::riverdb::pg::connection::Connection;
use crate::riverdb::pg::protocol::{Message, MessageParser};
use crate::pg::connection::read_and_flush_backlog;

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

    pub async fn next(&mut self, sender: Option<&W>) -> Result<Message> {
        loop {
            if let Some(result) = self.parser.next() {
                let msg = result?;
                let tag = msg.tag();
                debug!(%tag, sender=?self.read_conn, "received message");

                return self.read_conn
                    .msg_is_allowed(msg.tag())
                    .and(Ok(msg));
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

