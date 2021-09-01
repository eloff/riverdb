use std::marker::PhantomData;


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
            read_and_flush_backlog(
                self.read_conn,
                self.parser.bytes_mut(),
                sender,
            ).await?;

            loop {
                if let Some(result) = self.parser.next() {
                    let msgs = result?;
                    debug!(msgs=?&msgs, sender=?self.read_conn, "received messages");

                    return Ok(msgs);
                } else {
                    // We can keep reading cheaper than calling read_and_flush_backlog again
                    // Until try_read returns EWOULDBLOCK, which is Ok(0) in this case.
                    // Because the docs for ready() used inside read_and_flush_backlog say:
                    //   Once a readiness event occurs, the method will continue to return
                    //   immediately until the readiness event is consumed by an attempt to
                    //   read or write that fails with WouldBlock.
                    let bytes_read = self.read_conn.try_read(self.parser.bytes_mut())?;
                    if bytes_read == 0 {
                        break;
                    }
                }
            }
        }
    }
}

