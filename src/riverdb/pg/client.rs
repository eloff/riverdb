use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};
use tokio_stream::StreamExt;
use futures::SinkExt;

use crate::riverdb::common::{Error, Result};

pub struct PostgresSession {
    stream: TcpStream
}

impl PostgresSession {
    pub fn new(stream: TcpStream) -> PostgresSession {
        PostgresSession{stream}
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut lines = Framed::new(&mut self.stream, LinesCodec::new());

        let greeting = lines.next().await.ok_or(Error::new("end of stream"))?.map_err(Error::new)?;
        let wat = lines.send(greeting).await.map_err(Error::new)?;
        Ok(())
    }
}