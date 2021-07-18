use std::sync::Arc;

use tokio::net::TcpStream;

use crate::riverdb::pg::BackendConnState;
use crate::riverdb::server::{Transport};
use crate::riverdb::pg::Session;


pub struct BackendConn {
    pub session: Arc<Session>, // shared session data
    state: BackendConnState,
}

impl BackendConn {
    pub fn new(stream: TcpStream, session: Option<Arc<Session>>) -> Self {
        let transport = Transport::new(stream);
        BackendConn {
            session: session
                .clone()
                .unwrap_or_else(|| Session::new_with_backend(transport)),
            state: BackendConnState::StateInitial,
        }
    }
}