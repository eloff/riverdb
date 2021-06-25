use crate::riverdb::pg::protocol::Tag;
use std::fmt::{Display, Formatter};

pub enum PostgresClientConnectionState {
    ClientConnectionStateInitial,
    ClientConnectionSSLHandshake,
    ClientConnectionAuthentication,
    ClientConnectionReady,
    ClientConnectionClosed,
}

impl PostgresClientConnectionState {
    pub fn msg_is_allowed(&self, tag: Tag) -> bool {
        todo!()
    }
}

impl Display for PostgresClientConnectionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}