use crate::riverdb::pg::protocol::Tag;
use std::fmt::{Display, Formatter};

pub enum ClientConnState {
    ClientConnectionStateInitial,
    ClientConnectionSSLHandshake,
    ClientConnectionAuthentication,
    ClientConnectionReady,
    ClientConnectionClosed,
}

impl ClientConnState {
    pub fn msg_is_allowed(&self, tag: Tag) -> bool {
        todo!()
    }
}

impl Display for ClientConnState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}