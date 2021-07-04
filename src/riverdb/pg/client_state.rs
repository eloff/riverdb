use std::fmt::{Debug, Display};

use strum_macros::Display;

use crate::riverdb::pg::protocol::Tag;
use crate::riverdb::{Error, Result};

#[derive(Display, Debug)]
pub enum ClientConnState {
    StateInitial,
    SSLHandshake,
    Authentication,
    Ready,
    Transaction,
    FailedTransaction,
    Closed,
}

impl ClientConnState {
    pub fn msg_is_allowed(&self, tag: Tag) -> bool {
        todo!()
    }

    pub fn transition(&mut self, new_state: ClientConnState) -> Result<()> {
        todo!();
    }
}