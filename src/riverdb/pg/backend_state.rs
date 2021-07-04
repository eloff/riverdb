use std::fmt::{Debug, Display};

use crate::riverdb::pg::protocol::Tag;
use crate::riverdb::common::{Result, Error};

#[derive(Display, Debug)]
pub enum BackendConnState {
    StateInitial,
    SSLHandshake,
    Authentication,
    Startup,
    Ready,
    Transaction,
    FailedTransaction,
    Listen,
    InPool,
    SetRole,
    Closed,
}

impl BackendConnState {
    pub fn msg_is_allowed(&self, tag: Tag) -> bool {
        todo!()
    }

    pub fn transition(&mut self, new_state: BackendConnState) -> Result<()> {
        todo!();
    }
}