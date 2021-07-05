use std::fmt::{Debug};

use strum::Display;

use crate::riverdb::pg::protocol::Tag;
use crate::riverdb::{Error, Result};

#[derive(Display, Debug)]
#[non_exhaustive]
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