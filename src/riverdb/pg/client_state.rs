use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::Ordering::{Release, Relaxed, Acquire};

use strum::Display;

use crate::riverdb::pg::protocol::Tag;
use crate::riverdb::{Error, Result};
use crate::riverdb::common::AtomicCell8;


#[derive(Display, Debug, Clone, Copy)]
#[non_exhaustive]
#[repr(u8)]
pub enum ClientState {
    StateInitial,
    SSLHandshake,
    Authentication,
    Ready,
    Transaction,
    FailedTransaction,
    Closed,
}

pub struct ClientConnState(AtomicCell8<ClientState>);

impl ClientConnState {
    pub fn new(state: ClientState) -> Self {
        Self(AtomicCell8::new(state))
    }

    pub fn msg_is_allowed(&self, tag: Tag) -> bool {
        // TODO check if it's allowed

        true
    }

    pub fn transition(&self, new_state: ClientState) -> Result<()> {
        // TODO check if it's allowed

        self.0.store(new_state, Release);
        Ok(())
    }

    pub fn get(&self) -> ClientState {
        self.0.load(Acquire)
    }
}

impl Default for ClientConnState {
    fn default() -> Self {
        Self::new(ClientState::StateInitial)
    }
}

impl Debug for ClientConnState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0.load(Relaxed), f)
    }
}