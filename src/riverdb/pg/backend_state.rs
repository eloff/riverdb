use std::fmt::{Debug, Formatter};

use strum::Display;

use crate::riverdb::pg::protocol::Tag;
use crate::riverdb::{Error, Result};
use crate::riverdb::common::AtomicCell8;
use std::sync::atomic::Ordering::{Release, Relaxed, Acquire};

#[derive(Display, Debug, Clone, Copy)]
#[non_exhaustive]
#[repr(u8)]
pub enum BackendState {
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

pub struct BackendConnState(AtomicCell8<BackendState>);

impl BackendConnState {
    pub fn new(state: BackendState) -> Self {
        Self(AtomicCell8::new(state))
    }

    pub fn msg_is_allowed(&self, tag: Tag) -> bool {
        // TODO check if it's allowed

        true
    }

    pub fn transition(&self, new_state: BackendState) -> Result<()> {
        // TODO check if it's allowed

        self.0.store(new_state, Release);
        Ok(())
    }

    pub fn get(&self) -> BackendState {
        self.0.load(Acquire)
    }
}

impl Default for BackendConnState {
    fn default() -> Self {
        Self::new(BackendState::StateInitial)
    }
}

impl Debug for BackendConnState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0.load(Relaxed), f)
    }
}