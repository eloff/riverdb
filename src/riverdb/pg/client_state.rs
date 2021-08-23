use std::fmt::{Debug, Formatter};


use strum::Display;

use crate::riverdb::pg::protocol::Tag;
use crate::riverdb::{Result};
use crate::riverdb::common::{AtomicCell};
use std::mem::transmute;
use crate::riverdb::pg::ClientConn;
use crate::riverdb::pg::backend_state::{checked_state_transition, StateEnum};


#[derive(Display, Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
#[repr(u16)]
pub enum ClientState {
    StateInitial = 1,
    SSLHandshake = 2,
    Authentication = 4,
    Ready = 8,
    Transaction = 16,
    FailedTransaction = 32,
    Listen = 64,
    Closed = 128,
}

impl StateEnum for ClientState {
    fn is_final(&self) -> bool {
        if let ClientState::Closed = self {
            true
        } else {
            false
        }
    }
}

impl From<ClientState> for u16  {
    fn from(s: ClientState) -> Self {
        s.as_u16()
    }
}

impl From<ClientState> for u32 {
    fn from(s: ClientState) -> Self {
        s.as_u16() as u32
    }
}

impl ClientState {
    /// Returns the underlying u16 representation of the enum
    // TODO: once transmute/transmute_copy can be used in const functions, make this const
    // (and eliminate the transmutes in transition method)
    pub fn as_u16(&self) -> u16 {
        // Safety: ClientState enum is #[repr(u16)]
        unsafe { transmute::<ClientState, u16>(*self) }
    }
}

pub struct ClientConnState(AtomicCell<ClientState>);

impl ClientConnState {
    pub fn new(state: ClientState) -> Self {
        Self(AtomicCell::new(state))
    }

    pub fn msg_is_allowed(&self, tag: Tag) -> bool {
        if tag == Tag::TERMINATE {
            return true;
        }

        // Tags expected from server in Ready or Transaction states
        const REQUEST_TAGS: &'static [Tag] = &[
            Tag::QUERY,
            Tag::BIND,
            Tag::EXECUTE,
            Tag::FUNCTION_CALL,
            Tag::CLOSE,
            Tag::PARSE,
            Tag::DESCRIBE,
            Tag::FLUSH,
            Tag::SYNC,
        ];

        const ALLOWED_TAGS: [&'static [Tag]; 8] = [
            &[Tag::UNTAGGED], // StateInitial (the startup tag)
            &[], // no valid tags in SSLHandshake
            &[Tag::PASSWORD_MESSAGE, Tag::AUTHENTICATION_OK, Tag::ERROR_RESPONSE], // Authentication
            REQUEST_TAGS, // Ready
            REQUEST_TAGS, // Transaction
            &[], // FailedTransaction
            &[], // Listen
            &[], // no valid tags in Closed
        ];

        let state = self.0.load();
        unsafe {
            memchr::memchr(
                tag.as_u8(),
                transmute::<&[Tag], &[u8]>(ALLOWED_TAGS.get(state.ordinal() as usize).unwrap()),
            ).is_some()
        }
    }

    pub fn transition(&self, client: &ClientConn, new_state: ClientState) -> Result<()> {
        if new_state == ClientState::Closed {
            self.0.store(new_state);
            return Ok(());
        }
        // Indexed by log2(new_state), this is a list of allowed states that can transition to new_state
        // Indexing by new_state instead of state has fewer data dependencies
        // (can execute immediately, because it doesn't have to wait to load current state.)
        // Safety: BackendState enum is #[repr(u16)], see note on as_u16.
        static ALLOWED_TRANSITIONS: [u16; 7] = unsafe {
            [
                0, // no valid transitions to StateInitial
                transmute::<_, u16>(ClientState::StateInitial), // SSLHandshake
                transmute::<_, u16>(ClientState::StateInitial) | transmute::<_, u16>(ClientState::SSLHandshake), // Authentication
                transmute::<_, u16>(ClientState::Authentication) |
                    transmute::<_, u16>(ClientState::Transaction) |
                    transmute::<_, u16>(ClientState::FailedTransaction), // Ready
                transmute::<_, u16>(ClientState::Ready), // Transaction
                transmute::<_, u16>(ClientState::Transaction), // FailedTransaction
                transmute::<_, u16>(ClientState::Ready), // Listen
            ]
        };

        let state = self.0.load();
        checked_state_transition(client, &ALLOWED_TRANSITIONS[..], state, new_state)?;
        self.0.store(new_state);
        Ok(())
    }

    pub fn get(&self) -> ClientState {
        self.0.load()
    }
}

impl Default for ClientConnState {
    fn default() -> Self {
        Self::new(ClientState::StateInitial)
    }
}

impl Debug for ClientConnState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0.load(), f)
    }
}