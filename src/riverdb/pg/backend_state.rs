use std::fmt::{Debug, Formatter};
use std::sync::atomic::Ordering::{Release, Relaxed, Acquire};

use strum::Display;
use tracing::{debug, instrument};

use crate::riverdb::{Error, Result};
use crate::riverdb::common::AtomicCell;
use crate::riverdb::pg::protocol::Tag;
use crate::riverdb::pg::BackendConn;
use std::mem::transmute;


#[derive(Display, Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
#[repr(u16)]
pub enum BackendState {
    StateInitial = 1,
    SSLHandshake = 2,
    Authentication = 4,
    Startup = 8,
    Ready = 16,
    Transaction = 32,
    FailedTransaction = 64,
    Listen = 128,
    InPool = 256,
    Closed = 512,
}

pub trait StateEnum: Sized + Copy where u32: From<Self>
{
    fn ordinal(&self) -> u32 {
        let i = u32::from(*self);
        debug_assert_ne!(i, 0);
        i.trailing_zeros()
    }

    fn is_final(&self) -> bool {
        false
    }
}

impl StateEnum for BackendState {
    fn is_final(&self) -> bool {
        if let BackendState::Closed = self {
            true
        } else {
            false
        }
    }
}

impl From<BackendState> for u16  {
    fn from(s: BackendState) -> Self {
        s.as_u16()
    }
}

impl From<BackendState> for u32 {
    fn from(s: BackendState) -> Self {
        s.as_u16() as u32
    }
}

impl BackendState {
    /// Returns the underlying u16 representation of the enum
    // TODO: once transmute/transmute_copy can be used in const functions, make this const
    // (and eliminate the transmutes in transition method)
    pub fn as_u16(&self) -> u16 {
        // Safety: BackendState enum is #[repr(u16)]
        unsafe { transmute::<BackendState, u16>(*self) }
    }
}

pub struct BackendConnState(AtomicCell<BackendState>);

impl BackendConnState {
    pub fn new(state: BackendState) -> Self {
        Self(AtomicCell::new(state))
    }

    pub fn msg_is_allowed(&self, tag: Tag) -> bool {
        // TODO check if it's allowed

        true
    }

    pub fn transition(&self, backend: &BackendConn, new_state: BackendState) -> Result<()> {
        // Indexed by log2(new_state), this is a list of allowed states that can transition to new_state
        // Indexing by new_state instead of state has fewer data dependencies
        // (can execute immediately, because it doesn't have to wait to load current state.)
        // Safety: BackendState enum is #[repr(u16)], see note on as_u16.
        static ALLOWED_TRANSITIONS: [u16; 9] = unsafe {
            [
                0, // no valid transitions to StateInitial
                transmute::<_, u16>(BackendState::StateInitial), // SSLHandshake
                transmute::<_, u16>(BackendState::StateInitial) | transmute::<_, u16>(BackendState::SSLHandshake), // Authentication
                transmute::<_, u16>(BackendState::Authentication), // Startup
                transmute::<_, u16>(BackendState::Startup) |
                    transmute::<_, u16>(BackendState::InPool) |
                    transmute::<_, u16>(BackendState::Transaction) |
                    transmute::<_, u16>(BackendState::FailedTransaction), // Ready
                transmute::<_, u16>(BackendState::Ready), // Transaction
                transmute::<_, u16>(BackendState::Transaction), // FailedTransaction
                transmute::<_, u16>(BackendState::Ready), // Listen
                transmute::<_, u16>(BackendState::Ready), // InPool
            ]
        };

        let state = self.0.load();
        checked_state_transition(backend, &ALLOWED_TRANSITIONS[..], state, new_state)?;
        self.0.store(new_state);
        Ok(())
    }

    pub fn get(&self) -> BackendState {
        self.0.load()
    }
}

impl Default for BackendConnState {
    fn default() -> Self {
        Self::new(BackendState::StateInitial)
    }
}

impl Debug for BackendConnState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0.load(), f)
    }
}

#[instrument]
pub fn checked_state_transition<T: Debug, S: Copy + Debug + Eq + StateEnum>(subject: &T, allowed_transitions: &[u16], state: S, new_state: S) -> Result<()>
    where u32: From<S>
{
    if state == new_state {
        return Ok(())
    }

    let i = new_state.ordinal() as usize;
    if new_state.is_final() || allowed_transitions.get(i).unwrap() & (u32::from(state) as u16) == 0 {
        return Err(Error::new(format!("invalid transition from {:?} to {:?}", state, new_state)));
    }

    debug!("transitioned {:?} from {:?} to {:?}", subject, state, new_state);
    Ok(())
}