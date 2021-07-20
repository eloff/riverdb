use std::fmt::Debug;

use strum::Display;

#[derive(Display, Debug)]
pub enum IsolationLevel {
    None,
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

