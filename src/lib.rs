#![cfg(not(feature = "main"))]

mod riverdb;
#[cfg(test)]
mod tests;

pub use crate::riverdb::*;