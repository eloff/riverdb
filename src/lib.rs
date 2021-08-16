#![cfg(not(feature = "main"))]

pub mod riverdb;
#[cfg(test)]
mod tests;

pub use crate::riverdb::*;