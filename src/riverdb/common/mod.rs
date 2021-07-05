mod errors;
mod bytes;

pub use self::errors::{Error, Result};
pub use self::bytes::bytes_to_slice_mut;