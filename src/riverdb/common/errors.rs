use std::io;
use std::sync;
use std::fmt;
use std::net;
use std::sync::PoisonError;
use std::fmt::Formatter;

use serde_yaml;
use custom_error::custom_error;

use crate::riverdb::pg::protocol::PostgresError;


custom_error!{pub ErrorKind
    ClosedError = "resource is closed",
    ProtocolError{msg: String} = "{msg}",
    StringError{msg: String} = "{msg}",
    StrError{msg: &'static str} = "{msg}",
    StrumParseError = "matching variant not found",
    PosionError = "poison error",
    PostgresError{source: PostgresError} = "{source}",
    Io{source: io::Error} = "io error",
    Utf8Error{source: std::str::Utf8Error} = "{source}",
    ParseIntError{source: std::num::ParseIntError} = "{source}",
    AddrParseError{source: net::AddrParseError} = "{source}",
    Yaml{source: serde_yaml::Error} = "{source}",
    Tls{source: rustls::Error} = "{source}",
}

impl PartialEq for ErrorKind {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl Eq for ErrorKind {}

/// Error type that boxes errors for performance.
/// Having a large error type means copying large Result objects around everywhere.
#[derive(Debug, PartialEq, Eq)]
pub struct Error(Box<ErrorKind>);

impl Error {
    pub fn new<S: ToString>(s: S) -> Self {
        Error(Box::new(ErrorKind::StringError{msg: s.to_string()}))
    }

    pub fn protocol_error<S: ToString>(s: S) -> Self {
        Error(Box::new(ErrorKind::ProtocolError{msg: s.to_string()}))
    }

    pub fn closed() -> Self {
        Error(Box::new(ErrorKind::ClosedError))
    }
}

impl From<&'static str> for Error {
    fn from(s: &'static str) -> Self {
        Error(Box::new(ErrorKind::StrError { msg: s }))
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        Error(Box::new(ErrorKind::Utf8Error { source: e }))
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error(Box::new(ErrorKind::from(e)))
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Error(Box::new(ErrorKind::from(e)))
    }
}

impl From<net::AddrParseError> for Error {
    fn from(e: net::AddrParseError) -> Self {
        Error(Box::new(ErrorKind::from(e)))
    }
}

impl From<strum::ParseError> for Error {
    fn from(e: strum::ParseError) -> Self {
        Error(Box::new(ErrorKind::StrumParseError))
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(e: serde_yaml::Error) -> Self {
        Error(Box::new(ErrorKind::from(e)))
    }
}

impl From<rustls::Error> for Error {
    fn from(e: rustls::Error) -> Self {
        Error(Box::new(ErrorKind::from(e)))
    }
}

impl From<PostgresError> for Error {
    fn from(e: PostgresError) -> Self {
        Error(Box::new(ErrorKind::from(e)))
    }
}

impl<T> From<sync::PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Error(Box::new(ErrorKind::PosionError))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

