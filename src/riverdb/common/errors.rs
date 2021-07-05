use std::io;
use std::sync;
use std::fmt;
use std::net;

use serde_yaml;
use strum::EnumString;

use custom_error::custom_error;
use std::sync::PoisonError;
use std::fmt::Formatter;

custom_error!{pub ErrorKind
    ClosedError = "resource is closed",
    ProtocolError{msg: String} = "{msg}",
    StringError{msg: String} = "{msg}",
    StrError{msg: &'static str} = "{msg}",
    ParseError = "matching variant not found",
    Io{source: io::Error} = "io error",
    Utf8Error{source: std::str::Utf8Error} = "utf8 error {source}",
    AddrParseError{source: net::AddrParseError} = "address parse error {source}",
    Yaml{source: serde_yaml::Error} = "yaml error {source}",
    Tls{source: rustls::Error} = "rustls error {source}",
    PosionError = "poison error",
}

impl PartialEq for ErrorKind {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl Eq for ErrorKind {}

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

impl From<net::AddrParseError> for Error {
    fn from(e: net::AddrParseError) -> Self {
        Error(Box::new(ErrorKind::from(e)))
    }
}

impl From<strum::ParseError> for Error {
    fn from(e: strum::ParseError) -> Self {
        Error(Box::new(ErrorKind::ParseError))
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

