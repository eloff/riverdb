use std::fmt::{Debug, Display};
use std::{fmt, io, result};
use std::net::AddrParseError;
use std::sync::{TryLockError, MutexGuard, PoisonError};

#[derive(Debug, PartialEq, Eq)]
pub struct Error {
    err: Box<ErrorKind>, // use a Box to keep the Result type smaller
}

#[derive(Debug)]
pub struct ScriptError {
    pub msg: String,
    pub stack: String,
}

impl ScriptError {
    pub fn new(msg: String, stack: String) -> Self {
        ScriptError {
            msg, stack
        }
    }
}

#[derive(Debug)]
pub enum ErrorKind {
    LowMemoryError,
    TooBusyError,
    Timeout,
    PoisonError,
    ClosedError,
    StringError(String),
    IOError(io::Error),
    //JSONError(serde_json::Error),
    YAMLError(serde_yaml::Error),
    TlsError(rustls::Error),
    UTF8Error(std::str::Utf8Error),
    ArrayFromSliceError(std::array::TryFromSliceError),
    ScriptError(ScriptError),
}

pub type Result<T> = result::Result<T, Error>;

impl Error {
    pub fn new<S: ToString>(s: S) -> Self {
        Error {
            err: Box::new(ErrorKind::StringError(s.to_string())),
        }
    }

    pub fn low_mem() -> Self {
        Error {
            err: Box::new(ErrorKind::LowMemoryError),
        }
    }

    pub fn too_busy() -> Self {
        Error {
            err: Box::new(ErrorKind::TooBusyError),
        }
    }

    pub fn closed() -> Self {
        Error {
            err: Box::new(ErrorKind::ClosedError),
        }
    }

    pub fn script_error(msg: String, stack: String) -> Self {
        Error {
            err: Box::new(ErrorKind::ScriptError(ScriptError::new(msg, stack))),
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.err
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Self {
        Error {
            err: Box::new(ErrorKind::StringError(String::from(err))),
        }
    }
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error {
            err: Box::new(ErrorKind::StringError(err)),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error {
            err: Box::new(ErrorKind::IOError(err)),
        }
    }
}

// impl From<serde_json::Error> for Error {
//     fn from(err: serde_json::Error) -> Self {
//         Error {
//             err: Box::new(ErrorKind::JSONError(err)),
//         }
//     }
// }

impl From<serde_yaml::Error> for Error {
    fn from(err: serde_yaml::Error) -> Self {
        Error {
            err: Box::new(ErrorKind::YAMLError(err)),
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error {
            err: Box::new(ErrorKind::UTF8Error(err)),
        }
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error {
            err: Box::new(ErrorKind::ArrayFromSliceError(err)),
        }
    }
}

impl From<AddrParseError> for Error {
    fn from(err: AddrParseError) -> Self {
        Error::new(err)
    }
}

impl<Guard> From<PoisonError<Guard>> for Error {
    fn from(err: PoisonError<Guard>) -> Self {
        Error {
            err: Box::new(ErrorKind::PoisonError),
        }
    }
}

impl From<rustls::Error> for Error {
    fn from(err: rustls::Error) -> Self {
        Error {
            err: Box::new(ErrorKind::TlsError(err)),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        std::fmt::Display::fmt(&self.err, f)
    }
}

impl Display for ScriptError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        std::fmt::Display::fmt(&self.msg, f)
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ErrorKind::LowMemoryError => f.write_str("not enough memory to handle this request"),
            ErrorKind::TooBusyError => f.write_str("server is too busy to handle this request"),
            ErrorKind::ClosedError => f.write_str("socket/file is closed"),
            ErrorKind::PoisonError => f.write_str("another thread panicked while holding the mutex"),
            ErrorKind::StringError(s) => f.write_str(&s),
            ErrorKind::IOError(e) => std::fmt::Display::fmt(&e, f),
            //ErrorKind::JSONError(e) => std::fmt::Display::fmt(&e, f),
            ErrorKind::YAMLError(e) => std::fmt::Display::fmt(&e, f),
            ErrorKind::TlsError(e) => std::fmt::Display::fmt(&e, f),
            ErrorKind::UTF8Error(e) => std::fmt::Display::fmt(&e, f),
            ErrorKind::ArrayFromSliceError(e) => std::fmt::Display::fmt(&e, f),
            ErrorKind::ScriptError(e) => std::fmt::Display::fmt(&e, f),
            ErrorKind::Timeout => f.write_str("operation timed out"),
        }
    }
}

impl PartialEq for ErrorKind {
    fn eq(&self, other: &Self) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl Eq for ErrorKind {}
