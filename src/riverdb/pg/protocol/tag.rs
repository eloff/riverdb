use strum::Display;

use crate::riverdb::{Error, Result};

pub const SSL_ALLOWED: u8 = 'S' as u8;
pub const SSL_NOT_ALLOWED: u8 = 'N' as u8;
pub const SSL_REQUEST: i32 = 80877103;

// Tag defines the Postgres protocol message type tag bytes
#[derive(Display, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
#[non_exhaustive]
pub enum Tag {
    Untagged = 0, // includes Startup, CancelRequest, SSLRequest, GSSENCRequest
    // Frontend
    Bind = 'B' as u8,
    // close prepared statement or portal
    CopyFail = 'f' as u8,
    FunctionCall = 'F' as u8,
    Parse = 'P' as u8,
    PasswordMessage = 'p' as u8,
    // also used for GSSAPI, SSPI and SASL
    Query = 'Q' as u8,
    Terminate = 'X' as u8,
    // Frontend + Backend
    CopyData = 'd' as u8,
    CopyDone = 'c' as u8,
    DescribeOrDataRow = 'D' as u8,
    ExecuteOrError = 'E' as u8,
    SyncOrParameterStatus = 'S' as u8,
    CloseOrCommandComplete = 'C' as u8,
    FlushOrCopyOutResponse = 'H' as u8,
    // Backend
    AuthenticationOk = 'R' as u8,
    // one of AuthenticationKerberosV5, AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationSCMCredential, AuthenticationGSS, AuthenticationSSPI, AuthenticationGSSContinue, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal
    BackendKeyData = 'K' as u8,
    BindComplete = '2' as u8,
    CloseComplete = '3' as u8,
    CopyInResponse = 'G' as u8,
    CopyBothResponse = 'W' as u8,
    EmptyQuery = 'I' as u8,
    FunctionCallResponse = 'V' as u8,
    NegotiateProtocolVersion = 'v' as u8,
    NoData = 'n' as u8,
    ParameterDescription = 't' as u8,
    ParseComplete = '1' as u8,
    Portal = 's' as u8,
    ReadyForQuery = 'Z' as u8,
    RowDescription = 'T' as u8,
    // Backend Async Messages (can also be synchronous, depending on context)
    // ExecuteOrError 'E' as u8
    // can be sent async e.g. if server is shutdown gracefully
    NoticeResponse = 'N' as u8,
    NotificationResponse = 'A' as u8,
}

impl Tag {
    pub fn new(b: u8) -> Result<Self> {
        let tag = Self::new_unchecked(b as char);
        match tag {
            Tag::Untagged |
            Tag::Bind |
            Tag::CopyFail |
            Tag::FunctionCall |
            Tag::Parse |
            Tag::PasswordMessage |
            Tag::Query |
            Tag::Terminate |
            Tag::CopyData |
            Tag::CopyDone |
            Tag::DescribeOrDataRow |
            Tag::ExecuteOrError |
            Tag::SyncOrParameterStatus |
            Tag::AuthenticationOk |
            Tag::BackendKeyData |
            Tag::BindComplete |
            Tag::CloseComplete |
            Tag::CopyInResponse |
            Tag::CopyBothResponse |
            Tag::EmptyQuery |
            Tag::FunctionCallResponse |
            Tag::NegotiateProtocolVersion |
            Tag::NoData |
            Tag::ParameterDescription |
            Tag::ParseComplete |
            Tag::Portal |
            Tag::ReadyForQuery |
            Tag::RowDescription |
            Tag::NoticeResponse |
            Tag::NotificationResponse => Ok(tag),
            _ => Err(Error::new(format!("Unknown message tag '{}'", b as char))),
        }
    }

    pub fn new_unchecked(c: char) -> Self {
        unsafe { std::mem::transmute(c as u8) }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}
