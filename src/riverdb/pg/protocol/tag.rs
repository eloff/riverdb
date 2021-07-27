use std::fmt::{Display, Formatter, Debug};

use crate::riverdb::{Error, Result};

pub const SSL_ALLOWED: u8 = 'S' as u8;
pub const SSL_NOT_ALLOWED: u8 = 'N' as u8;
pub const SSL_REQUEST: i32 = 80877103;
pub const PROTOCOL_VERSION: i32 = 196608;

// Tag defines the Postgres protocol message type tag bytes
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Tag(u8);

impl Tag {
    pub const UNTAGGED: Tag = Tag(0);
    // includes Startup, CancelRequest, SSLRequest, GSSENCRequest
    // Frontend
    pub const BIND: Tag = Tag::new_unchecked('B' as u8);
    pub const CLOSE: Tag = Tag::new_unchecked('C' as u8);
    // close prepared statement or portal
    pub const COPY_FAIL: Tag = Tag::new_unchecked('f' as u8);
    pub const DESCRIBE: Tag = Tag::new_unchecked('D' as u8);
    pub const EXECUTE: Tag = Tag::new_unchecked('E' as u8);
    pub const FLUSH: Tag = Tag::new_unchecked('H' as u8);
    pub const FUNCTION_CALL: Tag = Tag::new_unchecked('F' as u8);
    pub const PARSE: Tag = Tag::new_unchecked('P' as u8);
    pub const PASSWORD_MESSAGE: Tag = Tag::new_unchecked('p' as u8);
    // also used for GSSAPI, SSPI and SASL
    pub const QUERY: Tag = Tag::new_unchecked('Q' as u8);
    pub const SYNC: Tag = Tag::new_unchecked('S' as u8);
    pub const TERMINATE: Tag = Tag::new_unchecked('X' as u8);
    // Frontend + Backend
    pub const COPY_DATA: Tag = Tag::new_unchecked('d' as u8);
    pub const COPY_DONE: Tag = Tag::new_unchecked('c' as u8);
    // Backend
    pub const AUTHENTICATION_OK: Tag = Tag::new_unchecked('R' as u8);
    // one of AuthenticationKerberosV5, AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationSCMCredential, AuthenticationGSS, AuthenticationSSPI, AuthenticationGSSContinue, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal
    pub const BACKEND_KEY_DATA: Tag = Tag::new_unchecked('K' as u8);
    pub const BIND_COMPLETE: Tag = Tag::new_unchecked('2' as u8);
    pub const CLOSE_COMPLETE: Tag = Tag::new_unchecked('3' as u8);
    pub const COMMAND_COMPLETE: Tag = Tag::new_unchecked('C' as u8);
    pub const COPY_IN_RESPONSE: Tag = Tag::new_unchecked('G' as u8);
    pub const COPY_OUT_RESPONSE: Tag = Tag::new_unchecked('H' as u8);
    pub const COPY_BOTH_RESPONSE: Tag = Tag::new_unchecked('W' as u8);
    pub const DATA_ROW: Tag = Tag::new_unchecked('D' as u8);
    pub const EMPTY_QUERY: Tag = Tag::new_unchecked('I' as u8);
    pub const FUNCTION_CALL_RESPONSE: Tag = Tag::new_unchecked('V' as u8);
    pub const NEGOTIATE_PROTOCOL_VERSION: Tag = Tag::new_unchecked('v' as u8);
    pub const NO_DATA: Tag = Tag::new_unchecked('n' as u8);
    pub const PARAMETER_DESCRIPTION: Tag = Tag::new_unchecked('t' as u8);
    pub const PARSE_COMPLETE: Tag = Tag::new_unchecked('1' as u8);
    pub const PORTAL: Tag = Tag::new_unchecked('s' as u8);
    pub const READY_FOR_QUERY: Tag = Tag::new_unchecked('Z' as u8);
    pub const ROW_DESCRIPTION: Tag = Tag::new_unchecked('T' as u8);
    // Backend Async Messages (can also be synchronous, depending on context)
    pub const ERROR_RESPONSE: Tag = Tag::new_unchecked('E' as u8);
    // can be sent async e.g. if server is shutdown gracefully
    pub const PARAMETER_STATUS: Tag = Tag::new_unchecked('S' as u8);
    pub const NOTICE_RESPONSE: Tag = Tag::new_unchecked('N' as u8);
    pub const NOTIFICATION_RESPONSE: Tag = Tag::new_unchecked('A' as u8);

    pub fn new(b: u8) -> Result<Self> {
        if let Some(name) = TAG_NAMES.get(b as usize) {
            if name.is_empty() {
                return Ok(Tag(b));
            }
        }
        Err(Error::new(format!("Unknown message tag '{}'", b as char)))
    }

    pub const fn new_unchecked(b: u8) -> Self {
        Tag(b)
    }

    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

static TAG_NAMES: [&'static str; ('z' as usize) + 1] = [
    "Untagged",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "ParseComplete",
    "BindComplete",
    "CloseComplete",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "NotificationResponse",
    "",
    "CommandComplete",
    "DataRow", // also Describe
    "ErrorResponse",
    "FunctionCall",
    "CopyInResponse",
    "CopyOutResponse",
    "EmptyQuery",
    "",
    "BackendKeyData",
    "",
    "",
    "NoticeResponse",
    "",
    "Parse",
    "Query",
    "AuthenticationOk",
    "ParameterStatus",
    "RowDescription",
    "",
    "FunctionCallResponse",
    "CopyBothResponse",
    "Terminate",
    "",
    "ReadyForQuery",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "CopyDone",
    "CopyData",
    "",
    "CopyFail",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "NoData",
    "",
    "PasswordMessage",
    "",
    "",
    "Portal",
    "ParameterDescription",
    "",
    "NegotiateProtocolVersion",
    "",
    "",
    "",
    "",
];

impl Display for Tag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(name) = TAG_NAMES.get(self.0 as usize) {
            return f.write_str(name);
        }
        f.write_fmt(format_args!("Unknown message tag '{}'", self.0))
    }
}

impl Debug for Tag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}