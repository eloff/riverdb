use std::fmt::{Display, Formatter};

use crate::riverdb::common::{Result, Error};

pub const SSL_ALLOWED: u8 = 'S' as u8;
pub const SSL_NOT_ALLOWED: u8 = 'N' as u8;
pub const SSL_REQUEST: i32 = 80877103;

// Tag defines the Postgres protocol message type tag bytes
#[derive(Copy, Clone)]
pub struct Tag(u8);

const UNTAGGED: Tag = Tag(0); // includes Startup, CancelRequest, SSLRequest, GSSENCRequest
// Frontend
const BIND: Tag = Tag::new_unchecked('B');
const CLOSE: Tag = Tag::new_unchecked('C'); // close prepared statement or portal
const COPY_FAIL: Tag = Tag::new_unchecked('f');
const DESCRIBE: Tag = Tag::new_unchecked('D');
const EXECUTE: Tag = Tag::new_unchecked('E');
const FLUSH: Tag = Tag::new_unchecked('H');
const FUNCTION_CALL: Tag = Tag::new_unchecked('F');
const PARSE: Tag = Tag::new_unchecked('P');
const PASSWORD_MESSAGE: Tag = Tag::new_unchecked('p'); // also used for GSSAPI, SSPI and SASL
const QUERY: Tag = Tag::new_unchecked('Q');
const SYNC: Tag = Tag::new_unchecked('S');
const TERMINATE: Tag = Tag::new_unchecked('X');
// Frontend + Backend
const COPY_DATA: Tag = Tag::new_unchecked('d');
const COPY_DONE: Tag = Tag::new_unchecked('c');
// Backend
const AUTHENTICATION_OK: Tag = Tag::new_unchecked('R'); // one of AuthenticationKerberosV5, AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationSCMCredential, AuthenticationGSS, AuthenticationSSPI, AuthenticationGSSContinue, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal
const BACKEND_KEY_DATA: Tag = Tag::new_unchecked('K');
const BIND_COMPLETE: Tag = Tag::new_unchecked('2');
const CLOSE_COMPLETE: Tag = Tag::new_unchecked('3');
const COMMAND_COMPLETE: Tag = Tag::new_unchecked('C');
const COPY_IN_RESPONSE: Tag = Tag::new_unchecked('G');
const COPY_OUT_RESPONSE: Tag = Tag::new_unchecked('H');
const COPY_BOTH_RESPONSE: Tag = Tag::new_unchecked('W');
const DATA_ROW: Tag = Tag::new_unchecked('D');
const EMPTY_QUERY: Tag = Tag::new_unchecked('I');
const FUNCTION_CALL_RESPONSE: Tag = Tag::new_unchecked('V');
const NEGOTIATE_PROTOCOL_VERSION: Tag = Tag::new_unchecked('v');
const NO_DATA: Tag = Tag::new_unchecked('n');
const PARAMETER_DESCRIPTION: Tag = Tag::new_unchecked('t');
const PARSE_COMPLETE: Tag = Tag::new_unchecked('1');
const PORTAL: Tag = Tag::new_unchecked('s');
const READY_FOR_QUERY: Tag = Tag::new_unchecked('Z');
const ROW_DESCRIPTION: Tag = Tag::new_unchecked('T');
// Backend Async Messages (can also be synchronous, depending on context)
const ERROR_RESPONSE: Tag = Tag::new_unchecked('E'); // can be sent async e.g. if server is shutdown gracefully
const PARAMETER_STATUS: Tag = Tag::new_unchecked('S');
const NOTICE_RESPONSE: Tag = Tag::new_unchecked('N');
const NOTIFICATION_RESPONSE: Tag = Tag::new_unchecked('A');

impl Tag {
    pub fn new(b: u8) -> Result<Self> {
        if let Some(name) = TAG_NAMES.get(b as usize) {
            if name.is_empty() {
                return Ok(Tag(b));
            }
        }
        Err(Error::new(format!("Unknown message tag '{}'", b as char)))
    }

    pub const fn new_unchecked(c: char) -> Self {
        Tag(c as u8)
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