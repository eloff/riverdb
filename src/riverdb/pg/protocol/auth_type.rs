use strum::Display;
use std::convert::TryFrom;

use crate::riverdb::{Error, Result};

/// An enum of PostgreSQL auth types with values corresponding to the auth byte sent on the wire.
/// Note that we don't support all of them.
/// Currently just clear text, md5, and sasl. PRs welcome.
#[derive(Display, Debug, Clone, Copy, Eq, PartialEq)]
#[non_exhaustive]
#[repr(u8)]
pub enum AuthType {
    Ok          = 0,
    KerberosV5  = 2,
    ClearText   = 3,
    MD5         = 5,
    SCM         = 6,
    GSS         = 7,
    GSSContinue = 8,
    SSPI        = 9,
    SASL        = 10,
    SASLContinue = 11,
    SASLFinal = 12,
}

impl AuthType {
    /// Convert the AuthType enum into its integer wire value
    pub fn as_i32(&self) -> i32 {
        unsafe { std::mem::transmute::<AuthType, u8>(*self) as i32 }
    }
}

impl TryFrom<i32> for AuthType {
    type Error = Error;

    /// Parse an AuthType enum from an integer wire value, if possible.
    fn try_from(i: i32) -> Result<Self> {
        Ok(match i {
            0 => AuthType::Ok,
            2 => AuthType::KerberosV5,
            3 => AuthType::ClearText,
            5 => AuthType::MD5,
            6 => AuthType::SCM,
            7 => AuthType::GSS,
            8 => AuthType::GSSContinue,
            9 => AuthType::SSPI,
            10 => AuthType::SASL,
            11 => AuthType::SASLContinue,
            12 => AuthType::SASLFinal,
            _ => return Err(Error::new(format!("unknown auth type {}", i)))
        })
    }
}

impl Default for AuthType {
    /// Returns AuthType::Ok
    fn default() -> Self {
        AuthType::Ok
    }
}