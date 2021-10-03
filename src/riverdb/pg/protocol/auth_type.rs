use strum::Display;

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
    pub fn as_i32(&self) -> i32 {
        unsafe { std::mem::transmute::<AuthType, u8>(*self) as i32 }
    }
}

impl From<i32> for AuthType {
    fn from(i: i32) -> Self {
        match i {
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
            _ => panic!("unknown auth type {}", i)
        }
    }
}

impl Default for AuthType {
    fn default() -> Self {
        AuthType::Ok
    }
}