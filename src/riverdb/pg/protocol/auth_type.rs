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
}

impl AuthType {
    pub fn as_u8(&self) -> u8 {
        unsafe { std::mem::transmute(*self) }
    }
}