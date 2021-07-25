use std::cmp::Ordering;
use std::str::FromStr;

use crate::riverdb::common::{Result, Error};
use std::convert::TryInto;

#[derive(Eq, PartialEq, PartialOrd, Ord, Copy, Clone, Debug)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
}

impl Version {
    pub fn new(major: u8, minor: u8, patch: u8) -> Self {
        Version{
            major, minor, patch
        }
    }
}

impl Default for Version {
    fn default() -> Self {
        Version::new(0, 0, 0)
    }
}

impl FromStr for Version {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let version_str = s.split_whitespace().next().unwrap_or(s);
        let mut it = version_str.split('.');
        let major_s = it.next().unwrap_or("0");
        let minor_s = it.next().unwrap_or("0");
        let patch_s = it.next().unwrap_or("0");
        let major = major_s.parse()?;
        let minor = minor_s.parse()?;
        let patch = patch_s.parse()?;
        Ok(Self::new(major, minor, patch))
    }
}