
use std::str::FromStr;

use crate::riverdb::common::{Result, Error};


/// A semantic version (major, minor, patch) where each component
/// can be no larger than 255.
#[derive(Eq, PartialEq, PartialOrd, Ord, Copy, Clone, Debug)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
}

impl Version {
    /// Create a new version with the given (major, minor, patch) components.
    pub fn new(major: u8, minor: u8, patch: u8) -> Self {
        Version{
            major, minor, patch
        }
    }
}

impl Default for Version {
    /// Return a new (0, 0, 0) version.
    fn default() -> Self {
        Version::new(0, 0, 0)
    }
}

impl FromStr for Version {
    type Err = Error;

    /// Parse version from a dotted string xxx[.yyy][.zzz] where
    /// xxx, yyy, and zzz are (major, minor, patch) version components.
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