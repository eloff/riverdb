use std::cmp::Ordering;

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