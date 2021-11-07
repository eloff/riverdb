use crate::riverdb::{Result, Error};

const LENGTHS: [u8; 32] = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 3, 3, 4, 0
];

const MASKS: [u8; 5] = [0x00, 0x7f, 0x1f, 0x0f, 0x07];
const MINS: [u32; 5] = [4194304, 0, 128, 2048, 65536];
const SHIFT: [u32; 5] = [0, 18, 12, 6, 0];
const SHIFT_ERR: [u32; 5] = [0, 6, 4, 2, 0];

// Decode a single code point from the utf8 stream.
// Returns (0, 0) if bytes is empty.
pub fn decode_utf8_char(bytes: &[u8]) -> Result<(char, usize)> {
    let n = bytes.len();
    if n == 0 {
        return Ok(('\0', 0)); // EOF
    }

    // We always load 4 bytes unless there aren't enough in bytes
    // That makes the code below effectively branchless.
    // Taken from: https://github.com/skeeto/branchless-utf8/blob/master/utf8.h
    let mut s1: u32 = 0;
    let mut s2: u32 = 0;
    let mut s3: u32 = 0;

    // Safety: we do the bounds checking here
    unsafe {
        // Compute these as early as possible for better pipelining
        let s0 = *bytes.get_unchecked(0) as u32;
        let len = *LENGTHS.get_unchecked((s0 >> 3) as usize) as usize;

        if n < 4 {
            if n >= 2 {
                s1 = *bytes.get_unchecked(1) as u32;
                if n == 3 {
                    s2 = *bytes.get_unchecked(2) as u32;
                }
            }
            // else n == 1
        } else {
            s1 = *bytes.get_unchecked(1) as u32;
            s2 = *bytes.get_unchecked(2) as u32;
            s3 = *bytes.get_unchecked(3) as u32;
        }

        s1 &= 0x3f;
        s2 &= 0x3f;
        s3 &= 0x3f;

        let mut c = (s0 & *MASKS.get_unchecked(len) as u32) << 18;
        c |= s1 << 12;
        c |= s2 << 6;
        c |= s3;
        c >>= SHIFT.get_unchecked(len);

        // Check for errors:
        // invalid byte sequence, non-canonical encoding, or a surrogate half.
        let mut e = ((c < *MINS.get_unchecked(len)) as u32) << 6; // non-canonical encoding
        e |= (((c >> 11) == 0x1b) as u32) << 7;  // surrogate half?
        e |= ((c > 0x10FFFF) as u32) << 8;  // out of range?
        e |= (s1 & 0xc0) >> 2;
        e |= (s2 & 0xc0) >> 4;
        e |= s3 >> 6;
        e ^= 0x2a; // top two bits of each tail byte correct?
        e >>= SHIFT_ERR.get_unchecked(len);

        if e != 0 {
            Err(Error::new("invalid utf8"))
        } else {
            // We checked error conditions above
            Ok((std::char::from_u32(c).unwrap(), len))
        }
    }
}