use crate::riverdb::{Result, Error};

const LENGTHS: [u8; 32] = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 3, 3, 4, 0
];

const MASKS: [u8; 5] = [0x00, 0x7f, 0x1f, 0x0f, 0x07];
const MINS: [u32; 5] = [4194304, 0, 128, 2048, 65536];
const SHIFT: [u32; 5] = [0, 18, 12, 6, 0];
const SHIFT_ERR: [u32; 5] = [0, 6, 4, 2, 0];

/// Decode a single code point from the utf8 stream.
/// Returns Ok(0, 0) if bytes is empty.
/// Returns Err("invalid utf8") on error.
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

        let mut c = (s0 & *MASKS.get_unchecked(len) as u32) << 18;
        c |= (s1 & 0x3f) << 12;
        c |= (s2 & 0x3f) << 6;
        c |= s3 & 0x3f;
        c >>= SHIFT.get_unchecked(len);

        // Check for errors:
        // invalid byte sequence, non-canonical encoding, or a surrogate half.
        let mut e = ((c < *MINS.get_unchecked(len)) as u32) << 6; // non-canonical encoding
        e |= (((c >> 11) == 0x1b) as u32) << 7;  // surrogate half?
        e |= ((c > 0x10ffff) as u32) << 8;  // out of range?
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

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use crate::riverdb::common::decode_utf8_char;

    fn is_surrogate(c: i32) -> bool {
        c >= 0xd800 && c <= 0xdfff
    }

    fn utf8_encode(c: i32) -> ([u8; 4], usize) {
        let mut len = 0;
        let mut s = [0; 4];
        if c >= (1 << 16) {
            s[0] = 0xf0 |  (c >> 18) as u8;
            s[1] = 0x80 | ((c >> 12) & 0x3f) as u8;
            s[2] = 0x80 | ((c >>  6) & 0x3f) as u8;
            s[3] = 0x80 | (c & 0x3f) as u8;
            len = 4;
        } else if c >= (1 << 11) {
            s[0] = 0xe0 |  (c >> 12) as u8;
            s[1] = 0x80 | ((c >>  6) & 0x3f) as u8;
            s[2] = 0x80 | (c & 0x3f) as u8;
            len = 3;
        } else if c >= (1 << 7) {
            s[0] = 0xc0 |  (c >>  6) as u8;
            s[1] = 0x80 | (c & 0x3f) as u8;
            len = 2;
        } else {
            s[0] = c as u8;
            len = 1;
        }
        (s, len)
    }

    #[test]
    fn decode_all_utf8() {
        for i in 0..0x10ffff {
            if is_surrogate(i) {
                continue;
            }
            let (input, size) = utf8_encode(i);
            assert!(size > 0 && size <= 4);
            let utf8_s = &input[..size];
            let s = std::str::from_utf8(utf8_s).expect("utf8 encode failure");
            let res = decode_utf8_char(utf8_s);
            assert!(res.is_ok(), "could not decode {}-byte '{:?}' as {}", size, utf8_s, i);
            let (c, size) = res.unwrap();
            assert_eq!(c as i32, i);
        }
    }

    #[test]
    fn reject_out_of_range() {
        for i in 0x110000..0x1fffff {
            let (input, size) = utf8_encode(i);
            assert_eq!(size, 4);
            let utf8_s = &input[..size];
            let res = decode_utf8_char(utf8_s);
            assert!(res.is_err());
        }
    }

    #[test]
    fn reject_surrogate_halves() {
        for i in 0xd800..0xdfff {
            let (input, size) = utf8_encode(i);
            assert_eq!(size, 3);
            let utf8_s = &input[..size];
            let res = decode_utf8_char(utf8_s);
            assert!(res.is_err());
        }
    }

    #[test]
    fn reject_invalid_utf8() {
        let tests: &[&'static [u8]] = &[
            &[0xff], // invalid first byte
            &[0x80], // invalid first byte
            &[0xc0, 0x0a], // invalid second byte
            // Non-canonical encodings
            &[0xc0, 0xa4],
            &[0xe0, 0x80, 0xa4],
            &[0xf0, 0x80, 0x80, 0xa4],
        ];

        for &input in tests {
            let res = decode_utf8_char(input);
            assert!(res.is_err());
        }
    }
}