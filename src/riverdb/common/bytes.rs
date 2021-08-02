use bytes::{BytesMut, BufMut, Bytes};

pub unsafe fn bytes_to_slice_mut(buf: &mut BytesMut) -> &mut [u8] {
    let maybe_uninit = buf.chunk_mut();
    std::slice::from_raw_parts_mut(maybe_uninit.as_mut_ptr(), maybe_uninit.len())
}

struct BytesAlike {
    data: *const u8,
    len: usize,
    _1: usize,
    _2: usize,
}

/// unsplit_bytes checks if b2 follows directly after b1 in memory, and if so merges it
/// into b1 and returns b1. Otherwise returns b1 and b2 unmodified.
/// Safety: because of pointer provenance https://rust-lang.github.io/unsafe-code-guidelines/glossary.html#pointer-provenance
/// this is may invoke undefined behavior if used to merge two Bytes not orginally part of
/// the same allocation (e.g. not split from the same BytesMut or similar.)
pub unsafe fn unsplit_bytes(mut b1: Bytes, b2: Bytes) -> (Option<Bytes>, Option<Bytes>) {
    if bytes_are_contiguous(&b1, &b2) {
        assert_eq!(std::mem::size_of::<Bytes>(), std::mem::size_of::<BytesAlike>());
        // Safety: this is pretty unsafe, we assume the length is a usize stored after a pointer.
        // So we check that both the pointer and length fields appear to be where expect them
        // and if they're not, we simply treat them as if they cannot be merged.
        let p = b1.as_ptr();
        let len = b1.len();
        let bytes_ref = unsafe { &mut *(&mut b1 as *mut Bytes as *mut BytesAlike) };
        if bytes_ref.data == p && bytes_ref.len == len {
            bytes_ref.len += b2.len();
            return (Some(b1), None);
        }
    }
    (Some(b1), Some(b2))
}

/// Returns true if b2 immediately follows b1 in memory
pub fn bytes_are_contiguous(b1: &Bytes, b2: &Bytes) -> bool {
    // Safety: we don't dereference end
    let end = unsafe { b1.as_ptr().add(b1.len()) };
    end == b2.as_ptr()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;

    #[test]
    fn test_unsplit_bytes_success() {
        let s = "foobar".as_bytes();
        let mut b1 = Bytes::from(s);
        let b2 = b1.split_off(3);
        assert!(are_contiguous(&b1, &b2));
        let (r1, r2) = unsafe { unsplit_bytes(b1, b2) };
        assert!(r1.is_some());
        assert!(r2.is_none());
        assert_eq!(r1.unwrap().chunk(), s);
    }

    #[test]
    fn test_unsplit_bytes_fail() {
        let foo = "foopad".as_bytes();
        let bar = "bar".as_bytes();
        let b1 = Bytes::from(&foo[..3]);
        let b2 = Bytes::from(bar);
        assert!(!are_contiguous(&b1, &b2));
        let (r1, r2) = unsafe { unsplit_bytes(b1, b2) };
        assert_eq!(r1.is_some(), r2.is_some());
        assert_eq!(r1.unwrap().chunk(), &foo[..3]);
        assert_eq!(r2.unwrap().chunk(), bar);
    }
}