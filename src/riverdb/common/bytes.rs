use bytes::{BytesMut, BufMut};

pub unsafe fn bytes_to_slice_mut(buf: &mut BytesMut) -> &mut [u8] {
    let maybe_uninit = buf.chunk_mut();
    std::slice::from_raw_parts_mut(maybe_uninit.as_mut_ptr(), maybe_uninit.len())
}