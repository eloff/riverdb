#[inline(always)]
pub fn fast_modulo32(i: u32, n: u32) -> u32 {
    // See https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
    ((i as u64 * n as u64) >> 32) as u32
}