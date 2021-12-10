use std::mem;
use std::ops::Range;


/// Range32 is a copyable equivalent to std::ops::Range<u32>.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Range32 {
    pub start: u32,
    pub end: u32,
}

impl Range32 {
    /// Returns a new (0, 0) range.
    pub const fn default() -> Self {
        Self{start: 0, end: 0}
    }

    /// Returns a new range with given start and end values.
    pub fn new(start: usize, end: usize) -> Self {
        Self{
            start: start as u32,
            end: end as u32,
        }
    }

    /// Returns self as Range<usize> (useful for slicing.)
    pub fn as_range(&self) -> Range<usize> {
        self.start as usize .. self.end as usize
    }
}

/// change_lifetime extends or shortens a lifetime via std::mem::transmute
/// # Safety
/// This is very unsafe, but it's safer than transmute because you can only
/// change the lifetime, not the type.
#[inline(always)]
pub unsafe fn change_lifetime<'a, 'b, T: ?Sized>(x: &'a T) -> &'b T {
    std::mem::transmute(x)
}

/// change_lifetime_mut extends or shortens a lifetime via std::mem::transmute
/// # Safety
/// This is very unsafe, but it's safer than transmute because you can only
/// change the lifetime, not the type.
#[inline(always)]
pub unsafe fn change_lifetime_mut<'a, 'b, T: ?Sized>(x: &'a mut T) -> &'b mut T {
    std::mem::transmute(x)
}

/// Returns `true` if values of type `A` can be transmuted into values of type `B`.
/// This means they have the same size and alignment.
pub const fn can_transmute<A, B>() -> bool {
    // Sizes must be equal, but alignment of `A` must be greater or equal than that of `B`.
    (mem::size_of::<A>() == mem::size_of::<B>()) & (mem::align_of::<A>() >= mem::align_of::<B>())
}