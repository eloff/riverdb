/// change_lifetime extends or shortens a lifetime via std::mem::transmute
/// # Safety
/// This is very unsafe, but it's safer than transmute because you can only
/// change the lifetime, not the type.
#[inline(always)]
pub unsafe fn change_lifetime<'a, 'b, T>(x: &'a T) -> &'b T {
    std::mem::transmute(x)
}

/// change_lifetime_mut extends or shortens a lifetime via std::mem::transmute
/// # Safety
/// This is very unsafe, but it's safer than transmute because you can only
/// change the lifetime, not the type.
#[inline(always)]
pub unsafe fn change_lifetime_mut<'a, 'b, T>(x: &'a mut T) -> &'b mut T {
    std::mem::transmute(x)
}