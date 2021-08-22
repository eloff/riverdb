use crate::riverdb::common::atomic_cell::AtomicCell;


/// Note: because this is based on AtomicCell which does not have a const constructor,
/// it doesn't work for initialization of statics. Try AtomicPtr instead.
/*
The reason the constructor can't be const is:

error[E0658]: trait bounds other than `Sized` on const fn parameters are unstable
  --> src/riverdb/common/atomic_cell.rs:41:6
   |
41 | impl<T: Copy> AtomicCell<T> {
   |      ^
   |
   = note: see issue #57563 <https://github.com/rust-lang/rust/issues/57563> for more information
 */
pub type AtomicRef<'a, T> = AtomicCell<Option<&'a T>>;

impl<'a, T> AtomicRef<'a, T> {
    #[inline]
    pub fn is_none(&self) -> bool {
        self.load().is_none()
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }
}