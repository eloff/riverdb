use crate::riverdb::common::atomic_cell::AtomicCell;


/// Note: because this is based on AtomicCell which does not have a const constructor,
/// it doesn't work for initialization of statics. Try AtomicPtr instead.
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