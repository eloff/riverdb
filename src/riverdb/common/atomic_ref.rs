use crate::riverdb::common::atomic_cell::AtomicCell;

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