use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::mem::{transmute_copy};



pub struct AtomicRefCell<T>(UnsafeCell<Option<T>>);

impl<T> AtomicRefCell<T> {
    pub fn new(value: T) -> Self {
        // We could use static_assertions, but debug-only runtime assertions don't hurt compile time as much
        debug_assert!(std::mem::size_of::<T>() <= std::mem::size_of::<usize>());
        Self(UnsafeCell::new(Some(value)))
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        atomic! { Option<T>, a: &AtomicUsize = &self.0, a.load(Relaxed) == 0 }
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    #[inline]
    pub fn load(&self) -> Option<&T> {
        let r = unsafe { &*(self.0.get() as *const Option<T>) };
        r.as_ref()
    }

    #[inline]
    pub fn store(&self, value: Option<T>) {
        atomic! { Option<T>, a: &AtomicUsize = &self.0, unsafe {
            let existing: Option<T> = transmute_copy(&a.load(Acquire)); // drop the existing value
            a.store(transmute_copy(&value), Release);
        }};
        std::mem::forget(value);
    }

    #[inline]
    pub fn swap(&self, value: Option<T>) -> Option<T> {
        atomic! { Option<T>, a: &AtomicUsize = &self.0, unsafe {
            let r = transmute_copy(&a.swap(transmute_copy(&value), AcqRel));
            std::mem::forget(value);
            return r;
        }};
        unreachable!();
    }
}

impl<T> Default for AtomicRefCell<T> {
    fn default() -> Self {
        Self(UnsafeCell::new(None))
    }
}

// Safety: we use UnsafeCell in a thread-safe manner by transmuting it to atomic types
unsafe impl<T> Sync for AtomicRefCell<T> {}