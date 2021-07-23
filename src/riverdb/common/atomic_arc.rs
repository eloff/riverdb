use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Release, AcqRel};
use std::mem::transmute;

pub struct AtomicArc<T>(UnsafeCell<Option<Arc<T>>>);

impl<T> AtomicArc<T> {
    pub fn new(value: Arc<T>) -> Self {
        // We could use static_assertions, but debug-only runtime assertions don't hurt compile time as much
        debug_assert!(std::mem::size_of::<T>() <= std::mem::size_of::<usize>());
        Self(UnsafeCell::new(Some(value)))
    }

    #[inline]
    pub fn load(&self) -> Option<Arc<T>> {
        atomic! { T, a: &AtomicUsize = &self.0,
            {
                // Make a private copy of the Arc by using unsafe by treating it as an AtomicUsize.
                return if let Some(arc) = unsafe { transmute::<usize, Option<Arc<T>>>(a.load(Acquire)) } {
                    Some(arc.clone())
                } else {
                    None
                };
            }
        };
        unreachable!();
    }

    #[inline]
    pub fn store(&self, value: Option<Arc<T>>) {
        atomic! { T, a: &AtomicUsize = &self.0, unsafe { a.store(transmute(value), Release) } };
    }

    #[inline]
    pub fn swap(&self, value: Option<Arc<T>>) -> Option<Arc<T>> {
        atomic! { T, a: &AtomicUsize = &self.0, unsafe { return transmute(a.swap(transmute(value), AcqRel)) } };
        unreachable!();
    }

    #[inline]
    pub fn is(&self, expected: &T) -> bool {
        atomic! { T, a: &AtomicUsize = &self.0, {
            // Make a private copy of the Arc by using unsafe by treating it as an AtomicUsize.
            return if let Some(arc) = unsafe { transmute::<usize, Option<Arc<T>>>(a.load(Acquire)) } {
                arc.as_ref() as *const T == expected as *const T
            } else {
                false
            };
        }};
        unreachable!();
    }
}

impl<T> Default for AtomicArc<T> {
    fn default() -> Self {
        Self(UnsafeCell::new(None))
    }
}

// Safety: we use UnsafeCell in a thread-safe manner by transmuting it to atomic types
unsafe impl<T> Sync for AtomicArc<T> {}