use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::mem::{transmute};

pub struct AtomicArc<T>(UnsafeCell<Option<Arc<T>>>);

impl<T> AtomicArc<T> {
    pub fn new(value: Arc<T>) -> Self {
        // We could use static_assertions, but debug-only runtime assertions don't hurt compile time as much
        debug_assert!(std::mem::size_of::<T>() <= std::mem::size_of::<usize>());
        Self(UnsafeCell::new(Some(value)))
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        atomic! { Option<Arc<T>>, a: &AtomicUsize = &self.0, a.load(Relaxed) == 0 }
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    #[inline]
    pub fn load(&self) -> Option<Arc<T>> {
        atomic! { Option<Arc<T>>, a: &AtomicUsize = &self.0,
            {
                // Make a private copy of the Arc by using unsafe by treating it as an AtomicUsize.
                if let Some(arc) = unsafe { transmute::<usize, Option<Arc<T>>>(a.load(Acquire)) } {
                    let cloned = arc.clone();
                    std::mem::forget(arc);
                    Some(cloned)
                } else {
                    None
                }
            }
        }
    }

    #[inline]
    pub fn store(&self, value: Option<Arc<T>>) {
        atomic! { Option<Arc<T>>, a: &AtomicUsize = &self.0, unsafe {
            let _existing: Option<Arc<T>> = transmute(a.load(Acquire)); // drop the existing value
            a.store(transmute(value), Release);
        }};
    }

    #[inline]
    pub fn swap(&self, value: Option<Arc<T>>) -> Option<Arc<T>> {
        atomic! { Option<Arc<T>>, a: &AtomicUsize = &self.0, unsafe { transmute(a.swap(transmute(value), AcqRel)) } }
    }

    #[inline]
    pub fn is(&self, expected: &T) -> bool {
        atomic! { Option<Arc<T>>, a: &AtomicUsize = &self.0, {
            // Make a private copy of the Arc by using unsafe by treating it as an AtomicUsize.
            if let Some(arc) = unsafe { transmute::<usize, Option<Arc<T>>>(a.load(Acquire)) } {
                let eq = arc.as_ref() as *const T == expected as *const T;
                std::mem::forget(arc);
                eq
            } else {
                false
            }
        }}
    }
}

impl<T> Default for AtomicArc<T> {
    fn default() -> Self {
        Self(UnsafeCell::new(None))
    }
}

// Safety: we use UnsafeCell in a thread-safe manner by transmuting it to atomic types
unsafe impl<T> Sync for AtomicArc<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_arc_load() {
        let a = Arc::new(12);
        let aa = AtomicArc::new(a.clone());
        assert_eq!(Arc::strong_count(&a), 2);
        let a_clone = aa.load();
        assert_eq!(Arc::strong_count(&a), 3);
        assert_eq!(a_clone, Some(a));
        assert!(!aa.is_none());
        assert!(aa.is_some());
        assert_eq!(AtomicArc::<u32>::default().load(), None);
    }

    #[test]
    fn test_atomic_arc_store() {
        let a = Arc::new(12);
        let aa = AtomicArc::default();
        assert_eq!(aa.load(), None);
        assert!(aa.is_none());
        assert!(!aa.is_some());
        aa.store(Some(a.clone()));
        assert_eq!(aa.load(), Some(a.clone()));
        assert!(!aa.is_none());
        assert!(aa.is_some());
        assert!(aa.is(a.as_ref()));
        let b = Arc::new(42);
        assert_eq!(Arc::strong_count(&a), 2);
        aa.store(Some(b.clone()));
        assert_eq!(Arc::strong_count(&a), 1);
        assert_eq!(aa.load(), Some(b.clone()));
        assert!(aa.is(b.as_ref()));
        assert!(!aa.is(a.as_ref()));
    }

    #[test]
    fn test_atomic_arc_swap() {
        let a = Arc::new(12);
        let b = Arc::new(42);
        let aa = AtomicArc::default();
        assert!(aa.is_none());
        let empty = aa.swap(Some(a.clone()));
        assert!(!aa.is_none());
        assert!(empty.is_none());
        assert!(aa.is(a.as_ref()));

        let a_clone = aa.swap(Some(b.clone()));
        assert!(!aa.is_none());
        assert!(a_clone.is_some());
        assert!(aa.is(b.as_ref()));
        assert_eq!(Arc::strong_count(&a), 2);
        assert_eq!(Some(a), a_clone);

        let b_clone = aa.swap(None);
        assert!(aa.is_none());
        assert!(b_clone.is_some());
        assert!(!aa.is(b.as_ref()));
        assert_eq!(Arc::strong_count(&b), 2);
        assert_eq!(Some(b), b_clone);
    }
}