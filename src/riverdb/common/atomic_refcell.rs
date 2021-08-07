use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release, AcqRel};
use std::mem::{transmute_copy};



pub struct AtomicRefCell<T>(UnsafeCell<Option<T>>);

impl<T> AtomicRefCell<T> {
    pub fn new(value: T) -> Self {
        // We could use static_assertions, but debug-only runtime assertions don't hurt compile time as much
        debug_assert!(std::mem::size_of::<Option<T>>() == std::mem::size_of::<usize>());
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

    /// Returns Some(&T) or None. Using this reference should be considered a Relaxed load.
    /// To synchronize with the Release store in store, swap, or compare_exchange, use a
    /// fence with ordering >= Acquire *immediately after* reading from this reference.
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

    #[inline]
    pub fn compare_exchange(&self, expected: Option<&T>, value: Option<T>) -> Result<Option<T>, Option<T>> {
        atomic! { Option<T>, a: &AtomicUsize = &self.0, unsafe {
            let v: usize = transmute_copy(&value);
            let expected = match expected {
                None => transmute_copy(&expected),
                Some(r) => transmute_copy(r)
            };
            return match a.compare_exchange(expected, v, AcqRel, Acquire) {
                Ok(prev) => {
                    std::mem::forget(value);
                    Ok(transmute_copy(&prev))
                },
                Err(_) => Err(value),
            };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_refcell_load() {
        let a = Arc::new(12);
        let aa = AtomicRefCell::new(a.clone());
        assert_eq!(Arc::strong_count(&a), 2);
        let a_ref = aa.load();
        assert_eq!(Arc::strong_count(&a), 2);
        assert_eq!(a_ref, Some(&a));
        assert!(!aa.is_none());
        assert!(aa.is_some());
        assert_eq!(AtomicRefCell::<u32>::default().load(), None);
    }

    #[test]
    fn test_atomic_refcell_store() {
        let a = Arc::new(12);
        let aa = AtomicRefCell::default();
        assert_eq!(aa.load(), None);
        assert!(aa.is_none());
        assert!(!aa.is_some());
        aa.store(Some(a.clone()));
        assert_eq!(aa.load(), Some(&a));
        assert!(!aa.is_none());
        assert!(aa.is_some());
        let b = Arc::new(42);
        assert_eq!(Arc::strong_count(&a), 2);
        aa.store(Some(b.clone()));
        assert_eq!(Arc::strong_count(&a), 1);
        assert_eq!(aa.load(), Some(&b));
    }

    #[test]
    fn test_atomic_refcell_swap() {
        let a = Arc::new(12);
        let b = Arc::new(42);
        let aa = AtomicRefCell::default();
        assert!(aa.is_none());
        let empty = aa.swap(Some(a.clone()));
        assert!(!aa.is_none());
        assert!(empty.is_none());

        let a_clone = aa.swap(Some(b.clone()));
        assert!(!aa.is_none());
        assert!(a_clone.is_some());
        assert_eq!(Arc::strong_count(&a), 2);
        assert_eq!(Some(a), a_clone);

        let b_clone = aa.swap(None);
        assert!(aa.is_none());
        assert!(b_clone.is_some());
        assert_eq!(Arc::strong_count(&b), 2);
        assert_eq!(Some(b), b_clone);
    }

    #[test]
    fn test_atomic_refcell_compare_exchange() {
        let a = Arc::new(12);
        let b = Arc::new(42);
        let aa = AtomicRefCell::default();
        assert!(aa.is_none());
        let empty = aa.compare_exchange(None, Some(a.clone()));
        assert!(!aa.is_none());
        assert!(empty.is_ok());
        assert!(empty.unwrap().is_none());

        {
            let a_clone = aa.compare_exchange(Some(&a), Some(b.clone()));
            assert!(!aa.is_none());
            assert!(a_clone.is_ok());
            assert_eq!(Arc::strong_count(&a), 2);
            assert_eq!(Ok(Some(a.clone())), a_clone);
        }
        assert_eq!(Arc::strong_count(&a), 1);

        let a_clone = aa.compare_exchange(None, Some(a.clone()));
        assert!(aa.is_some());
        assert!(a_clone.is_err());
        assert_eq!(Arc::strong_count(&a), 2);
        assert_eq!(Err(Some(a)), a_clone);

        let b_clone = aa.compare_exchange(Some(&b), None);
        assert!(aa.is_none());
        assert!(b_clone.is_ok());
        assert_eq!(Arc::strong_count(&b), 2);
        assert_eq!(Ok(Some(b)), b_clone);
    }
}