use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release};

/// A trait for types that implement thread-safe, shared reference counting.
/// For types that maintain internal reference counts, unlike Arc which
/// uses external reference counting. This can be more compact than an Arc,
/// which uses two usize counters internally.
pub trait AtomicRefCounted {
    /// Return the current reference count (which may have changed by the time this method returns.)
    fn refcount(&self) -> u32;
    /// Increase the shared, internal reference count.
    fn incref(&self);
    /// Decrease the shared, internal reference count. Returns true if the count after is 0.
    fn decref(&self) -> bool;
}

/// Ark is an atomic reference to any type that implements internal thread-safe, shared reference
/// counting and exposes the AtomicRefCounted trait methods. This can be more compact than an Arc
/// and allow for replacing/swapping the referenced struct atomically.
pub struct Ark<T: AtomicRefCounted> {
    ptr: AtomicPtr<T>,
    phantom: PhantomData<T>,
}

impl<T: AtomicRefCounted> Ark<T> {
    pub fn new(obj: T) -> Self {
        // No need to incref obj, it starts at ref == 1
        debug_assert_eq!(obj.refcount(), 1);
        Self {
            ptr: AtomicPtr::new(Box::leak(Box::new(obj)) as *mut T),
            phantom: PhantomData,
        }
    }

    /// Compare two Ark references for pointer equality (true if they both reference the same memory.)
    #[inline]
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        a.ptr.load(Relaxed) == b.ptr.load(Relaxed)
    }

    /// Return the pointer to T. Note this pointer remains valid only as long as the reference count
    /// is positive. Be very careful with this method.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.load(Acquire)
    }

    /// Return true if this Ark is None (doesn't point to an object.)
    #[inline]
    pub fn is_none(&self) -> bool {
        self.ptr.load(Relaxed).is_null()
    }

    /// Return true if this Ark is not None (points to an object.)
    #[inline]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    /// Construct and return an Ark pointing to obj. Does not modify reference count.
    pub unsafe fn from_raw(obj: *mut T) -> Self {
        Self{
            ptr: AtomicPtr::new(obj),
            phantom: PhantomData,
        }
    }

    /// Load the referenced object of type T. Acquire ordering.
    #[inline]
    pub fn load(&self) -> Option<&T> {
        let p = self.ptr.load(Acquire);
        if p.is_null() {
            None
        } else {
            Some(unsafe { &*p })
        }
    }

    /// Overwrite this Ark with obj. Decreasing the reference count of whatever
    /// was previously stored, if anything. Does not modify reference count of obj.
    /// Release ordering.
    #[inline]
    pub fn store(&self, obj: Self) {
        // Swap the pointers and let obj.drop cleanup the previous value
        let a = obj.ptr.load(Relaxed);
        let b = self.ptr.load(Relaxed);
        obj.ptr.store(b, Relaxed);
        self.ptr.store(a, Release);
    }

    /// Swap this Ark with obj. Returns this Ark. Does not modify reference counts.
    /// Acquire + Release ordering.
    #[inline]
    pub fn swap(&self, obj: Self) -> Self {
        let p = obj.ptr.load(Acquire);
        let r = self.ptr.swap(p, Relaxed);
        obj.ptr.store(r, Release);
        obj
    }

    /// Swap this Ark with default. Returns this Ark. Does not modify reference counts.
    /// Acquire + Release ordering.
    #[inline]
    pub fn take(&self) -> Self {
        self.swap(Self::default())
    }
}

impl<T: AtomicRefCounted> Deref for Ark<T> {
    type Target = T;

    /// Return a reference to the contained obj of type T.
    /// Note this pointer remains valid only as long as the reference count
    /// is positive. Be very careful with this method.
    fn deref(&self) -> &Self::Target {
        self.load().unwrap()
    }
}

impl<T: AtomicRefCounted> Drop for Ark<T> {
    /// Dereference the pointed to obj, dropping it if this was the last reference.
    fn drop(&mut self) {
        if let Some(obj) = self.load() {
            if obj.decref() {
                unsafe { Box::from_raw(obj as *const T as *mut T); }
            }
        }
    }
}

impl<T: AtomicRefCounted> Default for Ark<T> {
    /// Return an Ark containing None.
    fn default() -> Self {
        Self{ptr: AtomicPtr::new(std::ptr::null_mut()), phantom: PhantomData}
    }
}

impl<T: AtomicRefCounted> From<&T> for Ark<T> {
    /// Convert an AtomicRefCounted into an Ark. &T must be pinned TODO (not currently enforced)
    /// Increments the reference count of obj.
    fn from(obj: &T) -> Self {
        obj.incref();
        unsafe { Self::from_raw(obj as *const T as *mut T) }
    }
}

impl<T: AtomicRefCounted> Clone for Ark<T> {
    /// Creates another Ark pointer to the owned T. Increments the reference count.
    fn clone(&self) -> Self {
        if let Some(obj) = self.load() {
            obj.incref();
            Self{ptr: AtomicPtr::new(obj as *const T as *mut T), phantom: PhantomData}
        } else {
            Self::default()
        }
    }
}

unsafe impl<T: Sync + Send + AtomicRefCounted> Send for Ark<T> {}
unsafe impl<T: Sync + Send + AtomicRefCounted> Sync for Ark<T> {}