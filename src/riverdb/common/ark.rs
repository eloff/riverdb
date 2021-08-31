use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release};

pub trait AtomicRefCounted {
    fn refcount(&self) -> u32;
    fn incref(&self);
    fn decref(&self) -> bool;
}

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

    #[inline]
    pub fn ptr_eq(a: &Self, b: &Self) -> bool {
        a.ptr.load(Relaxed) == b.ptr.load(Relaxed)
    }

    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.load(Acquire)
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        self.ptr.load(Relaxed).is_null()
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    pub unsafe fn from_raw(obj: *mut T) -> Self {
        Self{
            ptr: AtomicPtr::new(obj),
            phantom: PhantomData,
        }
    }

    #[inline]
    pub fn load(&self) -> Option<&T> {
        let p = self.ptr.load(Acquire);
        if p.is_null() {
            None
        } else {
            Some(unsafe { &*p })
        }
    }

    #[inline]
    pub fn store(&self, obj: Self) {
        // Swap the pointers and let obj.drop cleanup the previous value
        let a = obj.ptr.load(Relaxed);
        let b = self.ptr.load(Relaxed);
        obj.ptr.store(b, Relaxed);
        self.ptr.store(a, Release);
    }

    #[inline]
    pub fn swap(&self, obj: Self) -> Self {
        let p = obj.ptr.load(Relaxed);
        let r = self.ptr.swap(p, Relaxed);
        obj.ptr.store(r, Release);
        obj
    }
}

impl<T: AtomicRefCounted> Deref for Ark<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.load().unwrap()
    }
}

impl<T: AtomicRefCounted> Drop for Ark<T> {
    fn drop(&mut self) {
        if let Some(obj) = self.load() {
            if obj.decref() {
                unsafe { Box::from_raw(obj as *const T as *mut T); }
            }
        }
    }
}

impl<T: AtomicRefCounted> Default for Ark<T> {
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