use std::mem::{transmute_copy};
use std::cell::UnsafeCell;

use std::sync::atomic::Ordering::{Acquire, Release, AcqRel};


macro_rules! atomic {
    // If values of type `$t` can be transmuted into values of the primitive atomic type `$atomic`,
    // declares variable `$a` of type `$atomic` and executes `$atomic_op`.
    (@check, $t:ty, $a:ident: &$atomic:ty = $init:expr, $atomic_op:expr) => {
        if crate::riverdb::common::can_transmute::<$t, $atomic>() {
            let $a = unsafe { &*($init as *const _ as *const $atomic) };
            $atomic_op
        }
    };

    ($t:ty, $a:ident: &$atomic:ty = $init:expr, $atomic_op:expr) => {
        loop {
            atomic!(@check, $t, $a: &$atomic = $init, break $atomic_op);
            std::unimplemented!();
        }
    };

    // If values of type `$t` can be transmuted into values of a primitive atomic type, declares
    // variable `$a` of that type and executes `$atomic_op`.
    ($t:ty, $a:ident = $init:expr, $atomic_op:expr) => {
        // Safety: see assertion in AtomicCell constructor
        loop {
            atomic!(@check, $t, $a: &std::sync::atomic::AtomicUsize = $init, break $atomic_op);
            atomic!(@check, $t, $a: &std::sync::atomic::AtomicU8 = $init, break $atomic_op);
            atomic!(@check, $t, $a: &std::sync::atomic::AtomicU16 = $init, break $atomic_op);
            atomic!(@check, $t, $a: &std::sync::atomic::AtomicU32 = $init, break $atomic_op);
            atomic!(@check, $t, $a: &std::sync::atomic::AtomicU64 = $init, break $atomic_op);
            std::unimplemented!();
        }
    };
}

/// AtomicCell is an atomic version of Cell.
/// It holds a word sized type (1, 2, 4, or 8 bytes on x64) and
/// allows returning or modifying it atomically by bitwise copy.
pub struct AtomicCell<T: Copy>(UnsafeCell<T>);

impl<T: Copy> AtomicCell<T> {
    /// Construct a new AtomicCell with a copy of the passed value of type T.
    pub fn new(value: T) -> Self {
        // We could use static_assertions, but debug-only runtime assertions don't hurt compile time as much
        debug_assert!(std::mem::size_of::<T>() <= std::mem::size_of::<usize>());
        Self(UnsafeCell::new(value))
    }

    /// Return a copy of the stored T. Acquire ordering.
    #[inline]
    pub fn load(&self) -> T {
        atomic! { T, a = &self.0, unsafe {
            let r = a.load(Acquire);
            transmute_copy(&r)
        }}
    }

    /// Store a copy of the passed T. Release ordering.
    #[inline]
    pub fn store(&self, value: T) {
        atomic! { T, a = &self.0, unsafe { a.store(transmute_copy(&value), Release) } };
    }

    /// Swap the stored T with the passed T, returning a copy of what was stored.
    /// Acquire + Release ordering.
    #[inline]
    pub fn swap(&self, value: T) -> T {
        atomic! { T, a = &self.0, unsafe {
            let r = a.swap(transmute_copy(&value), AcqRel);
            transmute_copy(&r)
        }}
    }

    /// Compare and swap the stored T with the new T, if it bitwise matches current.
    /// Returns Ok(current) if it succeeded, otherwise Err(new).
    /// As with the standard library, a weak CAS may fail spuriously.
    #[inline]
    pub fn compare_exchange_weak(&self, current: T, new: T) -> Result<T, T> {
        atomic! { T, a = &self.0, unsafe {
            let r = a.compare_exchange_weak(transmute_copy(&current), transmute_copy(&new), AcqRel, Acquire);
            transmute_copy(&r)
        }}
    }

    /// Compare and swap the stored T with the new T, if it bitwise matches current.
    /// Returns Ok(current) if it succeeded, otherwise Err(new).
    #[inline]
    pub fn compare_exchange(&self, current: T, new: T) -> Result<T, T> {
        atomic! { T, a = &self.0, unsafe {
            let r = a.compare_exchange(transmute_copy(&current), transmute_copy(&new), AcqRel, Acquire);
            transmute_copy(&r)
        }}
    }
}

impl<T: Copy + Default> Default for AtomicCell<T> {
    /// Construct a new AtomicCell with T::default().
    fn default() -> Self {
        Self::new(T::default())
    }
}

// Safety: we use UnsafeCell in a thread-safe manner by transmuting it to atomic types
unsafe impl<T: Copy> Sync for AtomicCell<T> {}

