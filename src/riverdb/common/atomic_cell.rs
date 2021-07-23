use std::mem::{transmute_copy};
use std::cell::UnsafeCell;

use std::sync::atomic::Ordering::{Acquire, Release, AcqRel};
use std::sync::Arc;

macro_rules! atomic {
    // If values of type `$t` can be transmuted into values of the primitive atomic type `$atomic`,
    // declares variable `$a` of type `$atomic` and executes `$atomic_op`.
    (@check, $t:ty, $a:ident: &$atomic:ty = $init:expr, $atomic_op:expr) => {
        if crate::riverdb::common::can_transmute::<$t, $atomic>() {
            let $a = unsafe { &*($init as *const _ as *const $atomic) };
            $atomic_op
        } else {
            println!("could not convert {} to {}", stringify!($t), stringify!($atomic));
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

pub struct AtomicCell<T: Copy>(UnsafeCell<T>);

impl<T: Copy> AtomicCell<T> {
    pub fn new(value: T) -> Self {
        // We could use static_assertions, but debug-only runtime assertions don't hurt compile time as much
        debug_assert!(std::mem::size_of::<T>() <= std::mem::size_of::<usize>());
        Self(UnsafeCell::new(value))
    }

    #[inline]
    pub fn load(&self) -> T {
        atomic! { T, a = &self.0, unsafe {
            let r = a.load(Acquire);
            transmute_copy(&r)
        }}
    }

    #[inline]
    pub fn store(&self, value: T) {
        atomic! { T, a = &self.0, unsafe { a.store(transmute_copy(&value), Release) } };
    }

    #[inline]
    pub fn swap(&self, value: T) -> T {
        atomic! { T, a = &self.0, unsafe {
            let r = a.swap(transmute_copy(&value), AcqRel);
            transmute_copy(&r)
        }}
    }

    #[inline]
    pub fn compare_exchange_weak(&self, current: T, new: T) -> Result<T, T> {
        atomic! { T, a = &self.0, unsafe {
            let r = a.compare_exchange_weak(transmute_copy(&current), transmute_copy(&new), AcqRel, Acquire);
            transmute_copy(&r)
        }}
    }
}

impl<T: Copy + Default> Default for AtomicCell<T> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

// Safety: we use UnsafeCell in a thread-safe manner by transmuting it to atomic types
unsafe impl<T: Copy> Sync for AtomicCell<T> {}

