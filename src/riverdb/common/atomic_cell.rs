use std::sync::atomic::{AtomicU8, Ordering};
use std::marker::PhantomData;
use std::mem::ManuallyDrop;

pub union AtomicCell8<T: Copy> {
    a: ManuallyDrop<AtomicU8>,
    b: u8,
    t: T,
}

impl<T: Copy> AtomicCell8<T> {
    pub fn new(value: T) -> Self {
        assert_eq!(std::mem::size_of::<T>(), 1);
        Self{t: value}
    }

    #[inline]
    pub fn load(&self, order: Ordering) -> T {
        // Safety: see assertion in constructor
        unsafe {
            Self { b: self.a.load(order) }.t
        }
    }

    #[inline]
    pub fn store(&self, value: T, order: Ordering) {
        // Safety: see assertion in constructor
        unsafe {
            self.a.store(Self::new(value).b, order);
        }
    }

    #[inline]
    pub fn compare_exchange(&self, current: T, new: T, success: Ordering, failure: Ordering) -> Result<T, T> {
        // Safety: see assertion in constructor
        unsafe {
            self.a.compare_exchange(
                Self::new(current).b,
                Self::new(new).b,
                success,
                failure)
                .map(|b| Self { b }.t)
                .map_err(|b| Self { b }.t)
        }
    }
}

impl<T: Copy + Default> Default for AtomicCell8<T> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

