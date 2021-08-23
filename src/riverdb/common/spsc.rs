use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, Acquire, Release};

use std::mem::MaybeUninit;

use tokio::sync::Notify;


/// A fixed-size single-producer, single-consumer ring buffer using tokio::sync::Notify to wait
/// when the queue is full/empty. SIZE must be a power of two.
pub struct SpscQueue<T, const SIZE: usize> {
    producer: AtomicUsize,
    notify_producer: Notify,
    ring: MaybeUninit<[UnsafeCell<T>; SIZE]>,
    consumer: AtomicUsize,
    notify_consumer: Notify,
}

impl<T, const SIZE: usize> SpscQueue<T, SIZE> {
    const MASK: usize = SIZE - 1;

    pub fn new() -> Self {
        debug_assert!(SIZE.is_power_of_two());
        Self{
            producer: AtomicUsize::new(0),
            notify_producer: Notify::const_new(),
            ring: MaybeUninit::uninit(),
            consumer: AtomicUsize::new(0),
            notify_consumer: Notify::const_new()
        }
    }

    /// Check if queue is empty. May have changed by the time you access the result.
    pub fn is_empty(&self) -> bool {
        let cpos = self.consumer.load(Relaxed);
        let ppos = self.producer.load(Acquire);
        cpos >= ppos
    }

    /// Add a value to the queue, waiting if queue is full.
    /// Safety: While the returned reference to value is always valid, the data it
    /// points to may not be. The consumer can pop it from the queue and drop it
    /// before you access it through the returned reference. Never use the reference
    /// unless you know for certain that the consumer will not pop it from the queue.
    pub async fn put(&self, value: T) -> &T {
        loop {
            let ppos = self.producer.load(Acquire);
            let cpos = self.consumer.load(Relaxed);
            if ppos >= cpos + SIZE {
                // Queue is full
                self.notify_producer.notified().await;
                continue;
            }
            // Safety: we mask the index so it's always in range
            unsafe {
                let slot = (&*self.ring.as_ptr()).get_unchecked(ppos & Self::MASK);
                let slot_ptr = slot.get();
                slot_ptr.write(value);
                self.producer.store(ppos + 1, Release); // publish the item
                self.notify_consumer.notify_one();
                return &*slot_ptr;
            }
        }
    }

    /// Remove and return a value from the queue, waiting if queue is empty.
    pub async fn pop(&self) -> T {
        loop {
            let cpos = self.consumer.load(Relaxed);
            let ppos = self.producer.load(Acquire);
            if cpos >= ppos {
                // Queue is empty
                self.notify_consumer.notified().await;
                continue;
            }
            let result = unsafe {
                let slot = (&*self.ring.as_ptr()).get_unchecked(cpos & Self::MASK);
                slot.get().read()
            };
            self.consumer.store(cpos + 1, Release); // remove the item
            if cpos == ppos - SIZE {
                // Queue was full, we just freed a slot, wake the producer
                self.notify_producer.notify_one();
            }
            return result;
        }
    }

    /// Get a reference to the item at the front of the queue without removing it, or None.
    pub fn peek(&self) -> Option<&T> {
        let cpos = self.consumer.load(Relaxed);
        let ppos = self.producer.load(Acquire);
        if cpos >= ppos {
            return None;
        }
        // Safety: we mask the index so it's always in range
        unsafe {
            let slot = (&*self.ring.as_ptr()).get_unchecked(cpos & Self::MASK);
            return Some(&*slot.get());
        }
    }
}

// Safety: we use UnsafeCell in a thread-safe manner
unsafe impl<T, const SIZE: usize> Sync for SpscQueue<T, SIZE> {}