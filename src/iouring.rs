use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// No. of page bufs we can store into mem for write syscalls
///
/// TODO: We must take `max_value` as config from user, and it
/// should match this, so we will not have race conditions, cause
/// we must block new writes (thread sleep, etc.) if no bufs are
/// available to write into
pub(super) const PAGE_BUF_SIZE: usize = 128;

#[derive(Debug)]
struct BufPool {
    head: AtomicU64,
    next: [AtomicU32; PAGE_BUF_SIZE],
}

impl BufPool {
    const LAST_IDX: u32 = u32::MAX;

    fn new() -> Self {
        let head = AtomicU64::new(Self::pack(0, 0));
        let next = std::array::from_fn(|i| {
            AtomicU32::new(if i + 1 == PAGE_BUF_SIZE {
                Self::LAST_IDX
            } else {
                (i + 1) as u32
            })
        });

        Self { head, next }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        let (idx, _) = Self::unpack(self.head.load(Ordering::Acquire));
        idx == Self::LAST_IDX
    }

    fn pop(&self) -> Option<usize> {
        loop {
            let observed = self.head.load(Ordering::Acquire);
            let (head_idx, head_tag) = Self::unpack(observed);

            // NOTE: no empty spot left in the pool, caller must wait!
            if head_idx == Self::LAST_IDX {
                return None;
            }

            let successor = self.next[head_idx as usize].load(Ordering::Relaxed);
            let new_tag = head_tag.wrapping_add(1);
            let new_packed = Self::pack(successor, new_tag);

            match self
                .head
                .compare_exchange_weak(observed, new_packed, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(head_idx as usize),
                Err(_) => std::hint::spin_loop(),
            }
        }
    }

    fn push(&self, idx: usize) {
        // sanity check
        debug_assert!(idx < PAGE_BUF_SIZE, "idx is out of bounds");

        loop {
            let observed = self.head.load(Ordering::Acquire);
            let (head_idx, head_tag) = Self::unpack(observed);

            self.next[idx].store(head_idx, Ordering::Relaxed);

            let new_tag = head_tag.wrapping_add(1);
            let new_packed = Self::pack(idx as u32, new_tag);

            match self
                .head
                .compare_exchange_weak(observed, new_packed, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(_) => std::hint::spin_loop(),
            }
        }
    }

    #[inline(always)]
    fn pack(idx: u32, tag: u32) -> u64 {
        (tag as u64) << 32 | idx as u64
    }

    #[inline(always)]
    fn unpack(v: u64) -> (u32, u32) {
        ((v & 0xFFFF_FFFF) as u32, (v >> 32) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod buf_pool {
        use super::*;
        use std::sync::Arc;
        use std::thread;

        #[test]
        fn test_basic_push_pop() {
            let pool = BufPool::new();

            let idx = pool.pop().expect("should pop");
            assert!(idx < PAGE_BUF_SIZE);

            pool.push(idx);
            assert!(!pool.is_empty());
        }

        #[test]
        fn test_popping_till_empty() {
            let pool = BufPool::new();

            // exhausts the head ptr
            for _ in 0..PAGE_BUF_SIZE {
                assert!(pool.pop().is_some());
            }

            assert!(pool.pop().is_none());
            assert!(pool.is_empty());
        }

        #[test]
        fn test_push_pop_with_multiple_threades() {
            let pool = Arc::new(BufPool::new());

            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let pool = pool.clone();

                    thread::spawn(move || {
                        for _ in 0..1000 {
                            if let Some(idx) = pool.pop() {
                                pool.push(idx);
                            } else {
                                std::thread::yield_now();
                            }
                        }
                    })
                })
                .collect();

            for t in threads {
                t.join().unwrap();
            }

            //
            // sanity check
            //

            let mut count = 0;

            while pool.pop().is_some() {
                count += 1;
            }

            assert_eq!(count, PAGE_BUF_SIZE);
        }

        #[test]
        fn reuse_after_empty() {
            let pool = BufPool::new();

            let mut popped = Vec::new();
            let mut count = 0;

            while let Some(idx) = pool.pop() {
                popped.push(idx);
            }

            assert!(pool.is_empty());

            for idx in popped {
                pool.push(idx);
            }

            while pool.pop().is_some() {
                count += 1;
            }

            assert_eq!(count, PAGE_BUF_SIZE);
        }

        #[cfg(debug_assertions)]
        #[test]
        #[should_panic(expected = "idx is out of bounds")]
        fn push_invalid_index_panics() {
            let pool = BufPool::new();

            // should panic as the idx is out of bounds
            pool.push(PAGE_BUF_SIZE);
        }
    }
}
