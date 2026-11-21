//! Process-local buddy allocator over one shared-memory half.
//!
//! Each direction owns one half of the region, so allocate/free is only ever
//! executed by one process and needs no atomics. The peer only reads payloads
//! and sends block descriptors back through the return ring.

/// Classic bitmap buddy allocator.
pub(crate) struct Buddy {
    base: u32,
    units: u32,
    min_unit: u32,
    max_order: u8,
    /// `free[order]` has one bit per block of that order; set means free.
    free: Vec<Vec<u64>>,
}

impl Buddy {
    pub(crate) fn new(base: u32, len: usize, min_unit: usize) -> Self {
        debug_assert!(min_unit.is_power_of_two());
        let units = (len / min_unit) as u32;
        debug_assert!(units > 0);
        let max_order = (31 - units.leading_zeros()) as u8;
        let mut free = Vec::with_capacity(max_order as usize + 1);
        for order in 0..=max_order {
            let blocks = units >> order;
            free.push(vec![0u64; (blocks as usize).div_ceil(64)]);
        }
        let mut buddy = Self {
            base,
            units,
            min_unit: min_unit as u32,
            max_order,
            free,
        };
        buddy.initialize();
        buddy
    }

    /// Mark the arena free using the largest naturally aligned blocks, so that
    /// arenas whose length is not a power of two still work.
    fn initialize(&mut self) {
        let mut start = 0u32;
        let mut remaining = self.units;
        while remaining > 0 {
            let alignment_order = if start == 0 {
                self.max_order
            } else {
                start.trailing_zeros().min(self.max_order as u32) as u8
            };
            let size_order = (31 - remaining.leading_zeros()) as u8;
            let order = alignment_order.min(size_order);
            self.set_free(order, start >> order);
            start += 1 << order;
            remaining -= 1 << order;
        }
    }

    fn blocks(&self, order: u8) -> u32 {
        self.units >> order
    }

    fn set_free(&mut self, order: u8, index: u32) {
        debug_assert!(index < self.blocks(order));
        let word = order as usize;
        let bit = index as usize;
        self.free[word][bit / 64] |= 1u64 << (bit % 64);
    }

    fn clear_free(&mut self, order: u8, index: u32) {
        if index >= self.blocks(order) {
            return;
        }
        let word = order as usize;
        let bit = index as usize;
        self.free[word][bit / 64] &= !(1u64 << (bit % 64));
    }

    fn is_free(&self, order: u8, index: u32) -> bool {
        if index >= self.blocks(order) {
            return false;
        }
        let word = order as usize;
        let bit = index as usize;
        self.free[word][bit / 64] & (1u64 << (bit % 64)) != 0
    }

    fn take_free(&mut self, order: u8) -> Option<u32> {
        let blocks = self.blocks(order);
        let words = self.free[order as usize].len();
        for word in 0..words {
            let value = self.free[order as usize][word];
            if value == 0 {
                continue;
            }
            let bit = value.trailing_zeros() as usize;
            let index = (word * 64 + bit) as u32;
            if index >= blocks {
                continue;
            }
            self.free[order as usize][word] &= !(1u64 << bit);
            return Some(index);
        }
        None
    }

    fn order_for(&self, len: usize) -> Option<u8> {
        let needed = (len.max(1) as u64).div_ceil(self.min_unit as u64);
        if needed > self.units as u64 {
            return None;
        }
        let mut order = 0u8;
        let mut capacity = 1u64;
        while capacity < needed {
            capacity <<= 1;
            order += 1;
        }
        (order <= self.max_order).then_some(order)
    }

    /// Allocate at least `len` bytes. Returns a region-absolute offset.
    pub(crate) fn alloc(&mut self, len: usize) -> Option<u32> {
        let order = self.order_for(len)?;
        for candidate in order..=self.max_order {
            if let Some(mut index) = self.take_free(candidate) {
                let mut current = candidate;
                while current > order {
                    current -= 1;
                    let right = (index << 1) | 1;
                    self.set_free(current, right);
                    index <<= 1;
                }
                let units = index as u64 * (1u64 << order);
                return Some(self.base + (units as u32) * self.min_unit);
            }
        }
        None
    }

    /// Return a block previously produced by [`Buddy::alloc`].
    pub(crate) fn free(&mut self, offset: u32, len: usize) {
        let Some(order) = self.order_for(len) else {
            return;
        };
        if offset < self.base {
            return;
        }
        let relative = (offset - self.base) / self.min_unit;
        if relative >= self.units {
            return;
        }
        let mut index = relative >> order;
        let mut current = order;
        loop {
            let buddy = index ^ 1;
            if current == self.max_order || !self.is_free(current, buddy) {
                self.set_free(current, index);
                return;
            }
            self.clear_free(current, buddy);
            self.clear_free(current, index);
            index >>= 1;
            current += 1;
        }
    }

    /// Bytes covered by the arena.
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.units as usize * self.min_unit as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn check_no_overlap(blocks: &BTreeMap<u32, usize>) {
        let mut previous_end = 0;
        for (&offset, &len) in blocks {
            assert!(offset as usize >= previous_end, "overlapping allocation");
            previous_end = offset as usize + len;
        }
    }

    #[test]
    fn allocates_and_coalesces() {
        let len = 32 * 1024 * 1024; // intentionally larger than a power-of-two unit count
        let mut buddy = Buddy::new(4096, len - 4096, 512);
        let mut blocks = BTreeMap::new();
        let mut rng = Rng(0x1234_5678_9abc_def0);
        for _ in 0..20_000 {
            if blocks.is_empty() || rng.next().is_multiple_of(2) {
                let len = 1 + (rng.next() % 8192) as usize;
                if let Some(offset) = buddy.alloc(len) {
                    assert_eq!(offset % 512, 0);
                    assert!(blocks.insert(offset, len).is_none());
                }
            } else {
                let index = (rng.next() as usize) % blocks.len();
                let offset = *blocks.keys().nth(index).unwrap();
                let len = blocks.remove(&offset).unwrap();
                buddy.free(offset, len);
            }
            check_no_overlap(&blocks);
        }
        for (offset, len) in blocks {
            buddy.free(offset, len);
        }
        // The arena is not a power-of-two number of units, so ask for half of it:
        // after a full sweep the largest naturally aligned block starts at base.
        let half = buddy.capacity() / 2;
        assert_eq!(buddy.alloc(half), Some(4096));
    }

    #[test]
    fn rejects_oversized_requests() {
        let mut buddy = Buddy::new(0, 1 << 20, 1024);
        assert!(buddy.alloc((1 << 20) + 1).is_none());
        assert_eq!(buddy.alloc(1 << 20), Some(0));
        assert!(buddy.alloc(1).is_none());
        buddy.free(0, 1 << 20);
        assert!(buddy.alloc(1).is_some());
    }

    #[test]
    fn splits_large_blocks() {
        let mut buddy = Buddy::new(0, 1 << 20, 1024);
        let first = buddy.alloc(1024).unwrap();
        let second = buddy.alloc(1024).unwrap();
        assert_ne!(first, second);
        assert_eq!(first, 0);
        assert_eq!(second, 1024);
    }
}
