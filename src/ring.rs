//! Single-producer / single-consumer rings stored inside the shared region.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::layout::{RET_SLOT_LEN, SLOT_LEN, RetSlot, Slot};
use crate::region::Region;

/// Location and capacity of one ring.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RingSpec {
    pub offset: usize,
    pub capacity: u32,
}

impl RingSpec {
    pub(crate) fn new(offset: usize, capacity: u32) -> Self {
        debug_assert!(capacity.is_power_of_two());
        Self { offset, capacity }
    }

    fn index(&self, position: u32) -> usize {
        (position & (self.capacity - 1)) as usize
    }

    fn full(&self, head: u32, tail: u32) -> bool {
        head.wrapping_sub(tail) >= self.capacity
    }
}

pub(crate) fn push_slot(
    region: &Region,
    ring: RingSpec,
    head: &AtomicU32,
    tail: &AtomicU32,
    slot: &Slot,
) -> bool {
    let position = head.load(Ordering::Relaxed);
    if ring.full(position, tail.load(Ordering::Acquire)) {
        return false;
    }
    let offset = ring.offset + ring.index(position) * SLOT_LEN;
    // SAFETY: `slot` is a plain-old-data value with the exact slot layout.
    let bytes = unsafe {
        std::slice::from_raw_parts((slot as *const Slot).cast::<u8>(), SLOT_LEN)
    };
    region.write(offset, bytes);
    head.store(position.wrapping_add(1), Ordering::Release);
    true
}

pub(crate) fn pop_slot(
    region: &Region,
    ring: RingSpec,
    head: &AtomicU32,
    tail: &AtomicU32,
) -> Option<Slot> {
    let position = tail.load(Ordering::Relaxed);
    if position == head.load(Ordering::Acquire) {
        return None;
    }
    let offset = ring.offset + ring.index(position) * SLOT_LEN;
    let bytes = region.slice(offset, SLOT_LEN);
    // SAFETY: `Slot` is plain-old-data and the slice has exactly its size.
    let slot = unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<Slot>()) };
    tail.store(position.wrapping_add(1), Ordering::Release);
    Some(slot)
}

pub(crate) fn push_return(
    region: &Region,
    ring: RingSpec,
    head: &AtomicU32,
    tail: &AtomicU32,
    offset: u32,
    len: u32,
) -> bool {
    let position = head.load(Ordering::Relaxed);
    if ring.full(position, tail.load(Ordering::Acquire)) {
        return false;
    }
    let entry = RetSlot { offset, len };
    let slot_offset = ring.offset + ring.index(position) * RET_SLOT_LEN;
    // SAFETY: `entry` is plain-old-data with the exact return-slot layout.
    let bytes = unsafe {
        std::slice::from_raw_parts((&entry as *const RetSlot).cast::<u8>(), RET_SLOT_LEN)
    };
    region.write(slot_offset, bytes);
    head.store(position.wrapping_add(1), Ordering::Release);
    true
}

pub(crate) fn pop_return(
    region: &Region,
    ring: RingSpec,
    head: &AtomicU32,
    tail: &AtomicU32,
) -> Option<RetSlot> {
    let position = tail.load(Ordering::Relaxed);
    if position == head.load(Ordering::Acquire) {
        return None;
    }
    let slot_offset = ring.offset + ring.index(position) * RET_SLOT_LEN;
    let bytes = region.slice(slot_offset, RET_SLOT_LEN);
    // SAFETY: `RetSlot` is plain-old-data and the slice has exactly its size.
    let entry = unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<RetSlot>()) };
    tail.store(position.wrapping_add(1), Ordering::Release);
    Some(entry)
}

