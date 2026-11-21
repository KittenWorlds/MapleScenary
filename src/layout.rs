//! On-wire shared-memory layout.
//!
//! The first page of the region is a versioned header. Four single-producer /
//! single-consumer rings follow it: two carry message descriptors, two carry
//! "block returned" descriptors. The remaining space is split into two equal
//! halves; each side exclusively allocates from its own half, so the buddy
//! allocator needs no cross-process locks.

use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU32};

use crate::config::Config;
use crate::error::{Error, Result};

/// Eight-byte magic written at offset zero of every region.
pub const MAGIC: [u8; 8] = *b"MPLSHM01";
/// Layout revision. Peers with a different value refuse to connect.
pub const FORMAT_VERSION: u32 = 1;
/// Space reserved for [`Header`] before the rings start.
pub const HEADER_SPACE: usize = 4096;
/// Size of one message descriptor.
pub const SLOT_LEN: usize = 32;
/// Size of one return descriptor.
pub const RET_SLOT_LEN: usize = 8;

/// Descriptor published by the sender and consumed by the receiver.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Slot {
    /// Absolute offset inside the region.
    pub offset: u32,
    /// Payload length in bytes.
    pub len: u32,
    /// Caller-defined message kind.
    pub kind: u32,
    /// Reserved flags, currently always zero.
    pub flags: u32,
    /// Monotonic sequence number for diagnostics.
    pub seq: u32,
    pub _reserved0: u32,
    pub _reserved1: u32,
    pub _reserved2: u32,
}

const _: () = assert!(size_of::<Slot>() == SLOT_LEN);

impl Slot {
    pub fn new(offset: u32, len: u32, kind: u32, seq: u32) -> Self {
        Self {
            offset,
            len,
            kind,
            flags: 0,
            seq,
            _reserved0: 0,
            _reserved1: 0,
            _reserved2: 0,
        }
    }
}

/// A block handed back to its owning allocator.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetSlot {
    pub offset: u32,
    pub len: u32,
}

const _: () = assert!(size_of::<RetSlot>() == RET_SLOT_LEN);

/// Shared header. Only fixed-width integers and atomics are used so that two
/// processes built from the same version observe the same layout.
#[repr(C)]
pub struct Header {
    pub magic: [u8; 8],
    pub format_version: u32,
    pub abi_version: u32,
    pub flags: u32,
    pub _pad0: u32,
    pub region_len: u64,
    pub meta_len: u64,
    pub half_a_off: u64,
    pub half_b_off: u64,
    pub half_len: u64,
    pub max_message: u64,
    pub min_unit: u32,
    pub ring_cap: u32,
    pub ret_cap: u32,
    pub _pad1: u32,
    pub a2b_off: u64,
    pub b2a_off: u64,
    pub ra2b_off: u64,
    pub rb2a_off: u64,
    /// Client -> server message ring.
    pub a2b_head: AtomicU32,
    pub a2b_tail: AtomicU32,
    /// Server -> client message ring.
    pub b2a_head: AtomicU32,
    pub b2a_tail: AtomicU32,
    /// Server -> client returns of client-owned blocks.
    pub ra2b_head: AtomicU32,
    pub ra2b_tail: AtomicU32,
    /// Client -> server returns of server-owned blocks.
    pub rb2a_head: AtomicU32,
    pub rb2a_tail: AtomicU32,
    pub a2b_notified: AtomicBool,
    pub b2a_notified: AtomicBool,
    pub closed: AtomicBool,
    pub _reserved: [u8; 128],
}

const _: () = assert!(size_of::<Header>() <= HEADER_SPACE);

/// Direction-independent byte layout of one region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    pub region_len: usize,
    pub meta_len: usize,
    pub half_len: usize,
    pub half_a_off: usize,
    pub half_b_off: usize,
    pub a2b_off: usize,
    pub b2a_off: usize,
    pub ra2b_off: usize,
    pub rb2a_off: usize,
    pub ring_cap: u32,
    pub ret_cap: u32,
    pub min_unit: usize,
    pub max_message: usize,
}

fn align_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (value + align - 1) & !(align - 1)
}

impl Layout {
    /// Compute a layout for `cfg`, or explain why the configuration is too small.
    pub fn new(cfg: &Config) -> Result<Self> {
        cfg.validate()?;
        let ring_cap = cfg.ring_capacity;
        let ret_cap = cfg.ring_capacity;

        let a2b_off = align_up(HEADER_SPACE, 64);
        let b2a_off = a2b_off + ring_cap as usize * SLOT_LEN;
        let ra2b_off = b2a_off + ring_cap as usize * SLOT_LEN;
        let rb2a_off = ra2b_off + ret_cap as usize * RET_SLOT_LEN;
        let meta_len = align_up(rb2a_off + ret_cap as usize * RET_SLOT_LEN, 4096);

        let available = cfg.region_len.checked_sub(meta_len).ok_or_else(|| {
            Error::invalid_config("region_len is too small for the ring metadata")
        })?;
        let half_len = (available / 2) & !(cfg.min_unit - 1);
        if half_len < cfg.min_unit * 2 {
            return Err(Error::invalid_config(
                "region_len leaves no room for two allocator halves",
            ));
        }
        if cfg.max_message_len > half_len {
            return Err(Error::invalid_config(format!(
                "max_message_len {} exceeds the {} B per-direction budget",
                cfg.max_message_len, half_len
            )));
        }
        if cfg.region_len > u32::MAX as usize {
            return Err(Error::invalid_config(
                "region_len must fit in 32-bit shared-memory offsets",
            ));
        }

        Ok(Self {
            region_len: cfg.region_len,
            meta_len,
            half_len,
            half_a_off: meta_len,
            half_b_off: meta_len + half_len,
            a2b_off,
            b2a_off,
            ra2b_off,
            rb2a_off,
            ring_cap,
            ret_cap,
            min_unit: cfg.min_unit,
            max_message: cfg.max_message_len,
        })
    }

    /// Reconstruct and validate a layout from a mapped header.
    pub fn from_header(header: &Header) -> Result<Self> {
        if header.magic != MAGIC {
            return Err(Error::protocol("shared-memory magic mismatch"));
        }
        if header.format_version != FORMAT_VERSION {
            return Err(Error::protocol(format!(
                "shared-memory format {} is not supported (expected {FORMAT_VERSION})",
                header.format_version
            )));
        }
        let region_len = header.region_len as usize;
        let meta_len = header.meta_len as usize;
        let half_len = header.half_len as usize;
        let half_a_off = header.half_a_off as usize;
        let half_b_off = header.half_b_off as usize;
        let ring_cap = header.ring_cap;
        let ret_cap = header.ret_cap;
        let min_unit = header.min_unit as usize;

        if region_len > u32::MAX as usize
            || meta_len >= region_len
            || half_len == 0
            || half_a_off < meta_len
            || half_b_off != half_a_off + half_len
            || half_b_off + half_len > region_len
            || !half_len.is_multiple_of(min_unit)
            || !ring_cap.is_power_of_two()
            || !ret_cap.is_power_of_two()
            || !min_unit.is_power_of_two()
            || header.max_message as usize > half_len
        {
            return Err(Error::protocol(
                "shared-memory header is internally inconsistent",
            ));
        }

        Ok(Self {
            region_len,
            meta_len,
            half_len,
            half_a_off,
            half_b_off,
            a2b_off: header.a2b_off as usize,
            b2a_off: header.b2a_off as usize,
            ra2b_off: header.ra2b_off as usize,
            rb2a_off: header.rb2a_off as usize,
            ring_cap,
            ret_cap,
            min_unit,
            max_message: header.max_message as usize,
        })
    }
}

/// Write a fully initialized header into a zeroed region.
///
/// # Safety
/// `base` must point to a writable mapping of at least [`HEADER_SPACE`] bytes,
/// aligned for [`Header`], and no other process may access it yet.
pub(crate) unsafe fn initialize_header(base: *mut u8, layout: &Layout, abi_version: u32) {
    let header = Header {
        magic: MAGIC,
        format_version: FORMAT_VERSION,
        abi_version,
        flags: 1,
        _pad0: 0,
        region_len: layout.region_len as u64,
        meta_len: layout.meta_len as u64,
        half_a_off: layout.half_a_off as u64,
        half_b_off: layout.half_b_off as u64,
        half_len: layout.half_len as u64,
        max_message: layout.max_message as u64,
        min_unit: layout.min_unit as u32,
        ring_cap: layout.ring_cap,
        ret_cap: layout.ret_cap,
        _pad1: 0,
        a2b_off: layout.a2b_off as u64,
        b2a_off: layout.b2a_off as u64,
        ra2b_off: layout.ra2b_off as u64,
        rb2a_off: layout.rb2a_off as u64,
        a2b_head: AtomicU32::new(0),
        a2b_tail: AtomicU32::new(0),
        b2a_head: AtomicU32::new(0),
        b2a_tail: AtomicU32::new(0),
        ra2b_head: AtomicU32::new(0),
        ra2b_tail: AtomicU32::new(0),
        rb2a_head: AtomicU32::new(0),
        rb2a_tail: AtomicU32::new(0),
        a2b_notified: AtomicBool::new(false),
        b2a_notified: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        _reserved: [0; 128],
    };
    // SAFETY: caller guarantees a suitably sized and aligned writable mapping.
    unsafe { std::ptr::write(base.cast::<Header>(), header) };
    std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
}
