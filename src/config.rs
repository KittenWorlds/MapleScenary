//! Channel configuration.

use crate::error::{Error, Result};

/// Default size of the shared-memory region (64 MiB).
pub const DEFAULT_REGION_LEN: usize = 64 * 1024 * 1024;
/// Default number of ring slots in each direction.
pub const DEFAULT_RING_CAPACITY: u32 = 1024;
/// Default allocation granularity of the shared-memory buddy allocator.
pub const DEFAULT_MIN_UNIT: usize = 512;
/// Default maximum payload accepted by one message (16 MiB, matching LavaCat's frame budget).
pub const DEFAULT_MAX_MESSAGE_LEN: usize = 16 * 1024 * 1024;

/// Tuning knobs negotiated by both peers.
///
/// The listening side owns the final configuration; the connecting side only
/// proposes it. Both sides validate the layout before exchanging any payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Total bytes mapped by both peers. Rounded down to a whole number of
    /// allocator units per direction. Defaults to 64 MiB.
    pub region_len: usize,
    /// Number of message slots per direction. Must be a power of two.
    pub ring_capacity: u32,
    /// Smallest buddy allocation unit. Must be a power of two.
    pub min_unit: usize,
    /// Largest single payload accepted by `send`.
    pub max_message_len: usize,
    /// Skip shared memory and send every message through the socket.
    ///
    /// Mostly useful for diagnostics and for platforms where shared memory is
    /// unavailable; normal operation uses the shared-memory data plane.
    pub force_stream: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            region_len: DEFAULT_REGION_LEN,
            ring_capacity: DEFAULT_RING_CAPACITY,
            min_unit: DEFAULT_MIN_UNIT,
            max_message_len: DEFAULT_MAX_MESSAGE_LEN,
            force_stream: false,
        }
    }
}

impl Config {
    /// A compact configuration suitable for tests and small utility channels.
    pub fn compact() -> Self {
        Self {
            region_len: 4 * 1024 * 1024,
            ring_capacity: 64,
            min_unit: 256,
            max_message_len: 1024 * 1024,
            force_stream: false,
        }
    }

    /// Validate invariants that both peers rely on.
    pub fn validate(&self) -> Result<()> {
        if self.region_len < 256 * 1024 {
            return Err(Error::invalid_config("region_len must be at least 256 KiB"));
        }
        if self.region_len > 4 * 1024 * 1024 * 1024 {
            return Err(Error::invalid_config("region_len must be at most 4 GiB"));
        }
        if !self.ring_capacity.is_power_of_two() || self.ring_capacity < 8 {
            return Err(Error::invalid_config(
                "ring_capacity must be a power of two and at least 8",
            ));
        }
        if !self.min_unit.is_power_of_two() || !(64..=1024 * 1024).contains(&self.min_unit) {
            return Err(Error::invalid_config(
                "min_unit must be a power of two between 64 B and 1 MiB",
            ));
        }
        if self.max_message_len < self.min_unit {
            return Err(Error::invalid_config(
                "max_message_len must be at least min_unit",
            ));
        }
        if self.max_message_len > self.region_len / 4 {
            return Err(Error::invalid_config(
                "max_message_len must not exceed a quarter of region_len",
            ));
        }
        Ok(())
    }
}
