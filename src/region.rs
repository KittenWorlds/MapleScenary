//! Typed access to a mapped shared-memory region.

use std::sync::Arc;

use crate::layout::Header;
use crate::sys::{Map, ShmSource};

pub(crate) struct Region {
    map: Map,
    /// Kept so callers can duplicate the object or map extra read-only views.
    source: ShmSource,
}

impl Region {
    pub(crate) fn new(map: Map, source: ShmSource) -> Arc<Self> {
        Arc::new(Self { map, source })
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn source(&self) -> &ShmSource {
        &self.source
    }

    pub(crate) fn header(&self) -> &Header {
        // SAFETY: every mapping is page aligned and at least `HEADER_SPACE`
        // bytes long; the header is only written during initialization, before
        // the peer is told that the region exists. Atomics handle the fields
        // that change afterwards.
        unsafe { &*(self.map.as_ptr().cast::<Header>()) }
    }

    pub(crate) fn slice(&self, offset: usize, len: usize) -> &[u8] {
        assert!(
            offset
                .checked_add(len)
                .is_some_and(|end| end <= self.map.len()),
            "shared-memory slice out of bounds"
        );
        // SAFETY: bounds checked above; the mapping lives as long as `&self`.
        unsafe { std::slice::from_raw_parts(self.map.as_ptr().add(offset), len) }
    }

    /// Exclusive access to a block that the caller's allocator owns.
    ///
    /// # Safety
    /// The caller must own the block exclusively: no other thread or process
    /// may read or write these bytes until the returned borrow ends.
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn slice_mut(&self, offset: usize, len: usize) -> &mut [u8] {
        assert!(
            offset
                .checked_add(len)
                .is_some_and(|end| end <= self.map.len()),
            "shared-memory slice out of bounds"
        );
        // SAFETY: delegated to the caller; bounds checked above.
        unsafe { std::slice::from_raw_parts_mut(self.map.as_ptr().add(offset), len) }
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) {
        assert!(
            offset
                .checked_add(data.len())
                .is_some_and(|end| end <= self.map.len()),
            "shared-memory write out of bounds"
        );
        // SAFETY: bounds checked above; `data` cannot overlap the mapping
        // because callers pass owned buffers.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.map.as_ptr().add(offset), data.len());
        }
    }
}
