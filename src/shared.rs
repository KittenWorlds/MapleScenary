//! User-visible access to the shared-memory region behind a channel.
//!
//! The data plane maps one region into both processes and hands out blocks with
//! the `reserve` / `Message` APIs. Renderers and texture uploaders usually need
//! more than that: a way to hand the same object to another component, and a
//! read-only window at an arbitrary offset. [`SharedRegion`] exposes exactly
//! those two operations without making the raw layout public.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::region::Region;
use crate::sys::{self, Map};

/// A view of the shared-memory region backing a channel.
///
/// Obtained with [`Channel::shared_region`](crate::Channel::shared_region),
/// [`Sender::shared_region`](crate::Sender::shared_region) or
/// [`Receiver::shared_region`](crate::Receiver::shared_region). Stream-fallback
/// channels have no region, so those return `None`.
///
/// The handle keeps the object alive: it remains valid after the channel is
/// dropped, which is useful when a renderer outlives the transport.
#[derive(Clone)]
pub struct SharedRegion {
    region: Arc<Region>,
}

impl SharedRegion {
    pub(crate) fn new(region: Arc<Region>) -> Self {
        Self { region }
    }

    /// Total length of the mapping in bytes, including the header and rings.
    pub fn len(&self) -> usize {
        self.region.len()
    }

    /// Always `false`: every channel region has a header.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// Duplicate the underlying OS object.
    ///
    /// The returned handle can be sent to another thread or process (for
    /// example with `SCM_RIGHTS` on Unix or `DuplicateHandle` on Windows) and
    /// mapped there independently of this channel.
    pub fn duplicate(&self) -> Result<SharedHandle> {
        let source = self.region.source();
        let inner = source.duplicate()?;
        Ok(SharedHandle {
            inner,
            #[cfg(windows)]
            name: source.name().to_owned(),
        })
    }

    /// Map `[offset, offset + len)` read-only.
    ///
    /// The mapping is widened to the enclosing page (or the 64 KiB allocation
    /// granularity on Windows); [`RegionView::as_slice`] already accounts for
    /// that, so callers only see the requested range.
    ///
    /// The caller must not assume any synchronization: the channel publishes
    /// blocks with atomics, but arbitrary reads of bytes still being written by
    /// the peer are a data race at the application level. Use this for data the
    /// peer has already handed over (for example a published frame).
    pub fn map_read_only(&self, offset: usize, len: usize) -> Result<RegionView> {
        let total = self.len();
        let end = offset.checked_add(len).filter(|end| *end <= total);
        if len == 0 || end.is_none() {
            return Err(Error::invalid_config(format!(
                "read-only view [{offset}, +{len}) is outside the {total} B region"
            )));
        }
        let (map, delta) = self.region.source().map_read_only(offset, len)?;
        Ok(RegionView { map, delta, len })
    }
}

/// A duplicated handle to the shared-memory object.
pub struct SharedHandle {
    inner: sys::OwnedShm,
    #[cfg(windows)]
    name: String,
}

impl SharedHandle {
    /// Raw descriptor (Unix).
    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.inner.as_raw_fd()
    }

    /// Take ownership of the descriptor (Unix).
    #[cfg(unix)]
    pub fn into_fd(self) -> std::os::fd::OwnedFd {
        self.inner
    }

    /// Raw mapping handle (Windows).
    #[cfg(windows)]
    pub fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
        use std::os::windows::io::AsRawHandle;
        self.inner.as_raw_handle()
    }

    /// Take ownership of the mapping handle (Windows).
    #[cfg(windows)]
    pub fn into_handle(self) -> std::os::windows::io::OwnedHandle {
        self.inner
    }

    /// Object name (Windows), used by `OpenFileMappingW` in a child process.
    #[cfg(windows)]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A read-only window into a [`SharedRegion`].
pub struct RegionView {
    map: Map,
    delta: usize,
    len: usize,
}

impl RegionView {
    /// The requested bytes.
    pub fn as_slice(&self) -> &[u8] {
        self.map.slice(self.delta, self.len)
    }

    /// Length of the requested range, not the widened mapping.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Always `false`: zero-length views are rejected.
    pub fn is_empty(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for RegionView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionView")
            .field("len", &self.len)
            .field("delta", &self.delta)
            .finish_non_exhaustive()
    }
}
