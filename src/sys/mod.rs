//! Platform abstraction: local sockets, wakeups and shared-memory mappings.

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub(crate) use unix::{Listener, Map, ShmHandle, Stream, connect, create_shm, socket_pair};
#[cfg(windows)]
pub(crate) use windows::{Listener, Map, ShmHandle, Stream, connect, create_shm, socket_pair};

use crate::error::Result;

/// Map a peer-provided shared-memory handle (or name) into this process.
pub(crate) fn attach_shm(handle: Option<ShmHandle>, name: &str, len: usize) -> Result<Map> {
    #[cfg(unix)]
    {
        let _ = name;
        let handle = handle.ok_or_else(|| {
            crate::error::Error::protocol("shared-memory file descriptor was not received")
        })?;
        unix::attach_shm(handle, len)
    }
    #[cfg(windows)]
    {
        let _ = handle;
        windows::attach_named_shm(name, len)
    }
}
