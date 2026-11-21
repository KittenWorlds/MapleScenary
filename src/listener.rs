//! Listening endpoints.
//!
//! A [`Listener`] binds a name and hands out accepted [`Channel`]s. The name
//! syntax follows the platform:
//!
//! * Unix: a filesystem path (`/tmp/lavacat-1234.sock`) or, on Linux/Android,
//!   an abstract name prefixed with `@` (`@lavacat-1234`).
//! * Windows: a pipe name (`\\.\pipe\lavacat-1234`).

use std::time::Duration;

use crate::channel::{self, Channel};
use crate::config::Config;
use crate::error::Result;
use crate::sys;

/// A bound endpoint that accepts connections.
///
/// Dropping the listener releases the name. On Unix the socket file is removed
/// as well.
#[derive(Debug)]
pub struct Listener {
    inner: sys::Listener,
    config: Config,
}

impl Listener {
    /// Bind `name` using `config`.
    ///
    /// The configuration is advertised to each accepted peer and also controls
    /// the size and shape of the shared-memory region.
    pub fn bind(name: &str, config: Config) -> Result<Self> {
        config.validate()?;
        let inner = sys::Listener::bind(name, !config.force_stream)?;
        Ok(Self { inner, config })
    }

    /// Wait for the next connection and finish its handshake.
    ///
    /// `timeout` bounds both the accept and the handshake.
    pub fn accept(&self, timeout: Option<Duration>) -> Result<Channel> {
        let stream = self.inner.accept(timeout)?;
        channel::accept_stream(stream, &self.config, timeout)
    }

    /// The configuration used for accepted channels.
    pub fn config(&self) -> &Config {
        &self.config
    }
}
