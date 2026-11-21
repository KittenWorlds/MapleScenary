//! Error type shared by the Rust API and the C ABI.

use std::fmt;
use std::io;

/// Stable failure categories. The C ABI maps each of these to a negative code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Generic operating-system failure.
    Io,
    /// The supplied [`Config`](crate::Config) is not usable.
    InvalidConfig,
    /// The endpoint name is already bound.
    AddrInUse,
    /// The operating system denied the operation.
    PermissionDenied,
    /// The platform or build cannot provide this feature.
    Unsupported,
    /// The peer violated the wire protocol or the shared-memory layout.
    Protocol,
    /// The peer closed the channel.
    Disconnected,
    /// A deadline elapsed.
    Timeout,
    /// The operation would block and non-blocking mode was requested.
    WouldBlock,
    /// The payload exceeds `max_message_len`.
    MessageTooLarge,
    /// The caller-provided buffer is too small for the next message.
    BufferTooSmall,
    /// The local endpoint has been closed.
    Closed,
    /// An invariant inside the middleware failed.
    Internal,
}

impl ErrorKind {
    /// Stable numeric code used by the C ABI.
    pub const fn ffi_code(self) -> i32 {
        match self {
            Self::Io => -2,
            Self::AddrInUse => -3,
            Self::PermissionDenied => -4,
            Self::Unsupported => -5,
            Self::Protocol => -6,
            Self::Disconnected => -7,
            Self::Timeout => -8,
            Self::WouldBlock => -9,
            Self::MessageTooLarge => -10,
            Self::BufferTooSmall => -11,
            Self::Closed => -12,
            Self::Internal => -13,
            Self::InvalidConfig => -14,
        }
    }
}

/// Failure returned by every fallible MapleScenary operation.
#[derive(Debug, Clone)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    os_code: Option<i32>,
}

impl Error {
    /// Construct an error with an explicit category.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            os_code: None,
        }
    }

    /// Construct an [`ErrorKind::InvalidConfig`] error.
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidConfig, message)
    }

    /// Construct an [`ErrorKind::Protocol`] error.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Protocol, message)
    }

    /// Category of the failure.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Human readable detail.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Raw operating-system error code, when the failure came from the OS.
    pub fn os_code(&self) -> Option<i32> {
        self.os_code
    }

    /// Attach a platform error code.
    #[cfg_attr(not(windows), allow(dead_code))]
    pub(crate) fn with_os_code(mut self, code: i32) -> Self {
        self.os_code = Some(code);
        self
    }

    #[cfg_attr(not(unix), allow(dead_code))]
    pub(crate) fn io(context: impl fmt::Display, err: io::Error) -> Self {
        let kind = match err.kind() {
            io::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
            io::ErrorKind::WouldBlock => ErrorKind::WouldBlock,
            io::ErrorKind::TimedOut => ErrorKind::Timeout,
            io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::NotConnected => ErrorKind::Disconnected,
            io::ErrorKind::AddrInUse => ErrorKind::AddrInUse,
            io::ErrorKind::Unsupported => ErrorKind::Unsupported,
            _ => ErrorKind::Io,
        };
        let mut error = Self::new(kind, format!("{context}: {err}"));
        error.os_code = err.raw_os_error();
        error
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.os_code {
            Some(code) => write!(f, "{} (os error {code})", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for Error {}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, Error>;
