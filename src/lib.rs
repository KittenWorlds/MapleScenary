//! MapleScenary: a cross-platform IPC middleware tuned for LavaCat.
//!
//! The crate provides bidirectional channels between two processes (or two
//! halves of one process) on Windows, macOS, Linux and Android. Every channel
//! negotiates its transport during a short handshake:
//!
//! * **Shared memory** (the default where available): payloads are placed in a
//!   mapped region split into two halves. Each side owns one half and allocates
//!   from it with a process-local buddy allocator, so the hot path contains no
//!   cross-process locks. A single-producer/single-consumer ring carries 32-byte
//!   message descriptors; a second ring returns blocks to their owner. The
//!   control socket carries one-byte doorbells and nothing else.
//! * **Stream fallback**: payloads are length-prefixed frames on the control
//!   socket. This is used when shared memory is unavailable or when
//!   [`Config::force_stream`] is set.
//!
//! # Quick start
//!
//! ```
//! use maple_scenary::{pair, Config};
//!
//! let (a, b) = pair(Config::compact()).unwrap();
//! a.send(7, b"hello").unwrap();
//! let message = b.recv(Some(std::time::Duration::from_secs(1))).unwrap();
//! assert_eq!(message.kind(), 7);
//! assert_eq!(message.bytes(), b"hello");
//! ```
//!
//! Zero-copy sends reserve a block inside the region and publish it without an
//! intermediate copy:
//!
//! ```
//! use maple_scenary::{pair, Config};
//!
//! let (a, b) = pair(Config::compact()).unwrap();
//! let mut outgoing = a.reserve(4).unwrap();
//! outgoing.buffer_mut().copy_from_slice(b"ping");
//! outgoing.publish(1).unwrap();
//! assert_eq!(b.recv(Some(std::time::Duration::from_secs(1))).unwrap().bytes(), b"ping");
//! ```
//!
//! # Platform support
//!
//! | Platform | Socket | Shared memory |
//! |----------|--------|---------------|
//! | Linux | `AF_UNIX` `SOCK_SEQPACKET` (stream fallback) | `memfd_create` |
//! | Android | abstract `AF_UNIX` `SOCK_SEQPACKET` | `memfd_create` |
//! | macOS | `AF_UNIX` `SOCK_STREAM` | `shm_open` |
//! | Windows | named pipes (message mode) | named file mapping |
//!
//! # License
//!
//! LGPL-3.0-or-later. See `LICENSE` and `COPYING`.

#![deny(unsafe_op_in_unsafe_fn)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod buddy;
mod ffi;
mod layout;
mod protocol;
mod region;
mod ring;
mod shared;
mod sys;

pub mod channel;
pub mod config;
pub mod error;
pub mod listener;
#[cfg(feature = "typed")]
pub mod typed;

pub use channel::{
    ABI_VERSION, Channel, ChannelStats, EventHandle, HANDSHAKE_TIMEOUT, Message, Outgoing, Ready,
    Receiver, Sender, Watcher, connect, pair,
};
pub use config::Config;
pub use error::{Error, ErrorKind, Result};
pub use listener::Listener;
pub use protocol::Mode;
pub use shared::{RegionView, SharedHandle, SharedRegion};
#[cfg(feature = "typed")]
pub use typed::{TypedChannel, TypedReceiver, TypedSender};
