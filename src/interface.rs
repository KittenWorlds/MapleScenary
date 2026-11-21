//! Lightweight versioned interface messages over the existing channel.
//!
//! This layer supplies framing and correlation, not an RPC runtime. Applications
//! dispatch methods, negotiate supported versions and track outstanding requests.
//! Use one long-lived [`RequestIds`] per sending direction; do not reset it while
//! responses can still arrive. Responses may arrive in any order.
//!
//! The 32-byte little-endian envelope is independent of the transport handshake
//! and C ABI versions. Its minimum version describes the method/payload schema;
//! field layout and optional-field decoding remain the application's contract.

use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Error, ErrorKind, Message, Outgoing, Result, Sender};

/// Reserved outer message kind for interface envelopes (distinct from typed).
pub const INTERFACE_KIND: u32 = u32::MAX - 1;
/// Bytes preceding the application payload.
pub const HEADER_LEN: usize = 32;
/// Revision of the interface envelope, not the application schema.
pub const ENVELOPE_VERSION: u16 = 1;
const MAGIC: [u8; 4] = *b"MIFC";

/// Whether a message expects a response, answers a request, or stands alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    /// No response is expected; the request ID is zero on the wire.
    OneWay,
    /// A nonzero request ID identifies the expected response.
    Request,
    /// Carries the original request's interface, method and request ID.
    Response,
}

/// Metadata for an interface message. Constructors enforce request-ID rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    interface_id: u32,
    method: u32,
    min_version: u32,
    message_type: MessageType,
    request_id: Option<NonZeroU64>,
}

impl Header {
    /// Describe an event or a method without a response.
    pub const fn one_way(interface_id: u32, method: u32, min_version: u32) -> Self {
        Self {
            interface_id,
            method,
            min_version,
            message_type: MessageType::OneWay,
            request_id: None,
        }
    }

    /// Describe a request. IDs must be unique among this direction's requests.
    pub const fn request(
        interface_id: u32,
        method: u32,
        min_version: u32,
        request_id: NonZeroU64,
    ) -> Self {
        Self {
            interface_id,
            method,
            min_version,
            message_type: MessageType::Request,
            request_id: Some(request_id),
        }
    }

    /// Construct a response preserving the request's routing metadata.
    pub fn response(self) -> Result<Self> {
        if self.message_type != MessageType::Request {
            return Err(Error::protocol("only a request can produce a response"));
        }
        Ok(Self {
            message_type: MessageType::Response,
            ..self
        })
    }

    /// Application-defined interface identifier.
    pub const fn interface_id(self) -> u32 {
        self.interface_id
    }

    /// Application-defined method ordinal.
    pub const fn method(self) -> u32 {
        self.method
    }

    /// Minimum application interface version needed to decode this payload.
    pub const fn min_version(self) -> u32 {
        self.min_version
    }

    /// Message role.
    pub const fn message_type(self) -> MessageType {
        self.message_type
    }

    /// Correlation ID, absent only for one-way messages.
    pub const fn request_id(self) -> Option<NonZeroU64> {
        self.request_id
    }

    /// Reject another interface or a schema newer than the local implementation.
    ///
    /// Call before interpreting the application payload. This does not negotiate
    /// versions or validate individual payload fields.
    pub fn validate_for(self, interface_id: u32, supported_version: u32) -> Result<()> {
        if self.interface_id != interface_id {
            return Err(Error::protocol("interface ID mismatch"));
        }
        if self.min_version > supported_version {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "message requires a newer interface version",
            ));
        }
        Ok(())
    }

    /// Check a response against an outstanding request, including routing fields.
    ///
    /// The application must remove completed requests from its pending table to
    /// reject duplicate or unsolicited responses.
    pub fn is_response_to(self, request: Self) -> bool {
        self.message_type == MessageType::Response
            && request.message_type == MessageType::Request
            && self.request_id == request.request_id
            && self.interface_id == request.interface_id
            && self.method == request.method
            && self.min_version == request.min_version
    }

    /// Encode the fixed header; application bytes follow immediately afterward.
    pub fn encode(self) -> [u8; HEADER_LEN] {
        let mut bytes = [0; HEADER_LEN];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4..6].copy_from_slice(&ENVELOPE_VERSION.to_le_bytes());
        let role: u16 = match self.message_type {
            MessageType::OneWay => 0,
            MessageType::Request => 1,
            MessageType::Response => 2,
        };
        bytes[6..8].copy_from_slice(&role.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.interface_id.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.method.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.min_version.to_le_bytes());
        let id = self.request_id.map(NonZeroU64::get).unwrap_or(0);
        bytes[24..32].copy_from_slice(&id.to_le_bytes());
        bytes
    }

    /// Validate an envelope prefix. Trailing bytes belong to the payload.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes = bytes
            .get(..HEADER_LEN)
            .ok_or_else(|| Error::protocol("truncated interface envelope"))?;
        if bytes[..4] != MAGIC {
            return Err(Error::protocol("interface envelope magic mismatch"));
        }
        if u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != ENVELOPE_VERSION {
            return Err(Error::protocol("unsupported interface envelope version"));
        }
        let message_type = match u16::from_le_bytes(bytes[6..8].try_into().unwrap()) {
            0 => MessageType::OneWay,
            1 => MessageType::Request,
            2 => MessageType::Response,
            _ => return Err(Error::protocol("unknown interface message type")),
        };
        if bytes[20..24] != [0; 4] {
            return Err(Error::protocol("nonzero interface reserved bytes"));
        }
        let request_id = NonZeroU64::new(u64::from_le_bytes(bytes[24..32].try_into().unwrap()));
        if (message_type == MessageType::OneWay) != request_id.is_none() {
            return Err(Error::protocol("invalid interface request ID"));
        }
        Ok(Self {
            interface_id: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            method: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            min_version: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            message_type,
            request_id,
        })
    }
}

/// Monotonic IDs scoped to one sending direction; never wraps or reuses an ID.
#[derive(Debug)]
pub struct RequestIds {
    next: AtomicU64,
}

impl Default for RequestIds {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestIds {
    /// Start allocating IDs at one.
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Allocate a unique ID, failing permanently once all nonzero IDs are used.
    pub fn next_id(&self) -> Result<NonZeroU64> {
        self.next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                (next != 0).then_some(next.wrapping_add(1))
            })
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or_else(|| Error::new(ErrorKind::Internal, "request IDs exhausted"))
    }
}

/// Received envelope retaining ownership of its underlying shared-memory block.
pub struct InterfaceMessage {
    header: Header,
    inner: Message,
}

impl InterfaceMessage {
    /// Consume and validate a raw message without copying its payload.
    ///
    /// A malformed message is dropped on error; subsequent messages remain queued.
    pub fn from_message(inner: Message) -> Result<Self> {
        if inner.kind() != INTERFACE_KIND {
            return Err(Error::protocol("expected an interface message kind"));
        }
        let header = Header::decode(inner.bytes())?;
        Ok(Self { header, inner })
    }

    /// Validated envelope metadata.
    pub fn header(&self) -> Header {
        self.header
    }

    /// Borrow application bytes, excluding the envelope.
    pub fn payload(&self) -> &[u8] {
        &self.inner.bytes()[HEADER_LEN..]
    }

    /// Recover the original message, including its envelope.
    pub fn into_inner(self) -> Message {
        self.inner
    }
}

/// A reserved interface payload, returned to the allocator if unpublished.
pub struct InterfaceOutgoing<'a> {
    inner: Outgoing<'a>,
}

impl InterfaceOutgoing<'_> {
    /// Fill application bytes directly in the shared-memory reservation.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.inner.buffer_mut()[HEADER_LEN..]
    }

    /// Publish with the reserved interface message kind.
    pub fn publish(self) -> Result<()> {
        self.inner.publish(INTERFACE_KIND)
    }
}

/// Reserve an envelope plus payload, checking the transport limit first.
///
/// Capacity and publication follow [`Sender::reserve`] and [`Outgoing::publish`]
/// semantics. Shared-memory allocation may return [`ErrorKind::WouldBlock`].
/// Stream mode uses the existing buffered fallback.
pub fn reserve(sender: &Sender, header: Header, payload_len: usize) -> Result<InterfaceOutgoing<'_>> {
    let len = payload_len
        .checked_add(HEADER_LEN)
        .filter(|len| *len <= sender.max_message_len())
        .ok_or_else(|| Error::new(ErrorKind::MessageTooLarge, "interface message exceeds limit"))?;
    let mut inner = sender.reserve(len)?;
    inner.buffer_mut()[..HEADER_LEN].copy_from_slice(&header.encode());
    Ok(InterfaceOutgoing { inner })
}

/// Copy a payload into a reservation and publish it with its envelope.
pub fn send(sender: &Sender, header: Header, payload: &[u8]) -> Result<()> {
    let mut outgoing = reserve(sender, header, payload.len())?;
    outgoing.payload_mut().copy_from_slice(payload);
    outgoing.publish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_do_not_wrap() {
        let ids = RequestIds {
            next: AtomicU64::new(u64::MAX),
        };
        assert_eq!(ids.next_id().unwrap().get(), u64::MAX);
        assert_eq!(ids.next_id().unwrap_err().kind(), ErrorKind::Internal);
        assert!(ids.next_id().is_err());
    }

    #[test]
    fn ids_are_unique_across_threads() {
        let ids = std::sync::Arc::new(RequestIds::new());
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let ids = ids.clone();
                std::thread::spawn(move || {
                    (0..100).map(|_| ids.next_id().unwrap()).collect::<Vec<_>>()
                })
            })
            .collect();
        let mut values: Vec<_> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
        values.sort_unstable();
        values.dedup();
        assert_eq!(values.len(), 400);
        assert_eq!(values.first().unwrap().get(), 1);
        assert_eq!(values.last().unwrap().get(), 400);
    }

    #[test]
    fn header_layout_and_validation() {
        let header = Header::request(0x0403_0201, 9, 2, NonZeroU64::new(7).unwrap());
        let encoded = header.encode();
        assert_eq!(
            encoded,
            [
                b'M', b'I', b'F', b'C', 1, 0, 1, 0, 1, 2, 3, 4, 9, 0, 0, 0,
                2, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0,
            ]
        );
        for len in 0..HEADER_LEN {
            assert!(Header::decode(&encoded[..len]).is_err());
        }
        for (index, value) in [(0, 0), (4, 2), (6, 3), (20, 1), (24, 0)] {
            let mut bad = encoded;
            bad[index] = value;
            assert!(Header::decode(&bad).is_err(), "byte {index}");
        }
        for valid in [header, header.response().unwrap(), Header::one_way(1, 2, 0)] {
            assert_eq!(Header::decode(&valid.encode()).unwrap(), valid);
        }
        let mut bad = Header::one_way(1, 2, 0).encode();
        bad[24] = 1;
        assert!(Header::decode(&bad).is_err());
        assert!(Header::one_way(1, 2, 0).response().is_err());
        assert!(header.response().unwrap().response().is_err());
        assert!(header.validate_for(0x0403_0201, 2).is_ok());
        assert!(header.validate_for(0x0403_0201, 3).is_ok());
        assert_eq!(header.validate_for(1, 2).unwrap_err().kind(), ErrorKind::Protocol);
        assert_eq!(
            header.validate_for(0x0403_0201, 1).unwrap_err().kind(),
            ErrorKind::Unsupported
        );
    }
}
