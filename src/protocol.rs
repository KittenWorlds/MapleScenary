//! Control-plane handshake exchanged over the local socket.

use crate::config::Config;
use crate::error::{Error, Result};

/// Wire protocol revision for the control plane.
pub const PROTO_VERSION: u16 = 1;
/// ASCII "MAPL".
const MAGIC: u32 = 0x4D41_504C;

/// How payloads travel once the handshake completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Payloads live in the shared-memory rings; the socket only carries wakeups.
    SharedMemory,
    /// Payloads are framed directly on the socket (fallback mode).
    Stream,
}

/// Connecting-side proposal.
#[derive(Debug, Clone)]
pub(crate) struct Hello {
    pub version: u16,
    pub prefer_stream: bool,
    pub region_len: u64,
    pub ring_capacity: u32,
    pub min_unit: u32,
    pub max_message: u64,
}

impl Hello {
    pub(crate) fn new(config: &Config) -> Self {
        Self {
            version: PROTO_VERSION,
            prefer_stream: config.force_stream,
            region_len: config.region_len as u64,
            ring_capacity: config.ring_capacity,
            min_unit: config.min_unit as u32,
            max_message: config.max_message_len as u64,
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&(self.prefer_stream as u16).to_le_bytes());
        out.extend_from_slice(&self.region_len.to_le_bytes());
        out.extend_from_slice(&self.ring_capacity.to_le_bytes());
        out.extend_from_slice(&self.min_unit.to_le_bytes());
        out.extend_from_slice(&self.max_message.to_le_bytes());
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        if reader.u32()? != MAGIC {
            return Err(Error::protocol("handshake magic mismatch"));
        }
        let version = reader.u16()?;
        let flags = reader.u16()?;
        let hello = Self {
            version,
            prefer_stream: flags & 1 != 0,
            region_len: reader.u64()?,
            ring_capacity: reader.u32()?,
            min_unit: reader.u32()?,
            max_message: reader.u64()?,
        };
        // Unknown commands are fine, but a different major version is not.
        if hello.version != PROTO_VERSION {
            return Err(Error::protocol(format!(
                "peer speaks protocol {}, expected {PROTO_VERSION}",
                hello.version
            )));
        }
        Ok(hello)
    }
}

/// Status codes carried by [`Welcome`].
pub(crate) mod status {
    pub(crate) const OK: u16 = 0;
    pub(crate) const VERSION: u16 = 1;
    pub(crate) const CONFIG: u16 = 2;
    pub(crate) const UNAVAILABLE: u16 = 3;
}

/// Listening-side answer.
#[derive(Debug, Clone)]
pub(crate) struct Welcome {
    pub status: u16,
    pub mode: Mode,
    pub region_len: u64,
    pub max_message: u64,
    pub ring_capacity: u32,
    pub min_unit: u32,
    pub region_name: String,
    pub message: String,
}

impl Welcome {
    pub(crate) fn ok(config: &Config, mode: Mode, region_name: String) -> Self {
        Self {
            status: status::OK,
            mode,
            region_len: config.region_len as u64,
            max_message: config.max_message_len as u64,
            ring_capacity: config.ring_capacity,
            min_unit: config.min_unit as u32,
            region_name,
            message: String::new(),
        }
    }

    pub(crate) fn failure(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            mode: Mode::Stream,
            region_len: 0,
            max_message: 0,
            ring_capacity: 0,
            min_unit: 0,
            region_name: String::new(),
            message: message.into(),
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let name = self.region_name.as_bytes();
        let message = self.message.as_bytes();
        let mut out = Vec::with_capacity(32 + name.len() + message.len());
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.extend_from_slice(&PROTO_VERSION.to_le_bytes());
        out.extend_from_slice(&self.status.to_le_bytes());
        out.push(match self.mode {
            Mode::SharedMemory => 0,
            Mode::Stream => 1,
        });
        out.extend_from_slice(&[0u8; 3]);
        out.extend_from_slice(&self.region_len.to_le_bytes());
        out.extend_from_slice(&self.max_message.to_le_bytes());
        out.extend_from_slice(&self.ring_capacity.to_le_bytes());
        out.extend_from_slice(&self.min_unit.to_le_bytes());
        out.extend_from_slice(&(name.len() as u32).to_le_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&(message.len() as u32).to_le_bytes());
        out.extend_from_slice(message);
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        if reader.u32()? != MAGIC {
            return Err(Error::protocol("welcome magic mismatch"));
        }
        let version = reader.u16()?;
        if version != PROTO_VERSION {
            return Err(Error::protocol("welcome protocol version mismatch"));
        }
        let status = reader.u16()?;
        let mode = match reader.u8()? {
            0 => Mode::SharedMemory,
            1 => Mode::Stream,
            other => {
                return Err(Error::protocol(format!("unknown transport mode {other}")));
            }
        };
        reader.skip(3)?;
        let region_len = reader.u64()?;
        let max_message = reader.u64()?;
        let ring_capacity = reader.u32()?;
        let min_unit = reader.u32()?;
        let name_len = reader.u32()? as usize;
        let region_name = String::from_utf8(reader.bytes(name_len)?.to_vec())
            .map_err(|_| Error::protocol("welcome region name is not UTF-8"))?;
        let message_len = reader.u32()? as usize;
        let message = String::from_utf8(reader.bytes(message_len)?.to_vec())
            .map_err(|_| Error::protocol("welcome message is not UTF-8"))?;
        Ok(Self {
            status,
            mode,
            region_len,
            max_message,
            ring_capacity,
            min_unit,
            region_name,
            message,
        })
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| Error::protocol("handshake length overflow"))?;
        if end > self.bytes.len() {
            return Err(Error::protocol("handshake is truncated"));
        }
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        self.take(len).map(|_| ())
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn hello_rejects_every_truncated_prefix() {
        let bytes = Hello::new(&Config::compact()).encode();
        for len in 0..bytes.len() {
            assert!(
                Hello::decode(&bytes[..len]).is_err(),
                "prefix of {len} B must be rejected"
            );
        }
        assert!(Hello::decode(&bytes).is_ok());
    }

    #[test]
    fn hello_rejects_a_foreign_protocol_version() {
        let mut bytes = Hello::new(&Config::compact()).encode();
        bytes[4..6].copy_from_slice(&PROTO_VERSION.wrapping_add(1).to_le_bytes());
        let error = Hello::decode(&bytes).expect_err("version mismatch");
        assert!(error.message().contains("protocol"), "{error}");
    }

    #[test]
    fn hello_rejects_bad_magic() {
        let mut bytes = Hello::new(&Config::compact()).encode();
        bytes[0] ^= 0xFF;
        assert!(Hello::decode(&bytes).is_err());
    }

    #[test]
    fn welcome_rejects_every_truncated_prefix() {
        let bytes = Welcome::ok(&Config::compact(), Mode::SharedMemory, "region".into()).encode();
        for len in 0..bytes.len() {
            assert!(
                Welcome::decode(&bytes[..len]).is_err(),
                "prefix of {len} B must be rejected"
            );
        }
        assert!(Welcome::decode(&bytes).is_ok());
    }

    #[test]
    fn welcome_rejects_an_unknown_transport_mode() {
        let mut bytes = Welcome::ok(&Config::compact(), Mode::Stream, String::new()).encode();
        bytes[8] = 9;
        assert!(Welcome::decode(&bytes).is_err());
    }

    #[test]
    fn welcome_rejects_oversized_and_invalid_region_names() {
        let mut bytes = Welcome::ok(&Config::compact(), Mode::SharedMemory, "x".into()).encode();
        // name_len sits at offset 36; a huge value must not trigger a panic or
        // an out-of-bounds read.
        bytes[36..40].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(Welcome::decode(&bytes).is_err());

        let mut bytes = Welcome::ok(&Config::compact(), Mode::SharedMemory, "x".into()).encode();
        // The single name byte at offset 40 is replaced with an invalid start
        // byte, so UTF-8 validation has to reject it.
        bytes[40] = 0xFF;
        assert!(Welcome::decode(&bytes).is_err());
    }
}
