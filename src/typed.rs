//! Optional `serde`-powered typed channels (`typed` feature).

use std::marker::PhantomData;
use std::time::Duration;

use bincode::config::standard;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::channel::{Channel, Receiver, Sender};
use crate::error::{Error, ErrorKind, Result};

/// Reserved message kind used by typed channels for every payload.
pub const TYPED_KIND: u32 = u32::MAX;

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serde::encode_to_vec(value, standard())
        .map_err(|error| Error::new(ErrorKind::Protocol, format!("encode: {error}")))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (value, _) = bincode::serde::decode_from_slice(bytes, standard())
        .map_err(|error| Error::new(ErrorKind::Protocol, format!("decode: {error}")))?;
    Ok(value)
}

/// Typed sending half.
pub struct TypedSender<T> {
    inner: Sender,
    marker: PhantomData<fn(T)>,
}

impl<T> TypedSender<T> {
    /// Wrap a raw sender.
    pub fn new(inner: Sender) -> Self {
        Self {
            inner,
            marker: PhantomData,
        }
    }

    /// Access the underlying raw sender.
    pub fn inner(&self) -> &Sender {
        &self.inner
    }
}

impl<T: Serialize> TypedSender<T> {
    /// Serialize and send `value` with the default deadline.
    pub fn send(&self, value: &T) -> Result<()> {
        self.inner.send(TYPED_KIND, &encode(value)?)
    }

    /// Serialize and send `value`, waiting at most `timeout` for capacity.
    pub fn send_timeout(&self, value: &T, timeout: Duration) -> Result<()> {
        self.inner
            .send_timeout(TYPED_KIND, &encode(value)?, timeout)
    }
}

/// Typed receiving half.
pub struct TypedReceiver<T> {
    inner: Receiver,
    marker: PhantomData<fn() -> T>,
}

impl<T> TypedReceiver<T> {
    /// Wrap a raw receiver.
    pub fn new(inner: Receiver) -> Self {
        Self {
            inner,
            marker: PhantomData,
        }
    }

    /// Access the underlying raw receiver.
    pub fn inner(&self) -> &Receiver {
        &self.inner
    }
}

impl<T: DeserializeOwned> TypedReceiver<T> {
    /// Receive and deserialize one value.
    pub fn recv(&self, timeout: Option<Duration>) -> Result<T> {
        decode(self.inner.recv(timeout)?.bytes())
    }

    /// Non-blocking receive.
    pub fn try_recv(&self) -> Result<Option<T>> {
        match self.inner.try_recv()? {
            Some(message) => Ok(Some(decode(message.bytes())?)),
            None => Ok(None),
        }
    }
}

/// A bidirectional typed channel.
pub struct TypedChannel<T> {
    sender: TypedSender<T>,
    receiver: TypedReceiver<T>,
}

impl<T: Serialize + DeserializeOwned> TypedChannel<T> {
    /// Promote a raw channel.
    pub fn new(channel: Channel) -> Self {
        let (sender, receiver) = channel.split();
        Self {
            sender: TypedSender::new(sender),
            receiver: TypedReceiver::new(receiver),
        }
    }

    /// Send a value.
    pub fn send(&self, value: &T) -> Result<()> {
        self.sender.send(value)
    }

    /// Receive a value.
    pub fn recv(&self, timeout: Option<Duration>) -> Result<T> {
        self.receiver.recv(timeout)
    }

    /// Split into typed halves.
    pub fn split(self) -> (TypedSender<T>, TypedReceiver<T>) {
        (self.sender, self.receiver)
    }
}
