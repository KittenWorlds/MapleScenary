//! Channels: handshake, zero-copy shared-memory transport and stream fallback.

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::buddy::Buddy;
use crate::config::Config;
use crate::error::{Error, ErrorKind, Result};
use crate::layout::{Header, Layout, Slot, initialize_header};
use crate::protocol::{Hello, Mode, Welcome, status};
use crate::region::Region;
use crate::ring::{self, RingSpec};
use crate::sys::{self, ShmHandle, Stream};

/// Wire/build ABI revision advertised during the handshake.
pub const ABI_VERSION: u32 = 1;
/// Default wait used while the control handshake is in flight.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(30);
const STREAM_CHUNK: usize = 64 * 1024;
/// Largest slice handed to a single socket write; keeps message-mode sockets
/// below their datagram limit when the channel fell back to stream framing.
const STREAM_WRITE_CHUNK: usize = 32 * 1024;

/// Snapshot of channel counters, useful for benchmarks and diagnostics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChannelStats {
    pub sent_messages: u64,
    pub sent_bytes: u64,
    pub recv_messages: u64,
    pub recv_bytes: u64,
    pub send_would_block: u64,
    pub allocations: u64,
    pub freed_blocks: u64,
    pub leaked_blocks: u64,
    pub wakeups_sent: u64,
    pub wakeups_received: u64,
}

#[derive(Default)]
struct Stats {
    sent_messages: AtomicU64,
    sent_bytes: AtomicU64,
    recv_messages: AtomicU64,
    recv_bytes: AtomicU64,
    send_would_block: AtomicU64,
    allocations: AtomicU64,
    freed_blocks: AtomicU64,
    leaked_blocks: AtomicU64,
    wakeups_sent: AtomicU64,
    wakeups_received: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> ChannelStats {
        ChannelStats {
            sent_messages: self.sent_messages.load(Ordering::Relaxed),
            sent_bytes: self.sent_bytes.load(Ordering::Relaxed),
            recv_messages: self.recv_messages.load(Ordering::Relaxed),
            recv_bytes: self.recv_bytes.load(Ordering::Relaxed),
            send_would_block: self.send_would_block.load(Ordering::Relaxed),
            allocations: self.allocations.load(Ordering::Relaxed),
            freed_blocks: self.freed_blocks.load(Ordering::Relaxed),
            leaked_blocks: self.leaked_blocks.load(Ordering::Relaxed),
            wakeups_sent: self.wakeups_sent.load(Ordering::Relaxed),
            wakeups_received: self.wakeups_received.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Client,
    Server,
}

impl Side {
    fn peer(self) -> Self {
        match self {
            Self::Client => Self::Server,
            Self::Server => Self::Client,
        }
    }

    fn own_half(self, layout: &Layout) -> usize {
        match self {
            Self::Client => layout.half_a_off,
            Self::Server => layout.half_b_off,
        }
    }

    fn peer_half(self, layout: &Layout) -> usize {
        self.peer().own_half(layout)
    }

    fn send_ring(self, layout: &Layout) -> RingSpec {
        match self {
            Self::Client => RingSpec::new(layout.a2b_off, layout.ring_cap),
            Self::Server => RingSpec::new(layout.b2a_off, layout.ring_cap),
        }
    }

    fn recv_ring(self, layout: &Layout) -> RingSpec {
        self.peer().send_ring(layout)
    }

    fn return_ring(self, layout: &Layout) -> RingSpec {
        match self {
            Self::Client => RingSpec::new(layout.ra2b_off, layout.ret_cap),
            Self::Server => RingSpec::new(layout.rb2a_off, layout.ret_cap),
        }
    }

    fn send_ring_atomics(self, header: &Header) -> (&AtomicU32, &AtomicU32) {
        match self {
            Self::Client => (&header.a2b_head, &header.a2b_tail),
            Self::Server => (&header.b2a_head, &header.b2a_tail),
        }
    }

    fn recv_ring_atomics(self, header: &Header) -> (&AtomicU32, &AtomicU32) {
        self.peer().send_ring_atomics(header)
    }

    fn return_atomics(self, header: &Header) -> (&AtomicU32, &AtomicU32) {
        match self {
            Self::Client => (&header.ra2b_head, &header.ra2b_tail),
            Self::Server => (&header.rb2a_head, &header.rb2a_tail),
        }
    }

    fn send_notified(self, header: &Header) -> &AtomicBool {
        match self {
            Self::Client => &header.a2b_notified,
            Self::Server => &header.b2a_notified,
        }
    }

    fn recv_notified(self, header: &Header) -> &AtomicBool {
        self.peer().send_notified(header)
    }
}

/// Control socket plus the selected data-plane mode.
struct Transport {
    stream: Stream,
    closed: AtomicBool,
}

impl Transport {
    fn new(stream: Stream) -> Self {
        Self {
            stream,
            closed: AtomicBool::new(false),
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn poison(&self) {
        self.closed.store(true, Ordering::Release);
        self.stream.shutdown();
    }

    fn close(&self) {
        self.poison();
    }

    fn wake(&self) {
        if self.stream.wake().is_err() {
            self.closed.store(true, Ordering::Release);
        }
    }

    fn wait(&self, timeout: Option<Duration>) -> Result<()> {
        match self.stream.wait_wake(timeout) {
            Ok(()) => Ok(()),
            Err(error) => {
                if error.kind() == ErrorKind::Disconnected {
                    self.closed.store(true, Ordering::Release);
                }
                Err(error)
            }
        }
    }

    fn write_frame(&self, kind: u32, payload: &[u8], timeout: Option<Duration>) -> Result<()> {
        if self.is_closed() {
            return Err(Error::new(ErrorKind::Closed, "channel is closed"));
        }
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let mut header = [0u8; 8];
        header[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[4..8].copy_from_slice(&kind.to_le_bytes());
        let result = (|| {
            self.stream.send_all(&header, remaining(deadline))?;
            for chunk in payload.chunks(STREAM_WRITE_CHUNK) {
                self.stream.send_all(chunk, remaining(deadline))?;
            }
            Ok(())
        })();
        if result.is_err() {
            // A partially written frame desynchronizes the peer, so fail closed.
            self.poison();
        }
        result
    }

    fn stream_read_some(&self, buf: &mut [u8], timeout: Option<Duration>) -> Result<usize> {
        if self.is_closed() {
            return Err(Error::new(ErrorKind::Closed, "channel is closed"));
        }
        match self.stream.read_some(buf, timeout) {
            Ok(0) => {
                self.closed.store(true, Ordering::Release);
                Err(Error::new(
                    ErrorKind::Disconnected,
                    "peer closed the channel",
                ))
            }
            Ok(n) => Ok(n),
            Err(error) => {
                if error.kind() == ErrorKind::Disconnected {
                    self.closed.store(true, Ordering::Release);
                }
                Err(error)
            }
        }
    }

    fn stream_try_read(&self, buf: &mut [u8]) -> Result<usize> {
        if self.is_closed() {
            return Err(Error::new(ErrorKind::Closed, "channel is closed"));
        }
        match self.stream.try_read(buf) {
            Ok(0) => {
                self.closed.store(true, Ordering::Release);
                Err(Error::new(
                    ErrorKind::Disconnected,
                    "peer closed the channel",
                ))
            }
            Ok(n) => Ok(n),
            Err(error) => {
                if error.kind() == ErrorKind::Disconnected {
                    self.closed.store(true, Ordering::Release);
                }
                Err(error)
            }
        }
    }
}

fn remaining(deadline: Option<Instant>) -> Option<Duration> {
    deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
}

struct ShmSender {
    region: Arc<Region>,
    layout: Layout,
    side: Side,
    buddy: RefCell<Buddy>,
    sequence: Cell<u32>,
}

impl ShmSender {
    fn drain_returns(&self, stats: &Stats) {
        let header = self.region.header();
        let (head, tail) = self.side.return_atomics(header);
        let ring = self.side.return_ring(&self.layout);
        while let Some(entry) = ring::pop_return(&self.region, ring, head, tail) {
            self.buddy
                .borrow_mut()
                .free(entry.offset, entry.len as usize);
            stats.freed_blocks.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn try_send(
        &self,
        kind: u32,
        payload: &[u8],
        transport: &Transport,
        stats: &Stats,
    ) -> Result<()> {
        if payload.len() > self.layout.max_message {
            return Err(Error::new(
                ErrorKind::MessageTooLarge,
                format!(
                    "payload of {} B exceeds max_message_len {}",
                    payload.len(),
                    self.layout.max_message
                ),
            ));
        }
        self.drain_returns(stats);
        let Some(offset) = self.buddy.borrow_mut().alloc(payload.len()) else {
            stats.send_would_block.fetch_add(1, Ordering::Relaxed);
            return Err(Error::new(
                ErrorKind::WouldBlock,
                "no shared-memory block is available (apply backpressure)",
            ));
        };
        stats.allocations.fetch_add(1, Ordering::Relaxed);
        self.region.write(offset as usize, payload);
        self.publish(offset, payload.len(), kind, transport, stats)
    }

    fn publish(
        &self,
        offset: u32,
        len: usize,
        kind: u32,
        transport: &Transport,
        stats: &Stats,
    ) -> Result<()> {
        let header = self.region.header();
        let (head, tail) = self.side.send_ring_atomics(header);
        let sequence = self.sequence.get();
        self.sequence.set(sequence.wrapping_add(1));
        let slot = Slot::new(offset, len as u32, kind, sequence);
        if !ring::push_slot(
            &self.region,
            self.side.send_ring(&self.layout),
            head,
            tail,
            &slot,
        ) {
            self.buddy.borrow_mut().free(offset, len);
            stats.send_would_block.fetch_add(1, Ordering::Relaxed);
            return Err(Error::new(
                ErrorKind::WouldBlock,
                "message ring is full (apply backpressure)",
            ));
        }
        if !self.side.send_notified(header).swap(true, Ordering::AcqRel) {
            transport.wake();
            stats.wakeups_sent.fetch_add(1, Ordering::Relaxed);
        }
        stats.sent_messages.fetch_add(1, Ordering::Relaxed);
        stats.sent_bytes.fetch_add(len as u64, Ordering::Relaxed);
        Ok(())
    }

    fn free(&self, offset: u32, len: usize) {
        self.buddy.borrow_mut().free(offset, len);
    }
}

struct ReturnQueue {
    region: Arc<Region>,
    layout: Layout,
    owner: Side,
    lock: Mutex<()>,
    stats: Arc<Stats>,
}

impl ReturnQueue {
    fn push(&self, offset: u32, len: u32) {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let header = self.region.header();
        let (head, tail) = self.owner.return_atomics(header);
        if !ring::push_return(
            &self.region,
            self.owner.return_ring(&self.layout),
            head,
            tail,
            offset,
            len,
        ) {
            self.stats.leaked_blocks.fetch_add(1, Ordering::Relaxed);
        }
        self.stats.freed_blocks.fetch_add(1, Ordering::Relaxed);
    }
}

struct ShmReceiver {
    region: Arc<Region>,
    layout: Layout,
    side: Side,
    returns: Arc<ReturnQueue>,
}

impl ShmReceiver {
    fn try_recv(&self, stats: &Stats, transport: &Transport) -> Result<Option<Message>> {
        let header = self.region.header();
        let ring = self.side.recv_ring(&self.layout);
        let (head, tail) = self.side.recv_ring_atomics(header);
        if let Some(slot) = ring::pop_slot(&self.region, ring, head, tail) {
            return self.finish(slot, stats, transport).map(Some);
        }
        // Clear the notification flag before re-checking, so a producer that
        // published in between either wrote a wakeup or is observed here.
        self.side
            .recv_notified(header)
            .store(false, Ordering::Release);
        if let Some(slot) = ring::pop_slot(&self.region, ring, head, tail) {
            return self.finish(slot, stats, transport).map(Some);
        }
        Ok(None)
    }

    fn finish(&self, slot: Slot, stats: &Stats, transport: &Transport) -> Result<Message> {
        let offset = slot.offset as usize;
        let len = slot.len as usize;
        let peer_half = self.side.peer_half(&self.layout);
        let valid = len <= self.layout.max_message
            && offset >= peer_half
            && offset
                .checked_add(len)
                .is_some_and(|end| end <= peer_half + self.layout.half_len)
            && offset.is_multiple_of(self.layout.min_unit);
        if !valid {
            transport.poison();
            return Err(Error::protocol(format!(
                "peer published an invalid block (offset {offset}, len {len})"
            )));
        }
        stats.recv_messages.fetch_add(1, Ordering::Relaxed);
        stats.recv_bytes.fetch_add(len as u64, Ordering::Relaxed);
        Ok(Message {
            kind: slot.kind,
            data: MessageData::Shm {
                region: self.region.clone(),
                offset: slot.offset,
                len: slot.len,
                returns: self.returns.clone(),
            },
        })
    }
}

struct FrameReader {
    buffer: Vec<u8>,
    start: usize,
}

impl FrameReader {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            start: 0,
        }
    }

    fn buffered(&self) -> usize {
        self.buffer.len() - self.start
    }

    fn append(&mut self, data: &[u8]) {
        if self.start > 0 {
            self.buffer.drain(..self.start);
            self.start = 0;
        }
        self.buffer.extend_from_slice(data);
    }

    fn next(&mut self, max_message: usize) -> Result<Option<(u32, Vec<u8>)>> {
        if self.buffered() < 8 {
            return Ok(None);
        }
        let len = u32::from_le_bytes(self.buffer[self.start..self.start + 4].try_into().unwrap())
            as usize;
        if len > max_message {
            return Err(Error::protocol(format!(
                "peer announced a {len} B stream frame, limit is {max_message}"
            )));
        }
        let total = 8 + len;
        if self.buffered() < total {
            return Ok(None);
        }
        let kind = u32::from_le_bytes(
            self.buffer[self.start + 4..self.start + 8]
                .try_into()
                .unwrap(),
        );
        let payload = self.buffer[self.start + 8..self.start + total].to_vec();
        self.start += total;
        if self.start == self.buffer.len() {
            self.buffer.clear();
            self.start = 0;
        }
        Ok(Some((kind, payload)))
    }
}

enum SenderBackend {
    Shm(ShmSender),
    Stream,
}

enum ReceiverBackend {
    Shm(ShmReceiver),
    Stream {
        max_message: usize,
        reader: RefCell<FrameReader>,
    },
}

/// Sending half of a channel.
///
/// `Sender` is `Send` but not `Sync`: one thread drives send operations. Use the
/// C ABI when a foreign runtime needs to call concurrently.
pub struct Sender {
    backend: SenderBackend,
    transport: Arc<Transport>,
    stats: Arc<Stats>,
    max_message: usize,
}

/// Receiving half of a channel.
///
/// As with [`Sender`], only one thread may call receive operations.
pub struct Receiver {
    backend: ReceiverBackend,
    transport: Arc<Transport>,
    stats: Arc<Stats>,
    max_message: usize,
}

impl Sender {
    /// Largest payload accepted by this channel.
    pub fn max_message_len(&self) -> usize {
        self.max_message
    }

    /// Attempt to send once. Returns [`ErrorKind::WouldBlock`] when the ring or
    /// the sender's half of shared memory is exhausted.
    pub fn try_send(&self, kind: u32, payload: &[u8]) -> Result<()> {
        match &self.backend {
            SenderBackend::Shm(sender) => {
                sender.try_send(kind, payload, &self.transport, &self.stats)
            }
            SenderBackend::Stream => {
                // Stream mode is a fallback: writes are completed (possibly
                // blocking) so a frame is never left half-published.
                self.transport
                    .write_frame(kind, payload, Some(DEFAULT_SEND_TIMEOUT))
            }
        }
    }

    /// Send, waiting up to `timeout` for capacity.
    pub fn send_timeout(&self, kind: u32, payload: &[u8], timeout: Duration) -> Result<()> {
        match &self.backend {
            SenderBackend::Stream => self.transport.write_frame(kind, payload, Some(timeout)),
            SenderBackend::Shm(sender) => {
                let deadline = Instant::now() + timeout;
                let mut backoff = Duration::from_micros(20);
                loop {
                    match sender.try_send(kind, payload, &self.transport, &self.stats) {
                        Ok(()) => return Ok(()),
                        Err(error) if error.kind() == ErrorKind::WouldBlock => {
                            let now = Instant::now();
                            if now >= deadline {
                                return Err(Error::new(
                                    ErrorKind::Timeout,
                                    "timed out waiting for shared-memory capacity",
                                ));
                            }
                            if self.transport.is_closed() {
                                return Err(Error::new(
                                    ErrorKind::Closed,
                                    "channel closed while waiting for capacity",
                                ));
                            }
                            thread::sleep(backoff.min(deadline - now));
                            backoff = (backoff * 2).min(Duration::from_millis(1));
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    /// Send with the default thirty-second deadline.
    pub fn send(&self, kind: u32, payload: &[u8]) -> Result<()> {
        self.send_timeout(kind, payload, DEFAULT_SEND_TIMEOUT)
    }

    /// Reserve a payload buffer. With shared memory the returned slice points
    /// directly into the region, so filling it and calling
    /// [`Outgoing::publish`] performs no copy.
    pub fn reserve(&self, len: usize) -> Result<Outgoing<'_>> {
        match &self.backend {
            SenderBackend::Shm(sender) => {
                sender.drain_returns(&self.stats);
                if len > sender.layout.max_message {
                    return Err(Error::new(
                        ErrorKind::MessageTooLarge,
                        "reservation exceeds max_message_len",
                    ));
                }
                let Some(offset) = sender.buddy.borrow_mut().alloc(len) else {
                    return Err(Error::new(
                        ErrorKind::WouldBlock,
                        "no shared-memory block is available",
                    ));
                };
                self.stats.allocations.fetch_add(1, Ordering::Relaxed);
                // SAFETY: the buddy allocator just handed this block to us and
                // no other code can reference it until the `Outgoing` is consumed.
                let buffer = unsafe { sender.region.slice_mut(offset as usize, len) };
                Ok(Outgoing {
                    state: OutgoingState::Shm {
                        sender,
                        transport: &self.transport,
                        stats: &self.stats,
                        offset,
                        len,
                        buffer,
                        published: false,
                    },
                })
            }
            SenderBackend::Stream => Ok(Outgoing {
                state: OutgoingState::Buffered {
                    sender: self,
                    buffer: vec![0u8; len],
                },
            }),
        }
    }

    /// Close the channel from this side. The peer observes end-of-stream.
    pub fn close(&self) {
        self.transport.close();
    }

    /// Current counters.
    pub fn stats(&self) -> ChannelStats {
        self.stats.snapshot()
    }
}

impl Receiver {
    /// Largest payload accepted by this channel.
    pub fn max_message_len(&self) -> usize {
        self.max_message
    }

    /// Non-blocking receive.
    pub fn try_recv(&self) -> Result<Option<Message>> {
        match &self.backend {
            ReceiverBackend::Shm(receiver) => receiver.try_recv(&self.stats, &self.transport),
            ReceiverBackend::Stream {
                max_message,
                reader,
            } => self.try_recv_stream(*max_message, reader),
        }
    }

    fn try_recv_stream(
        &self,
        max_message: usize,
        reader: &RefCell<FrameReader>,
    ) -> Result<Option<Message>> {
        let mut reader = reader.borrow_mut();
        if let Some((kind, payload)) = reader.next(max_message)? {
            self.stats.recv_messages.fetch_add(1, Ordering::Relaxed);
            self.stats
                .recv_bytes
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
            return Ok(Some(Message {
                kind,
                data: MessageData::Owned(payload),
            }));
        }
        let mut chunk = [0u8; STREAM_CHUNK];
        loop {
            match self.transport.stream_try_read(&mut chunk) {
                Ok(0) => {
                    return Err(Error::new(
                        ErrorKind::Disconnected,
                        "peer closed the channel",
                    ));
                }
                Ok(n) => {
                    reader.append(&chunk[..n]);
                    if let Some((kind, payload)) = reader.next(max_message)? {
                        self.stats.recv_messages.fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .recv_bytes
                            .fetch_add(payload.len() as u64, Ordering::Relaxed);
                        return Ok(Some(Message {
                            kind,
                            data: MessageData::Owned(payload),
                        }));
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(None),
                Err(error) => return Err(error),
            }
        }
    }

    /// Receive one message, waiting at most `timeout`.
    pub fn recv(&self, timeout: Option<Duration>) -> Result<Message> {
        if matches!(timeout, Some(timeout) if timeout.is_zero()) {
            return self
                .try_recv()?
                .ok_or_else(|| Error::new(ErrorKind::Timeout, "no message available"));
        }
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        match &self.backend {
            ReceiverBackend::Shm(receiver) => loop {
                if let Some(message) = receiver.try_recv(&self.stats, &self.transport)? {
                    return Ok(message);
                }
                if self.transport.is_closed() {
                    if let Some(message) = receiver.try_recv(&self.stats, &self.transport)? {
                        return Ok(message);
                    }
                    return Err(Error::new(ErrorKind::Disconnected, "channel is closed"));
                }
                let left = remaining(deadline);
                if matches!(left, Some(left) if left.is_zero()) {
                    return Err(Error::new(ErrorKind::Timeout, "receive timed out"));
                }
                match self.transport.wait(left) {
                    Ok(()) => {
                        self.stats.wakeups_received.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(error) if error.kind() == ErrorKind::Timeout => {
                        return Err(error);
                    }
                    Err(error) => {
                        if let Some(message) = receiver.try_recv(&self.stats, &self.transport)? {
                            return Ok(message);
                        }
                        return Err(error);
                    }
                }
            },
            ReceiverBackend::Stream {
                max_message,
                reader,
            } => {
                let mut chunk = [0u8; STREAM_CHUNK];
                loop {
                    {
                        let mut reader_guard = reader.borrow_mut();
                        if let Some((kind, payload)) = reader_guard.next(*max_message)? {
                            self.stats.recv_messages.fetch_add(1, Ordering::Relaxed);
                            self.stats
                                .recv_bytes
                                .fetch_add(payload.len() as u64, Ordering::Relaxed);
                            return Ok(Message {
                                kind,
                                data: MessageData::Owned(payload),
                            });
                        }
                    }
                    let left = remaining(deadline);
                    if matches!(left, Some(left) if left.is_zero()) {
                        return Err(Error::new(ErrorKind::Timeout, "receive timed out"));
                    }
                    let n = self.transport.stream_read_some(&mut chunk, left)?;
                    if n == 0 {
                        return Err(Error::new(
                            ErrorKind::Disconnected,
                            "peer closed the channel",
                        ));
                    }
                    reader.borrow_mut().append(&chunk[..n]);
                }
            }
        }
    }

    /// Close the channel from this side.
    pub fn close(&self) {
        self.transport.close();
    }

    /// Current counters.
    pub fn stats(&self) -> ChannelStats {
        self.stats.snapshot()
    }
}

enum OutgoingState<'a> {
    Shm {
        sender: &'a ShmSender,
        transport: &'a Transport,
        stats: &'a Stats,
        offset: u32,
        len: usize,
        buffer: &'a mut [u8],
        published: bool,
    },
    Buffered {
        sender: &'a Sender,
        buffer: Vec<u8>,
    },
    Empty,
}

/// A reserved payload buffer. Dropping it without [`publish`](Self::publish)
/// releases the underlying block.
pub struct Outgoing<'a> {
    state: OutgoingState<'a>,
}

impl Outgoing<'_> {
    /// Mutable view of the reserved bytes.
    pub fn buffer_mut(&mut self) -> &mut [u8] {
        match &mut self.state {
            OutgoingState::Shm { buffer, .. } => buffer,
            OutgoingState::Buffered { buffer, .. } => buffer.as_mut_slice(),
            OutgoingState::Empty => &mut [],
        }
    }

    /// Publish the reserved payload with `kind`.
    pub fn publish(mut self, kind: u32) -> Result<()> {
        let state = std::mem::replace(&mut self.state, OutgoingState::Empty);
        match state {
            OutgoingState::Shm {
                sender,
                transport,
                stats,
                offset,
                len,
                published: _,
                buffer: _,
            } => sender.publish(offset, len, kind, transport, stats),
            OutgoingState::Buffered { sender, buffer } => {
                sender.send_timeout(kind, &buffer, DEFAULT_SEND_TIMEOUT)
            }
            OutgoingState::Empty => Err(Error::new(ErrorKind::Internal, "empty reservation")),
        }
    }
}

impl Drop for Outgoing<'_> {
    fn drop(&mut self) {
        if let OutgoingState::Shm {
            sender,
            offset,
            len,
            published: false,
            ..
        } = &self.state
        {
            sender.free(*offset, *len);
        }
    }
}

enum MessageData {
    Shm {
        region: Arc<Region>,
        offset: u32,
        len: u32,
        returns: Arc<ReturnQueue>,
    },
    Owned(Vec<u8>),
}

/// One received payload. Shared-memory messages are views into the region; the
/// block is returned to the sender when the message is dropped.
pub struct Message {
    kind: u32,
    data: MessageData,
}

impl Message {
    /// Caller-defined message kind.
    pub fn kind(&self) -> u32 {
        self.kind
    }

    /// Payload length.
    pub fn len(&self) -> usize {
        match &self.data {
            MessageData::Shm { len, .. } => *len as usize,
            MessageData::Owned(bytes) => bytes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the payload. For shared-memory messages this is a zero-copy view.
    pub fn bytes(&self) -> &[u8] {
        match &self.data {
            MessageData::Shm {
                region,
                offset,
                len,
                ..
            } => region.slice(*offset as usize, *len as usize),
            MessageData::Owned(bytes) => bytes,
        }
    }

    /// Copy the payload into a fresh vector.
    pub fn to_vec(&self) -> Vec<u8> {
        self.bytes().to_vec()
    }
}

impl Drop for Message {
    fn drop(&mut self) {
        if let MessageData::Shm {
            offset,
            len,
            returns,
            ..
        } = &self.data
        {
            returns.push(*offset, *len);
        }
    }
}

/// A bidirectional channel. Split it to move sending and receiving to
/// different threads.
pub struct Channel {
    sender: Sender,
    receiver: Receiver,
    mode: Mode,
}

impl Channel {
    /// Send with the default deadline.
    pub fn send(&self, kind: u32, payload: &[u8]) -> Result<()> {
        self.sender.send(kind, payload)
    }

    /// Send with an explicit deadline.
    pub fn send_timeout(&self, kind: u32, payload: &[u8], timeout: Duration) -> Result<()> {
        self.sender.send_timeout(kind, payload, timeout)
    }

    /// Attempt to send once without waiting for capacity.
    pub fn try_send(&self, kind: u32, payload: &[u8]) -> Result<()> {
        self.sender.try_send(kind, payload)
    }

    /// Receive with an optional deadline.
    pub fn recv(&self, timeout: Option<Duration>) -> Result<Message> {
        self.receiver.recv(timeout)
    }

    /// Non-blocking receive.
    pub fn try_recv(&self) -> Result<Option<Message>> {
        self.receiver.try_recv()
    }

    /// Reserve a payload buffer for a zero-copy send.
    pub fn reserve(&self, len: usize) -> Result<Outgoing<'_>> {
        self.sender.reserve(len)
    }

    /// Largest payload accepted by this channel.
    pub fn max_message_len(&self) -> usize {
        self.sender.max_message_len()
    }

    /// Transport selected during setup.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Split into independent halves.
    pub fn split(self) -> (Sender, Receiver) {
        (self.sender, self.receiver)
    }

    /// Close both directions. Idempotent.
    pub fn close(&self) {
        self.sender.close();
    }

    /// Combined counters from both halves.
    pub fn stats(&self) -> ChannelStats {
        self.sender.stats()
    }
}

fn build_shm(role: Side, region: Arc<Region>, stream: Stream) -> Result<Channel> {
    let layout = Layout::from_header(region.header())?;
    let transport = Arc::new(Transport::new(stream));
    let stats = Arc::new(Stats::default());
    let buddy = Buddy::new(
        role.own_half(&layout) as u32,
        layout.half_len,
        layout.min_unit,
    );
    let returns = Arc::new(ReturnQueue {
        region: region.clone(),
        layout,
        owner: role.peer(),
        lock: Mutex::new(()),
        stats: stats.clone(),
    });
    Ok(Channel {
        sender: Sender {
            backend: SenderBackend::Shm(ShmSender {
                region: region.clone(),
                layout,
                side: role,
                buddy: RefCell::new(buddy),
                sequence: Cell::new(0),
            }),
            transport: transport.clone(),
            stats: stats.clone(),
            max_message: layout.max_message,
        },
        receiver: Receiver {
            backend: ReceiverBackend::Shm(ShmReceiver {
                region,
                layout,
                side: role,
                returns,
            }),
            transport,
            stats,
            max_message: layout.max_message,
        },
        mode: Mode::SharedMemory,
    })
}

fn build_stream(stream: Stream, max_message: usize) -> Channel {
    let transport = Arc::new(Transport::new(stream));
    let stats = Arc::new(Stats::default());
    Channel {
        sender: Sender {
            backend: SenderBackend::Stream,
            transport: transport.clone(),
            stats: stats.clone(),
            max_message,
        },
        receiver: Receiver {
            backend: ReceiverBackend::Stream {
                max_message,
                reader: RefCell::new(FrameReader::new()),
            },
            transport,
            stats,
            max_message,
        },
        mode: Mode::Stream,
    }
}

/// Connect to a listening endpoint.
pub fn connect(name: &str, config: Config) -> Result<Channel> {
    config.validate()?;
    let stream = sys::connect(name, !config.force_stream)?;
    stream.send_handshake(&Hello::new(&config).encode(), None, Some(HANDSHAKE_TIMEOUT))?;
    let (welcome_bytes, handle) = stream.recv_handshake(Some(HANDSHAKE_TIMEOUT))?;
    let welcome = Welcome::decode(&welcome_bytes)?;
    if welcome.status != status::OK {
        return Err(match welcome.status {
            status::VERSION => Error::new(ErrorKind::Unsupported, welcome.message),
            status::CONFIG => Error::invalid_config(welcome.message),
            status::UNAVAILABLE => Error::new(ErrorKind::Unsupported, welcome.message),
            _ => Error::protocol(welcome.message),
        });
    }
    match welcome.mode {
        Mode::SharedMemory => {
            if welcome.region_len == 0 {
                return Err(Error::protocol("server offered an empty shared region"));
            }
            if (welcome.max_message as usize) < config.max_message_len {
                return Err(Error::invalid_config(format!(
                    "server grants {} B messages, this peer requested {} B",
                    welcome.max_message, config.max_message_len
                )));
            }
            let map = sys::attach_shm(handle, &welcome.region_name, welcome.region_len as usize)?;
            build_shm(Side::Client, Region::new(map), stream)
        }
        Mode::Stream => {
            if welcome.max_message == 0 {
                return Err(Error::protocol("server offered a zero message limit"));
            }
            Ok(build_stream(stream, welcome.max_message as usize))
        }
    }
}

pub(crate) fn accept_stream(
    stream: Stream,
    config: &Config,
    timeout: Option<Duration>,
) -> Result<Channel> {
    let (hello_bytes, _) = stream.recv_handshake(timeout)?;
    let hello = match Hello::decode(&hello_bytes) {
        Ok(hello) => hello,
        Err(error) => {
            let welcome = Welcome::failure(status::VERSION, error.message());
            let _ = stream.send_handshake(&welcome.encode(), None, timeout);
            return Err(error);
        }
    };
    if hello.region_len == 0
        || hello.ring_capacity < 8
        || !hello.ring_capacity.is_power_of_two()
        || hello.min_unit < 64
        || !hello.min_unit.is_power_of_two()
        || hello.max_message == 0
    {
        let error = Error::invalid_config("peer proposed an invalid configuration");
        let welcome = Welcome::failure(status::CONFIG, error.message());
        let _ = stream.send_handshake(&welcome.encode(), None, timeout);
        return Err(error);
    }
    if config.max_message_len < hello.max_message as usize {
        let error = Error::invalid_config(format!(
            "peer needs {} B messages, listener grants {} B",
            hello.max_message, config.max_message_len
        ));
        let welcome = Welcome::failure(status::CONFIG, error.message());
        let _ = stream.send_handshake(&welcome.encode(), None, timeout);
        return Err(error);
    }

    let mut region = None;
    let mut handle: Option<ShmHandle> = None;
    if !config.force_stream
        && !hello.prefer_stream
        && let Ok(layout) = Layout::new(config)
        && let Ok((map, shm_handle)) = sys::create_shm(config.region_len)
    {
        // SAFETY: the mapping is private to this process until the handshake
        // publishes it, and it is large enough for the header.
        unsafe { initialize_header(map.as_ptr(), &layout, ABI_VERSION) };
        region = Some(Region::new(map));
        handle = Some(shm_handle);
    }

    let mode = if region.is_some() {
        Mode::SharedMemory
    } else {
        Mode::Stream
    };
    let region_name = handle
        .as_ref()
        .map(|handle| handle.name().to_owned())
        .unwrap_or_default();
    let welcome = Welcome::ok(config, mode, region_name);
    stream.send_handshake(&welcome.encode(), handle.as_ref(), timeout)?;
    drop(handle);

    match (region, mode) {
        (Some(region), Mode::SharedMemory) => build_shm(Side::Server, region, stream),
        _ => Ok(build_stream(stream, config.max_message_len)),
    }
}

/// Create a connected pair of channels without spawning a listener. Used for
/// in-process tests, benchmarks and worker pools.
pub fn pair(config: Config) -> Result<(Channel, Channel)> {
    config.validate()?;
    if !config.force_stream
        && let Ok((map, handle)) = sys::create_shm(config.region_len)
    {
        drop(handle);
        let layout = Layout::new(&config)?;
        // SAFETY: the region is local until both channels are built.
        unsafe { initialize_header(map.as_ptr(), &layout, ABI_VERSION) };
        let region = Region::new(map);
        let (first, second) = sys::socket_pair(!config.force_stream)?;
        let client = build_shm(Side::Client, region.clone(), first)?;
        let server = build_shm(Side::Server, region, second)?;
        return Ok((client, server));
    }
    let (first, second) = sys::socket_pair(false)?;
    Ok((
        build_stream(first, config.max_message_len),
        build_stream(second, config.max_message_len),
    ))
}
