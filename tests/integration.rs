//! End-to-end tests for the public API.

use std::time::Duration;

use maple_scenary::{Channel, Config, ErrorKind, Listener, Mode, Ready, pair};

const WAIT: Option<Duration> = Some(Duration::from_secs(5));

fn compact() -> Config {
    Config::compact()
}

#[test]
fn pair_roundtrip() {
    let (client, server) = pair(compact()).expect("pair");
    for index in 0..1_000u32 {
        let payload = format!("message-{index}");
        client.send(index, payload.as_bytes()).expect("send");
        let message = server.recv(WAIT).expect("recv");
        assert_eq!(message.kind(), index);
        assert_eq!(message.bytes(), payload.as_bytes());
    }
    assert_eq!(client.stats().sent_messages, 1_000);
    assert_eq!(server.stats().recv_messages, 1_000);
    assert_eq!(client.stats().leaked_blocks, 0);
}

#[test]
fn empty_payload_roundtrip() {
    let (client, server) = pair(compact()).expect("pair");
    client.send(9, b"").expect("send empty");
    let message = server.recv(WAIT).expect("recv");
    assert_eq!(message.kind(), 9);
    assert!(message.is_empty());
}

#[test]
fn batched_send_rings_once() {
    let (client, server) = pair(compact()).expect("pair");
    let payloads: Vec<Vec<u8>> = (0..16u8).map(|index| vec![index; 32]).collect();
    let refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
    let sent = client.send_many(11, &refs).expect("send_many");
    assert_eq!(sent, payloads.len());
    assert_eq!(client.stats().wakeups_sent, 1);
    for (index, payload) in payloads.iter().enumerate() {
        let message = server.recv(WAIT).expect("recv");
        assert_eq!(message.kind(), 11);
        assert_eq!(message.bytes(), payload.as_slice());
        assert_eq!(message.bytes()[0] as usize, index);
    }
}

#[test]
fn zero_copy_reservation() {
    let (client, server) = pair(compact()).expect("pair");
    let mut outgoing = client.reserve(4096).expect("reserve");
    for (index, byte) in outgoing.buffer_mut().iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
    outgoing.publish(3).expect("publish");

    let message = server.recv(WAIT).expect("recv");
    assert_eq!(message.kind(), 3);
    assert_eq!(message.len(), 4096);
    assert!(
        message
            .bytes()
            .iter()
            .enumerate()
            .all(|(i, b)| *b == (i % 251) as u8)
    );
}

#[test]
fn dropped_reservation_returns_block() {
    let (client, _server) = pair(compact()).expect("pair");
    let max = client.max_message_len();
    let reservation = client.reserve(max).expect("first reserve");
    drop(reservation);
    // The block was returned to the local allocator, so this must succeed.
    client.reserve(max).expect("second reserve");
}

#[test]
fn backpressure_recycles_blocks() {
    let (client, server) = pair(compact()).expect("pair");
    let max = client.max_message_len();
    let payload = vec![0x11u8; max];
    client.send(1, &payload).expect("first send");
    let blocked = client
        .try_send(1, &payload)
        .expect_err("second send must hit backpressure");
    assert_eq!(blocked.kind(), ErrorKind::WouldBlock);

    let message = server.recv(WAIT).expect("recv");
    assert_eq!(message.len(), max);
    drop(message);

    client
        .send_timeout(1, &payload, Duration::from_secs(5))
        .expect("send after the block was recycled");
}

#[test]
fn stream_fallback_large_payload() {
    let mut config = compact();
    config.force_stream = true;
    let (client, server) = pair(config).expect("pair");
    assert_eq!(client.mode(), Mode::Stream);

    let payload = vec![0x5au8; 300 * 1024];
    let reply = vec![0xa5u8; 200 * 1024];
    let server_payload = payload.clone();
    let server_reply = reply.clone();
    // The stream socket has a bounded kernel buffer, so both sides must run
    // concurrently once payloads exceed it.
    let server_thread = std::thread::spawn(move || {
        let message = server.recv(WAIT).expect("recv");
        assert_eq!(message.kind(), 4);
        assert_eq!(message.bytes(), server_payload.as_slice());
        server.send(5, &server_reply).expect("reply");
    });

    client.send(4, &payload).expect("send");
    let message = client.recv(WAIT).expect("recv reply");
    assert_eq!(message.kind(), 5);
    assert_eq!(message.bytes(), reply.as_slice());
    server_thread.join().expect("server thread");
}

fn endpoint(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!("maple-{tag}-{}.sock", std::process::id()))
        .display()
        .to_string()
}

fn exercise(listener: Listener, name: String) {
    let client = std::thread::spawn(move || {
        let channel = maple_scenary::connect(&name, compact()).expect("connect");
        channel.send(7, b"from client").expect("send");
        let reply = channel.recv(WAIT).expect("reply");
        assert_eq!(reply.bytes(), b"from server");
    });

    let server: Channel = listener.accept(WAIT).expect("accept");
    let message = server.recv(WAIT).expect("recv");
    assert_eq!(message.bytes(), b"from client");
    server.send(8, b"from server").expect("send");
    client.join().expect("client thread");
}

#[test]
fn listener_connect_shared_memory() {
    let name = endpoint("shm");
    let listener = Listener::bind(&name, compact()).expect("bind");
    exercise(listener, name);
}

#[test]
fn listener_connect_stream() {
    let name = endpoint("stream");
    let mut config = compact();
    config.force_stream = true;
    let listener = Listener::bind(&name, config).expect("bind");
    exercise(listener, name);
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[test]
fn listener_connect_abstract_socket() {
    let name = format!("@maple-test-{}", std::process::id());
    let listener = Listener::bind(&name, compact()).expect("bind abstract");
    exercise(listener, name);
}

#[cfg(feature = "typed")]
#[test]
fn typed_channel_roundtrip() {
    use maple_scenary::TypedChannel;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Job {
        id: u64,
        name: String,
        weights: Vec<u8>,
    }

    let (a, b) = pair(compact()).expect("pair");
    let a = TypedChannel::<Job>::new(a);
    let b = TypedChannel::<Job>::new(b);
    let job = Job {
        id: 42,
        name: "layout".into(),
        weights: vec![1, 2, 3],
    };
    a.send(&job).expect("send");
    assert_eq!(b.recv(WAIT).expect("recv"), job);
}

#[cfg(feature = "typed")]
#[test]
fn typed_try_recv_is_non_blocking() {
    use maple_scenary::TypedChannel;

    let (a, b) = pair(compact()).expect("pair");
    let a = TypedChannel::<u64>::new(a);
    let b = TypedChannel::<u64>::new(b);
    assert!(b.recv(Some(Duration::ZERO)).is_err());
    a.send(&7).expect("send");
    assert_eq!(b.recv(WAIT).expect("recv"), 7);
}

#[test]
fn shared_region_exposes_read_only_views() {
    let (client, _server) = pair(compact()).expect("pair");
    let region = client.shared_region().expect("shared-memory channel");
    assert!(region.len() > 4096);

    let header = region.map_read_only(0, 8).expect("map header");
    assert_eq!(header.as_slice(), b"MPLSHM01");

    // Unaligned offsets are widened to the enclosing page internally.
    let misaligned = region.map_read_only(3, 5).expect("unaligned view");
    assert_eq!(misaligned.as_slice(), &b"MPLSHM01"[3..8]);

    let error = region
        .map_read_only(region.len() - 1, 2)
        .expect_err("out of bounds");
    assert_eq!(error.kind(), ErrorKind::InvalidConfig);
    assert!(region.map_read_only(0, 0).is_err());
}

#[test]
fn shared_region_duplicate_is_a_live_handle() {
    let (client, _server) = pair(compact()).expect("pair");
    let region = client.shared_region().expect("shared-memory channel");
    let handle = region.duplicate().expect("duplicate");
    #[cfg(unix)]
    {
        assert!(handle.as_raw_fd() >= 0);
        let _fd = handle.into_fd();
    }
    #[cfg(windows)]
    {
        assert!(!handle.as_raw_handle().is_null());
        assert!(handle.name().starts_with("Local\\"));
        let _handle = handle.into_handle();
    }
}

#[test]
fn stream_fallback_has_no_shared_region() {
    let mut config = compact();
    config.force_stream = true;
    let (client, server) = pair(config).expect("pair");
    assert!(client.shared_region().is_none());
    assert!(server.shared_region().is_none());
}

#[test]
fn event_handle_is_live() {
    let (client, _server) = pair(compact()).expect("pair");
    #[cfg(unix)]
    assert!(client.event_handle() >= 0);
    #[cfg(windows)]
    assert!(!client.event_handle().is_null());
    assert_eq!(client.event_handle(), client.watcher().event_handle());
}

fn watcher_roundtrip(config: Config) {
    let (client, server) = pair(config).expect("pair");
    let watcher = server.watcher();
    assert_eq!(watcher.try_wait().expect("probe"), Ready::Timeout);

    let sender = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        client.send(21, b"wake").expect("send");
        client
    });
    assert_eq!(watcher.wait(WAIT).expect("wait"), Ready::Readable);

    let message = server.try_recv().expect("try_recv").expect("message");
    assert_eq!(message.kind(), 21);
    assert_eq!(message.bytes(), b"wake");

    let client = sender.join().expect("sender thread");
    drop(client);
    assert_eq!(watcher.wait(WAIT).expect("wait closed"), Ready::Closed);
    assert!(server.is_closed());
}

#[test]
fn watcher_wakes_shared_memory() {
    watcher_roundtrip(compact());
}

#[test]
fn watcher_wakes_stream() {
    let mut config = compact();
    config.force_stream = true;
    watcher_roundtrip(config);
}

#[test]
fn stats_report_queue_watermarks() {
    let (client, server) = pair(compact()).expect("pair");
    let (_, server_receiver) = server.split();
    assert_eq!(client.stats().queued_messages, 0);
    assert_eq!(server_receiver.stats().queued_bytes, 0);

    client.send(1, b"aaaa").expect("send");
    client.send(1, b"bb").expect("send");

    let sender = client.stats();
    assert_eq!(sender.queued_messages, 2);
    assert_eq!(sender.queued_bytes, 6);
    assert_eq!(sender.return_backlog, 0);
    let receiver = server_receiver.stats();
    assert_eq!(receiver.queued_messages, 2);
    assert_eq!(receiver.queued_bytes, 6);

    let first = server_receiver.try_recv().expect("recv").expect("message");
    let second = server_receiver.try_recv().expect("recv").expect("message");
    assert_eq!(server_receiver.stats().queued_messages, 0);
    assert_eq!(first.bytes(), b"aaaa");
    assert_eq!(second.bytes(), b"bb");
    drop(first);
    drop(second);

    // Both blocks are back on the sender's return ring until its next
    // allocation drains them.
    assert_eq!(client.stats().return_backlog, 2);
    client.send(1, b"c").expect("send");
    assert_eq!(client.stats().return_backlog, 0);
}

#[test]
fn stats_watermarks_are_zero_for_stream_fallback() {
    let mut config = compact();
    config.force_stream = true;
    let (client, server) = pair(config).expect("pair");
    let (_, receiver) = server.split();
    client.send(1, b"payload").expect("send");
    assert_eq!(client.stats().queued_messages, 0);
    assert_eq!(client.stats().queued_bytes, 0);
    assert_eq!(client.stats().return_backlog, 0);
    assert_eq!(receiver.stats().queued_bytes, 0);
}

#[test]
fn watcher_drains_before_reporting_close() {
    let (client, server) = pair(compact()).expect("pair");
    client.send(31, b"last").expect("send");
    drop(client);

    let watcher = server.watcher();
    assert_eq!(watcher.wait(WAIT).expect("wait"), Ready::Readable);
    let message = server.try_recv().expect("try_recv").expect("message");
    assert_eq!(message.bytes(), b"last");
    assert_eq!(watcher.wait(WAIT).expect("wait closed"), Ready::Closed);
}
