//! End-to-end tests for the public API.

use std::time::Duration;

use maple_scenary::{Channel, Config, ErrorKind, Listener, Mode, pair};

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
