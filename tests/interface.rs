//! Interface envelopes share the raw channel's transport and ownership rules.

use std::time::Duration;

use maple_scenary::interface::{
    self, HEADER_LEN, Header, INTERFACE_KIND, InterfaceMessage, MessageType, RequestIds,
};
use maple_scenary::{Config, ErrorKind, Mode, pair};

const WAIT: Option<Duration> = Some(Duration::from_secs(5));

fn correlated_responses(force_stream: bool) {
    let mut config = Config::compact();
    config.force_stream = force_stream;
    let (client, server) = pair(config).unwrap();
    assert_eq!(
        client.mode(),
        if force_stream {
            Mode::Stream
        } else {
            Mode::SharedMemory
        }
    );
    let (client_tx, client_rx) = client.split();
    let (server_tx, server_rx) = server.split();
    let ids = RequestIds::new();
    let first = Header::request(10, 4, 1, ids.next_id().unwrap());
    let second = Header::request(10, 4, 1, ids.next_id().unwrap());
    interface::send(&client_tx, first, b"first").unwrap();
    let mut outgoing = interface::reserve(&client_tx, second, 6).unwrap();
    outgoing.payload_mut().copy_from_slice(b"second");
    outgoing.publish().unwrap();

    let a = InterfaceMessage::from_message(server_rx.recv(WAIT).unwrap()).unwrap();
    let b = InterfaceMessage::from_message(server_rx.recv(WAIT).unwrap()).unwrap();
    a.header().validate_for(10, 1).unwrap();
    b.header().validate_for(10, 1).unwrap();
    assert_eq!(a.payload(), b"first");
    assert_eq!(b.payload(), b"second");
    // An event can be interleaved, and responses need not follow request order.
    interface::send(&server_tx, Header::one_way(10, 8, 0), b"event").unwrap();
    interface::send(&server_tx, b.header().response().unwrap(), b"reply two").unwrap();
    interface::send(&server_tx, a.header().response().unwrap(), b"reply one").unwrap();

    let event = InterfaceMessage::from_message(client_rx.recv(WAIT).unwrap()).unwrap();
    assert_eq!(event.header().message_type(), MessageType::OneWay);
    assert!(!event.header().is_response_to(first));
    assert_eq!(event.payload(), b"event");
    let reply = InterfaceMessage::from_message(client_rx.recv(WAIT).unwrap()).unwrap();
    assert!(reply.header().is_response_to(second));
    assert!(!reply.header().is_response_to(first));
    assert_eq!(reply.payload(), b"reply two");
    let reply = InterfaceMessage::from_message(client_rx.recv(WAIT).unwrap()).unwrap();
    assert!(reply.header().is_response_to(first));
    assert_eq!(reply.payload(), b"reply one");
    assert!(client_rx.try_recv().unwrap().is_none());
}

#[test]
fn shared_memory_correlation() {
    correlated_responses(false);
}

#[test]
fn stream_correlation() {
    correlated_responses(true);
}

#[test]
fn mismatched_response_metadata_is_rejected() {
    let ids = RequestIds::new();
    let id = ids.next_id().unwrap();
    let request = Header::request(10, 4, 1, id);
    for other in [
        Header::request(11, 4, 1, id),
        Header::request(10, 5, 1, id),
        Header::request(10, 4, 2, id),
        Header::request(10, 4, 1, ids.next_id().unwrap()),
    ] {
        assert!(!other.response().unwrap().is_response_to(request));
    }
    assert!(!request.is_response_to(request));
}

#[test]
fn malformed_envelope_does_not_consume_next_message() {
    for force_stream in [false, true] {
        let mut config = Config::compact();
        config.force_stream = force_stream;
        let (sender, receiver) = pair(config).unwrap();
        sender.send(INTERFACE_KIND, b"short").unwrap();
        sender.send(7, b"raw").unwrap();
        sender.send(INTERFACE_KIND, &Header::one_way(1, 2, 0).encode()).unwrap();
        for _ in 0..2 {
            let error = InterfaceMessage::from_message(receiver.recv(WAIT).unwrap())
                .err()
                .expect("bad message");
            assert_eq!(error.kind(), ErrorKind::Protocol);
        }
        let valid = InterfaceMessage::from_message(receiver.recv(WAIT).unwrap()).unwrap();
        assert!(valid.payload().is_empty());
        assert_eq!(valid.header().method(), 2);
        let raw = valid.into_inner();
        assert_eq!(raw.kind(), INTERFACE_KIND);
        assert_eq!(raw.len(), HEADER_LEN);
    }
}

#[test]
fn limits_include_header_and_dropped_reservations_are_reusable() {
    for force_stream in [false, true] {
        let mut config = Config::compact();
        config.force_stream = force_stream;
        let (client, _server) = pair(config).unwrap();
        let (sender, _receiver) = client.split();
        let header = Header::one_way(1, 2, 0);
        let max = sender.max_message_len();
        for len in [max - HEADER_LEN + 1, usize::MAX] {
            let error = interface::reserve(&sender, header, len)
                .err()
                .expect("oversized");
            assert_eq!(error.kind(), ErrorKind::MessageTooLarge);
        }
        drop(interface::reserve(&sender, header, max - HEADER_LEN).unwrap());
        drop(interface::reserve(&sender, header, max - HEADER_LEN).unwrap());
    }
}
