//! Simple latency/throughput benchmark, run with `cargo bench`.
//!
//! The harness is disabled so that a plain `cargo bench` prints the numbers
//! without pulling in an external benchmarking framework.

use std::time::{Duration, Instant};

use maple_scenary::{Config, pair};

fn latency() {
    let (client, server) = pair(Config::compact()).expect("pair");
    let payload = [0x5au8; 64];
    let iterations = 200_000u64;
    // Warm up the rings and the allocator.
    for _ in 0..1_000 {
        client.send(1, &payload).expect("send");
        server.recv(Some(Duration::from_secs(1))).expect("recv");
    }
    let start = Instant::now();
    for _ in 0..iterations {
        client.send(1, &payload).expect("send");
        let message = server.recv(Some(Duration::from_secs(1))).expect("recv");
        std::hint::black_box(message.bytes());
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() as f64 / iterations as f64;
    println!(
        "latency 64 B: {:>8.0} ns/roundtrip  ({:.2} M roundtrips/s)",
        per_op,
        iterations as f64 / elapsed.as_secs_f64() / 1e6
    );
}

fn throughput() {
    let (client, server) = pair(Config::compact()).expect("pair");
    let payload = vec![0xa5u8; 64 * 1024];
    let total_bytes = 512 * 1024 * 1024u64;
    let iterations = total_bytes / payload.len() as u64;

    let start = Instant::now();
    for _ in 0..iterations {
        client
            .send_timeout(2, &payload, Duration::from_secs(5))
            .expect("send");
        let message = server.recv(Some(Duration::from_secs(5))).expect("recv");
        std::hint::black_box(message.bytes());
    }
    let elapsed = start.elapsed();
    let mib = total_bytes as f64 / (1024.0 * 1024.0);
    println!(
        "throughput 64 KiB: {:>8.1} MiB/s  ({:.2} M messages/s)",
        mib / elapsed.as_secs_f64(),
        iterations as f64 / elapsed.as_secs_f64() / 1e6
    );
}

fn main() {
    println!("MapleScenary benchmark (mode: {})", std::env::consts::OS);
    latency();
    throughput();
}
