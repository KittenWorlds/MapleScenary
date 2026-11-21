//! Cross-platform benchmark harness used by CI.
//!
//! Run locally:
//!
//! ```bash
//! cargo bench --bench roundtrip
//! cargo bench --bench roundtrip -- --json bench-results/linux.json
//! ```
//!
//! The harness measures MapleScenary in both data-plane modes and compares it
//! with `ipc-channel`, the crate LavaCat uses today for the same job. Numbers
//! are emitted as JSON so the CI chart job can aggregate Linux, macOS and
//! Windows runs.

use std::hint::black_box;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::{Duration, Instant};

use ipc_channel::ipc;
use maple_scenary::{Config, pair};

const LATENCY_ITERATIONS: u64 = 100_000;
const THROUGHPUT_BYTES: u64 = 256 * 1024 * 1024;
const THROUGHPUT_CHUNK: usize = 64 * 1024;
const WARMUP: u64 = 1_000;
const WAIT: Option<Duration> = Some(Duration::from_secs(10));

struct Sample {
    library: &'static str,
    workload: &'static str,
    value: f64,
    unit: &'static str,
    ops_per_sec: f64,
}

fn maple_latency(stream: bool) -> Sample {
    let mut config = Config::compact();
    config.force_stream = stream;
    let (client, server) = pair(config).expect("maple pair");
    let payload = [0x5au8; 64];
    for _ in 0..WARMUP {
        client.send(1, &payload).expect("send");
        black_box(server.recv(WAIT).expect("recv").bytes());
    }
    let start = Instant::now();
    for _ in 0..LATENCY_ITERATIONS {
        client.send(1, &payload).expect("send");
        black_box(server.recv(WAIT).expect("recv").bytes());
    }
    let elapsed = start.elapsed();
    Sample {
        library: if stream {
            "maple-scenary (stream)"
        } else {
            "maple-scenary"
        },
        workload: "latency-64B",
        value: elapsed.as_nanos() as f64 / LATENCY_ITERATIONS as f64,
        unit: "ns/op",
        ops_per_sec: LATENCY_ITERATIONS as f64 / elapsed.as_secs_f64(),
    }
}

fn maple_throughput(stream: bool) -> Sample {
    let mut config = Config::compact();
    config.force_stream = stream;
    let (client, server) = pair(config).expect("maple pair");
    throughput(
        if stream {
            "maple-scenary (stream)"
        } else {
            "maple-scenary"
        },
        move |payload| {
            client
                .send_timeout(2, payload, Duration::from_secs(10))
                .expect("send");
        },
        move || {
            black_box(server.recv(WAIT).expect("recv").bytes());
        },
    )
}

/// Throughput is measured with the receiver on a second thread: the stream
/// fallback writes whole frames into the socket, so a single thread that sends
/// before it reads can deadlock once a message outgrows the socket buffer
/// (macOS defaults to 8 KiB). Separate reader/writer threads also match how a
/// real host talks to a renderer process.
fn throughput(
    library: &'static str,
    send: impl Fn(&[u8]),
    recv: impl Fn() + Send + 'static,
) -> Sample {
    let payload = vec![0xa5u8; THROUGHPUT_CHUNK];
    let iterations = THROUGHPUT_BYTES / THROUGHPUT_CHUNK as u64;
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        ready_tx.send(()).expect("ready");
        for _ in 0..iterations {
            recv();
        }
    });
    ready_rx.recv().expect("receiver ready");
    let start = Instant::now();
    for _ in 0..iterations {
        send(&payload);
    }
    let elapsed = start.elapsed();
    reader.join().expect("receiver thread");
    let mib = THROUGHPUT_BYTES as f64 / (1024.0 * 1024.0);
    Sample {
        library,
        workload: "throughput-64KiB",
        value: mib / elapsed.as_secs_f64(),
        unit: "MiB/s",
        ops_per_sec: iterations as f64 / elapsed.as_secs_f64(),
    }
}

fn ipc_channel_latency() -> Sample {
    let (sender, receiver) = ipc::channel::<Vec<u8>>().expect("ipc-channel");
    let payload = vec![0x5au8; 64];
    for _ in 0..WARMUP {
        sender.send(payload.clone()).expect("send");
        black_box(receiver.recv().expect("recv"));
    }
    let start = Instant::now();
    for _ in 0..LATENCY_ITERATIONS {
        sender.send(payload.clone()).expect("send");
        black_box(receiver.recv().expect("recv"));
    }
    let elapsed = start.elapsed();
    Sample {
        library: "ipc-channel",
        workload: "latency-64B",
        value: elapsed.as_nanos() as f64 / LATENCY_ITERATIONS as f64,
        unit: "ns/op",
        ops_per_sec: LATENCY_ITERATIONS as f64 / elapsed.as_secs_f64(),
    }
}

fn ipc_channel_throughput() -> Sample {
    let (sender, receiver) = ipc::channel::<Vec<u8>>().expect("ipc-channel");
    throughput(
        "ipc-channel",
        move |payload| sender.send(payload.to_vec()).expect("send"),
        move || {
            black_box(receiver.recv().expect("recv"));
        },
    )
}

fn measure(label: &str, run: impl FnOnce() -> Sample) -> Option<Sample> {
    match catch_unwind(AssertUnwindSafe(run)) {
        Ok(sample) => Some(sample),
        Err(_) => {
            eprintln!("skipped {label}: benchmark panicked");
            None
        }
    }
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn write_json(path: &str, platform: &str, samples: &[Sample]) -> std::io::Result<()> {
    let mut json = format!(
        "{{\"platform\":\"{}\",\"os\":\"{}\",\"arch\":\"{}\",\"results\":[",
        json_escape(platform),
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    for (index, sample) in samples.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            "{{\"library\":\"{}\",\"workload\":\"{}\",\"value\":{:.3},\"unit\":\"{}\",\"ops_per_sec\":{:.3}}}",
            json_escape(sample.library),
            json_escape(sample.workload),
            sample.value,
            json_escape(sample.unit),
            sample.ops_per_sec
        ));
    }
    json.push_str("]}");
    std::fs::write(path, json)
}

fn print_table(samples: &[Sample]) {
    println!(
        "{:<24} {:<19} {:>14} {:>15}",
        "library", "workload", "value", "ops/sec"
    );
    for sample in samples {
        println!(
            "{:<24} {:<19} {:>10.1} {:<3} {:>15.0}",
            sample.library, sample.workload, sample.value, sample.unit, sample.ops_per_sec
        );
    }
}

fn main() {
    let mut json_path = None;
    let mut platform = std::env::var("BENCH_PLATFORM")
        .unwrap_or_else(|_| format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH));
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--json" => json_path = args.next(),
            "--label" => {
                if let Some(label) = args.next() {
                    platform = label;
                }
            }
            // `cargo bench` forwards `--bench` to the (harness-less) binary.
            "--bench" => {}
            other => eprintln!("ignoring unknown argument {other:?}"),
        }
    }

    println!("MapleScenary benchmark on {platform}");
    let mut samples = Vec::new();
    for (label, sample) in [
        (
            "maple-scenary latency",
            measure("maple-scenary latency", || maple_latency(false)),
        ),
        (
            "maple-scenary throughput",
            measure("maple-scenary throughput", || maple_throughput(false)),
        ),
        (
            "maple-scenary (stream) latency",
            measure("maple-scenary (stream) latency", || maple_latency(true)),
        ),
        (
            "maple-scenary (stream) throughput",
            measure("maple-scenary (stream) throughput", || {
                maple_throughput(true)
            }),
        ),
        (
            "ipc-channel latency",
            measure("ipc-channel latency", ipc_channel_latency),
        ),
        (
            "ipc-channel throughput",
            measure("ipc-channel throughput", ipc_channel_throughput),
        ),
    ] {
        if let Some(sample) = sample {
            samples.push(sample);
        } else {
            eprintln!("no sample for {label}");
        }
    }

    print_table(&samples);
    if let Some(path) = json_path {
        if let Some(parent) = std::path::Path::new(&path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(error) = write_json(&path, &platform, &samples) {
            eprintln!("failed to write {path}: {error}");
            std::process::exit(1);
        }
        println!("wrote {path}");
    }
}
