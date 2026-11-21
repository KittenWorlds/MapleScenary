//! Cross-platform benchmark harness used by CI.
//!
//! Run locally:
//!
//! ```bash
//! cargo bench --bench roundtrip
//! cargo bench --bench roundtrip -- --json bench-results/linux.json --repeats 3
//! cargo bench --bench roundtrip -- --save-baseline benches/baseline/linux.json
//! cargo bench --bench roundtrip -- --baseline benches/baseline/linux.json --threshold 30
//! ```
//!
//! Every number is the median of `--repeats` runs (default 3). Latency and
//! throughput are measured over `pair` (same process, two endpoints) and, for
//! the shared-memory path, over a real `Listener`/`connect` pair where the peer
//! is a child process. The receiver always consumes the payload (a copy or a
//! checksum), so throughput is not inflated by untouched pages.
//!
//! `ipc-channel` is the same-category competitor LavaCat uses today. It is
//! measured with `Vec<u8>` payloads, i.e. its numbers include serde/postcard
//! serialization on send and deserialization on receive.

use std::hint::black_box;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use ipc_channel::ipc;
use maple_scenary::{Channel, Config, Listener, pair};

const LATENCY_ITERATIONS: u64 = 100_000;
/// Cross-process roundtrips include scheduling, so use fewer iterations.
const TWO_PROCESS_ITERATIONS: u64 = 20_000;
const THROUGHPUT_BYTES: u64 = 256 * 1024 * 1024;
const THROUGHPUT_CHUNK: usize = 64 * 1024;
const WARMUP: u64 = 1_000;
const WAIT: Option<Duration> = Some(Duration::from_secs(30));
const DEFAULT_REPEATS: usize = 3;

struct Measurement {
    library: &'static str,
    workload: &'static str,
    unit: &'static str,
    values: Vec<f64>,
}

impl Measurement {
    fn median(&self) -> f64 {
        let mut values = self.values.clone();
        values.sort_by(f64::total_cmp);
        values[values.len() / 2]
    }

    fn stddev(&self) -> f64 {
        if self.values.len() < 2 {
            return 0.0;
        }
        let mean = self.values.iter().sum::<f64>() / self.values.len() as f64;
        let variance = self
            .values
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (self.values.len() - 1) as f64;
        variance.sqrt()
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| message.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

fn bench(
    library: &'static str,
    workload: &'static str,
    unit: &'static str,
    repeats: usize,
    mut once: impl FnMut() -> Result<f64, String>,
) -> Result<Measurement, String> {
    let mut values = Vec::with_capacity(repeats);
    for _ in 0..repeats {
        let value = match catch_unwind(AssertUnwindSafe(&mut once)) {
            Ok(result) => result?,
            // A skipped sample would silently bias the chart, so surface panics.
            Err(payload) => return Err(format!("benchmark panicked: {}", panic_message(payload))),
        };
        values.push(value);
    }
    Ok(Measurement {
        library,
        workload,
        unit,
        values,
    })
}

fn latency_ns(elapsed: Duration, iterations: u64) -> f64 {
    elapsed.as_nanos() as f64 / iterations as f64
}

fn throughput_mib(elapsed: Duration) -> f64 {
    let mib = THROUGHPUT_BYTES as f64 / (1024.0 * 1024.0);
    mib / elapsed.as_secs_f64()
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0u64, |acc, byte| acc.wrapping_add(u64::from(*byte)))
}

fn maple_latency(stream: bool) -> Result<f64, String> {
    let mut config = Config::compact();
    config.force_stream = stream;
    let (client, server) = pair(config).map_err(|error| error.to_string())?;
    let payload = [0x5au8; 64];
    for _ in 0..WARMUP {
        client
            .send(1, &payload)
            .map_err(|error| error.to_string())?;
        black_box(checksum(
            server.recv(WAIT).map_err(|e| e.to_string())?.bytes(),
        ));
    }
    let start = Instant::now();
    let mut seen = 0u64;
    for _ in 0..LATENCY_ITERATIONS {
        client
            .send(1, &payload)
            .map_err(|error| error.to_string())?;
        seen = seen.wrapping_add(checksum(
            server.recv(WAIT).map_err(|e| e.to_string())?.bytes(),
        ));
    }
    black_box(seen);
    Ok(latency_ns(start.elapsed(), LATENCY_ITERATIONS))
}

/// Zero-copy variant: the payload is written straight into the mapped region.
fn maple_reserve_latency() -> Result<f64, String> {
    let (client, server) = pair(Config::compact()).map_err(|error| error.to_string())?;
    let mut warmup = 0u64;
    for _ in 0..WARMUP {
        let mut outgoing = client.reserve(64).map_err(|error| error.to_string())?;
        outgoing.buffer_mut().fill(0x5a);
        outgoing.publish(1).map_err(|error| error.to_string())?;
        warmup = warmup.wrapping_add(checksum(
            server.recv(WAIT).map_err(|e| e.to_string())?.bytes(),
        ));
    }
    let start = Instant::now();
    let mut seen = warmup;
    for _ in 0..LATENCY_ITERATIONS {
        let mut outgoing = client.reserve(64).map_err(|error| error.to_string())?;
        outgoing.buffer_mut().fill(0x5a);
        outgoing.publish(1).map_err(|error| error.to_string())?;
        seen = seen.wrapping_add(checksum(
            server.recv(WAIT).map_err(|e| e.to_string())?.bytes(),
        ));
    }
    black_box(seen);
    Ok(latency_ns(start.elapsed(), LATENCY_ITERATIONS))
}

/// Throughput runs the receiver on its own thread and copies every payload, so
/// the numbers include the read side a real consumer pays.
fn throughput(
    send: impl Fn(&[u8]) -> Result<(), String>,
    recv: impl Fn() + Send + 'static,
) -> Result<f64, String> {
    let payload = vec![0xa5u8; THROUGHPUT_CHUNK];
    let iterations = THROUGHPUT_BYTES / THROUGHPUT_CHUNK as u64;
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let _ = ready_tx.send(());
        for _ in 0..iterations {
            recv();
        }
    });
    ready_rx
        .recv()
        .map_err(|error| format!("receiver failed to start: {error}"))?;
    let start = Instant::now();
    for _ in 0..iterations {
        send(&payload)?;
    }
    let elapsed = start.elapsed();
    reader
        .join()
        .map_err(|_| "receiver thread panicked".to_string())?;
    Ok(throughput_mib(elapsed))
}

fn maple_throughput(stream: bool) -> Result<f64, String> {
    let mut config = Config::compact();
    config.force_stream = stream;
    let (client, server) = pair(config).map_err(|error| error.to_string())?;
    throughput(
        move |payload| {
            client
                .send_timeout(2, payload, Duration::from_secs(10))
                .map_err(|error| error.to_string())
        },
        move || {
            let message = server.recv(WAIT).expect("recv");
            let copy = message.bytes().to_vec();
            black_box(copy);
        },
    )
}

fn maple_reserve_throughput() -> Result<f64, String> {
    let (client, server) = pair(Config::compact()).map_err(|error| error.to_string())?;
    throughput(
        move |payload| {
            loop {
                match client.reserve(payload.len()) {
                    Ok(mut outgoing) => {
                        outgoing.buffer_mut().copy_from_slice(payload);
                        return outgoing.publish(2).map_err(|error| error.to_string());
                    }
                    Err(error) if error.kind() == maple_scenary::ErrorKind::WouldBlock => {
                        std::hint::spin_loop();
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
        },
        move || {
            let message = server.recv(WAIT).expect("recv");
            let copy = message.bytes().to_vec();
            black_box(copy);
        },
    )
}

fn ipc_channel_latency() -> Result<f64, String> {
    let (sender, receiver) = ipc::channel::<Vec<u8>>().map_err(|error| error.to_string())?;
    let payload = vec![0x5au8; 64];
    for _ in 0..WARMUP {
        sender
            .send(payload.clone())
            .map_err(|error| error.to_string())?;
        black_box(checksum(&receiver.recv().map_err(|e| e.to_string())?));
    }
    let start = Instant::now();
    let mut seen = 0u64;
    for _ in 0..LATENCY_ITERATIONS {
        sender
            .send(payload.clone())
            .map_err(|error| error.to_string())?;
        seen = seen.wrapping_add(checksum(&receiver.recv().map_err(|e| e.to_string())?));
    }
    black_box(seen);
    Ok(latency_ns(start.elapsed(), LATENCY_ITERATIONS))
}

fn ipc_channel_throughput() -> Result<f64, String> {
    let (sender, receiver) = ipc::channel::<Vec<u8>>().map_err(|error| error.to_string())?;
    throughput(
        move |payload| {
            sender
                .send(payload.to_vec())
                .map_err(|error| error.to_string())
        },
        move || {
            black_box(receiver.recv().expect("recv"));
        },
    )
}

/// True two-process benchmark: the peer is a child process reached through
/// `Listener`/`connect`, so shared-memory handle passing and scheduling are
/// part of the measured path.
fn two_process_latency(run_id: u32) -> Result<f64, String> {
    let (listener, _child, channel) = spawn_peer(run_id, Peer::Echo)?;
    let payload = [0x5au8; 64];
    for _ in 0..WARMUP {
        channel
            .send(1, &payload)
            .map_err(|error| error.to_string())?;
        black_box(checksum(
            channel.recv(WAIT).map_err(|e| e.to_string())?.bytes(),
        ));
    }
    let start = Instant::now();
    let mut seen = 0u64;
    for _ in 0..TWO_PROCESS_ITERATIONS {
        channel
            .send(1, &payload)
            .map_err(|error| error.to_string())?;
        seen = seen.wrapping_add(checksum(
            channel.recv(WAIT).map_err(|e| e.to_string())?.bytes(),
        ));
    }
    black_box(seen);
    let elapsed = start.elapsed();
    drop(channel);
    drop(listener);
    Ok(latency_ns(elapsed, TWO_PROCESS_ITERATIONS))
}

fn two_process_throughput(run_id: u32) -> Result<f64, String> {
    let (_listener, mut child, channel) = spawn_peer(run_id, Peer::Sink)?;
    let payload = vec![0xa5u8; THROUGHPUT_CHUNK];
    let iterations = THROUGHPUT_BYTES / THROUGHPUT_CHUNK as u64;
    let start = Instant::now();
    for _ in 0..iterations {
        channel
            .send_timeout(2, &payload, Duration::from_secs(10))
            .map_err(|error| error.to_string())?;
    }
    // Stop the clock only once the peer has consumed everything, otherwise the
    // sender's queue would be measured while the reader is still behind.
    drop(channel);
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("peer exited with {status}"));
    }
    Ok(throughput_mib(start.elapsed()))
}

enum Peer {
    Echo,
    Sink,
}

/// Kills the child process if the measurement unwinds or fails.
struct ChildGuard(Child);

impl ChildGuard {
    fn wait(&mut self) -> Result<std::process::ExitStatus, String> {
        self.0.wait().map_err(|error| error.to_string())
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn endpoint_name(run_id: u32) -> String {
    #[cfg(windows)]
    {
        format!(r"\\.\pipe\maple-bench-{}-{run_id}", std::process::id())
    }
    #[cfg(not(windows))]
    {
        std::env::temp_dir()
            .join(format!("maple-bench-{}-{run_id}.sock", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }
}

fn spawn_peer(run_id: u32, peer: Peer) -> Result<(Listener, ChildGuard, Channel), String> {
    let name = endpoint_name(run_id);
    let listener = Listener::bind(&name, Config::compact()).map_err(|error| error.to_string())?;
    let exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let mut command = Command::new(exe);
    match peer {
        Peer::Echo => {
            command.arg("--child-echo").arg(&name);
        }
        Peer::Sink => {
            command
                .arg("--child-sink")
                .arg(&name)
                .arg(THROUGHPUT_BYTES.to_string());
        }
    }
    let child = command.spawn().map_err(|error| error.to_string())?;
    let guard = ChildGuard(child);
    let channel = listener
        .accept(Some(Duration::from_secs(30)))
        .map_err(|error| error.to_string())?;
    Ok((listener, guard, channel))
}

fn child_echo(name: &str) -> ! {
    let channel = maple_scenary::connect(name, Config::compact()).expect("connect");
    while let Ok(message) = channel.recv(WAIT) {
        let _ = channel.send(message.kind(), message.bytes());
    }
    std::process::exit(0)
}

fn child_sink(name: &str, expected: u64) -> ! {
    let channel = maple_scenary::connect(name, Config::compact()).expect("connect");
    let mut total = 0u64;
    while let Ok(message) = channel.recv(WAIT) {
        total += message.len() as u64;
        black_box(message.bytes().to_vec());
        if total >= expected {
            break;
        }
    }
    std::process::exit(if total >= expected { 0 } else { 1 })
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn write_results(
    path: &str,
    platform: &str,
    measurements: &[Measurement],
    failures: &[String],
) -> std::io::Result<()> {
    let mut json = format!(
        "{{\"platform\":\"{}\",\"os\":\"{}\",\"arch\":\"{}\",\"results\":[",
        json_escape(platform),
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    for (index, measurement) in measurements.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        let median = measurement.median();
        let ops_per_sec = match measurement.unit {
            "ns/op" => 1e9 / median,
            "MiB/s" => median,
            _ => 0.0,
        };
        json.push_str(&format!(
            "{{\"library\":\"{}\",\"workload\":\"{}\",\"unit\":\"{}\",\"value\":{:.3},\"stddev\":{:.3},\"min\":{:.3},\"max\":{:.3},\"repeats\":{},\"ops_per_sec\":{:.3}}}",
            json_escape(measurement.library),
            json_escape(measurement.workload),
            json_escape(measurement.unit),
            median,
            measurement.stddev(),
            measurement.values.iter().copied().fold(f64::INFINITY, f64::min),
            measurement.values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
            measurement.values.len(),
            ops_per_sec
        ));
    }
    json.push_str("],\"failures\":[");
    for (index, failure) in failures.iter().enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&format!("\"{}\"", json_escape(failure)));
    }
    json.push_str("]}");
    if let Some(parent) = std::path::Path::new(path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(path, json)
}

/// Minimal JSON reader for the baseline file written by `--save-baseline`.
fn read_baseline(path: &str) -> Result<Vec<(String, String, f64, String)>, String> {
    let text = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
    let mut entries = Vec::new();
    for object in text.split('{').skip(1) {
        let Some(end) = object.find('}') else {
            continue;
        };
        let object = &object[..end];
        let get = |key: &str| -> Option<String> {
            let start = object.find(&format!("\"{key}\":"))? + key.len() + 3;
            let rest = &object[start..];
            let rest = rest.strip_prefix('"')?;
            let end = rest.find('"')?;
            Some(rest[..end].to_string())
        };
        let number = |key: &str| -> Option<f64> {
            let start = object.find(&format!("\"{key}\":"))? + key.len() + 3;
            let rest = &object[start..];
            let end = rest.find([',', '}']).unwrap_or(rest.len());
            rest[..end].trim().parse().ok()
        };
        if let (Some(library), Some(workload), Some(value), Some(unit)) = (
            get("library"),
            get("workload"),
            number("value"),
            get("unit"),
        ) {
            entries.push((library, workload, value, unit));
        }
    }
    Ok(entries)
}

fn check_regressions(
    baseline: &[(String, String, f64, String)],
    measurements: &[Measurement],
    latency_threshold: f64,
    throughput_threshold: f64,
) -> Vec<String> {
    let mut regressions = Vec::new();
    for measurement in measurements {
        // Only gate our own in-process paths: competitor numbers are
        // informational, and cross-process scheduling on shared runners is too
        // noisy for an absolute threshold.
        if !measurement.library.starts_with("maple-scenary")
            || measurement.library.contains("2 proc")
        {
            continue;
        }
        let Some((_, _, base, _)) = baseline.iter().find(|(library, workload, _, _)| {
            library == measurement.library && workload == measurement.workload
        }) else {
            continue;
        };
        let current = measurement.median();
        let change = (current - base) / *base * 100.0;
        // Shared CI runners vary a lot in memory bandwidth, but are far more
        // consistent on latency, so use a much looser throughput threshold.
        let threshold = if measurement.unit == "ns/op" {
            latency_threshold
        } else {
            throughput_threshold
        };
        let regressed = if measurement.unit == "ns/op" {
            change > threshold
        } else {
            change < -threshold
        };
        if regressed {
            regressions.push(format!(
                "{} {} regressed {:.1}% ({:.1} -> {:.1} {})",
                measurement.library, measurement.workload, change, base, current, measurement.unit
            ));
        }
    }
    regressions
}

/// Hardware-independent contract: on the same runner the shared-memory path
/// must stay at least this many times faster than `ipc-channel`, and its
/// latency must stay within this factor of the competitor.
const MIN_THROUGHPUT_RATIO: f64 = 2.0;
const MAX_LATENCY_RATIO: f64 = 2.0;

fn check_competitor_ratio(measurements: &[Measurement]) -> Vec<String> {
    let competitor_throughput = measurements
        .iter()
        .find(|measurement| {
            measurement.library == "ipc-channel" && measurement.workload == "throughput-64KiB"
        })
        .map(Measurement::median);
    let competitor_latency = measurements
        .iter()
        .find(|measurement| {
            measurement.library == "ipc-channel" && measurement.workload == "latency-64B"
        })
        .map(Measurement::median);
    let mut failures = Vec::new();
    for measurement in measurements {
        if !measurement.library.starts_with("maple-scenary")
            || measurement.library.contains("2 proc")
        {
            continue;
        }
        if measurement.workload == "throughput-64KiB"
            && !measurement.library.contains("stream")
            && let Some(competitor) = competitor_throughput
        {
            let ratio = measurement.median() / competitor;
            if ratio < MIN_THROUGHPUT_RATIO {
                failures.push(format!(
                    "{} throughput is only {:.2}x ipc-channel (minimum {MIN_THROUGHPUT_RATIO:.1}x)",
                    measurement.library, ratio
                ));
            }
        }
        if measurement.workload == "latency-64B"
            && let Some(competitor) = competitor_latency
        {
            let ratio = measurement.median() / competitor;
            if ratio > MAX_LATENCY_RATIO {
                failures.push(format!(
                    "{} latency is {:.2}x ipc-channel (maximum {MAX_LATENCY_RATIO:.1}x)",
                    measurement.library, ratio
                ));
            }
        }
    }
    failures
}

fn print_table(measurements: &[Measurement]) {
    println!(
        "{:<26} {:<19} {:>12} {:>10} {:>10}",
        "library", "workload", "median", "stddev", "unit"
    );
    for measurement in measurements {
        println!(
            "{:<26} {:<19} {:>12.2} {:>10.2} {:>10}",
            measurement.library,
            measurement.workload,
            measurement.median(),
            measurement.stddev(),
            measurement.unit
        );
    }
}

fn main() {
    let mut json_path = None;
    let mut save_baseline = None;
    let mut baseline_path = None;
    let mut threshold = 30.0f64;
    let mut throughput_threshold = 70.0f64;
    let mut repeats = DEFAULT_REPEATS;
    let mut platform = std::env::var("BENCH_PLATFORM")
        .unwrap_or_else(|_| format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH));

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--child-echo" => {
                let name = args.next().expect("--child-echo needs an endpoint");
                child_echo(&name);
            }
            "--child-sink" => {
                let name = args.next().expect("--child-sink needs an endpoint");
                let expected = args
                    .next()
                    .expect("--child-sink needs a byte count")
                    .parse()
                    .expect("invalid byte count");
                child_sink(&name, expected);
            }
            "--json" => json_path = args.next(),
            "--save-baseline" => save_baseline = args.next(),
            "--baseline" => baseline_path = args.next(),
            "--threshold" => {
                if let Some(value) = args.next() {
                    threshold = value.parse().unwrap_or(30.0);
                }
            }
            "--throughput-threshold" => {
                if let Some(value) = args.next() {
                    throughput_threshold = value.parse().unwrap_or(70.0);
                }
            }
            "--repeats" => {
                if let Some(value) = args.next() {
                    repeats = value.parse().unwrap_or(DEFAULT_REPEATS).max(1);
                }
            }
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

    println!("MapleScenary benchmark on {platform} ({repeats} repeats)");
    let mut measurements = Vec::new();
    let mut failures = Vec::new();
    let mut run_id = 0u32;

    macro_rules! run {
        ($library:expr, $workload:expr, $unit:expr, $body:expr) => {{
            match bench($library, $workload, $unit, repeats, $body) {
                Ok(measurement) => measurements.push(measurement),
                Err(error) => failures.push(format!("{} {}: {}", $library, $workload, error)),
            }
        }};
    }

    run!("maple-scenary", "latency-64B", "ns/op", || {
        maple_latency(false)
    });
    run!("maple-scenary", "throughput-64KiB", "MiB/s", || {
        maple_throughput(false)
    });
    run!("maple-scenary (reserve)", "latency-64B", "ns/op", {
        maple_reserve_latency
    });
    run!("maple-scenary (reserve)", "throughput-64KiB", "MiB/s", {
        maple_reserve_throughput
    });
    run!("maple-scenary (stream)", "latency-64B", "ns/op", || {
        maple_latency(true)
    });
    run!(
        "maple-scenary (stream)",
        "throughput-64KiB",
        "MiB/s",
        || { maple_throughput(true) }
    );
    run!("ipc-channel", "latency-64B", "ns/op", ipc_channel_latency);
    run!("ipc-channel", "throughput-64KiB", "MiB/s", {
        ipc_channel_throughput
    });
    run!("maple-scenary (2 proc)", "latency-64B", "ns/op", || {
        run_id += 1;
        two_process_latency(run_id)
    });
    run!(
        "maple-scenary (2 proc)",
        "throughput-64KiB",
        "MiB/s",
        || {
            run_id += 1;
            two_process_throughput(run_id)
        }
    );

    print_table(&measurements);
    for failure in &failures {
        eprintln!("benchmark failed: {failure}");
    }

    if let Some(path) = save_baseline.as_deref()
        && let Err(error) = write_results(path, &platform, &measurements, &[])
    {
        eprintln!("failed to write baseline {path}: {error}");
        std::process::exit(1);
    }

    if let Some(path) = baseline_path.as_deref() {
        match read_baseline(path) {
            Ok(baseline) => {
                let regressions =
                    check_regressions(&baseline, &measurements, threshold, throughput_threshold);
                if regressions.is_empty() {
                    println!(
                        "no regression above {threshold:.0}% latency / \
                         {throughput_threshold:.0}% throughput against {path}"
                    );
                } else {
                    for regression in &regressions {
                        eprintln!("regression: {regression}");
                    }
                    failures.extend(regressions);
                }
            }
            Err(error) => println!("baseline {path} is unavailable ({error}); skipping check"),
        }
    }

    let ratio_failures = check_competitor_ratio(&measurements);
    for failure in &ratio_failures {
        eprintln!("contract: {failure}");
    }
    failures.extend(ratio_failures);

    if let Some(path) = json_path.as_deref()
        && let Err(error) = write_results(path, &platform, &measurements, &failures)
    {
        eprintln!("failed to write {path}: {error}");
        std::process::exit(1);
    }

    if !failures.is_empty() {
        std::process::exit(1);
    }
}
