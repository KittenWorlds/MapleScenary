#!/usr/bin/env python3
"""Aggregate benchmark JSON files and render comparison charts.

Usage: bench_chart.py <results-dir> <output-dir>
"""

import json
import pathlib
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

PALETTE = ["#2f6feb", "#f0883e", "#3fb950", "#a371f7", "#db61a2"]


def load(results_dir):
    entries = []
    for path in sorted(pathlib.Path(results_dir).glob("*.json")):
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError as error:
            print(f"skip {path}: {error}")
            continue
        platform = data.get("platform") or path.stem
        for result in data.get("results", []):
            entries.append(
                {
                    "platform": platform,
                    "library": result["library"],
                    "workload": result["workload"],
                    "value": float(result["value"]),
                    "unit": result.get("unit", ""),
                }
            )
    return entries


def grouped_chart(entries, workload, output, title, ylabel, scale):
    rows = [entry for entry in entries if entry["workload"] == workload]
    if not rows:
        print(f"no data for {workload}")
        return
    platforms = sorted({entry["platform"] for entry in rows})
    libraries = sorted({entry["library"] for entry in rows})
    values = {(entry["platform"], entry["library"]): entry["value"] for entry in rows}

    width = 0.8 / max(len(libraries), 1)
    figure, axes = plt.subplots(figsize=(max(8.0, 2.0 * len(platforms) + 2.5), 4.8))
    positions = list(range(len(platforms)))
    for index, library in enumerate(libraries):
        heights = [values.get((platform, library), 0.0) * scale for platform in platforms]
        offsets = [position + (index - (len(libraries) - 1) / 2) * width for position in positions]
        bars = axes.bar(
            offsets,
            heights,
            width,
            label=library,
            color=PALETTE[index % len(PALETTE)],
        )
        axes.bar_label(bars, fmt="%.1f", fontsize=7, padding=2)

    axes.set_xticks(positions)
    axes.set_xticklabels(platforms, fontsize=9)
    axes.set_ylabel(ylabel)
    axes.set_title(title)
    axes.grid(axis="y", alpha=0.25)
    axes.legend(fontsize=8)
    figure.tight_layout()
    figure.savefig(output, dpi=160)
    plt.close(figure)
    print(f"wrote {output}")


def markdown(entries):
    lines = [
        "## IPC benchmark comparison",
        "",
        "Generated from the CI matrix (Linux, macOS, Windows). Lower latency and "
        "higher throughput are better. `ipc-channel` is measured with `Vec<u8>` "
        "payloads, so serialization is included; MapleScenary sends raw bytes. "
        "Runner hardware differs per platform, so compare libraries inside a row, "
        "not absolute numbers across rows.",
        "",
        "### Latency (64 B roundtrip, µs/op)",
        "",
        "| Platform | Library | µs/op |",
        "|----------|---------|------:|",
    ]
    latency = sorted(
        (entry for entry in entries if entry["workload"] == "latency-64B"),
        key=lambda entry: (entry["platform"], entry["library"]),
    )
    for entry in latency:
        lines.append(
            f"| {entry['platform']} | {entry['library']} | {entry['value'] / 1000:.3f} |"
        )
    lines += [
        "",
        "### Throughput (64 KiB messages, MiB/s)",
        "",
        "| Platform | Library | MiB/s |",
        "|----------|---------|------:|",
    ]
    throughput = sorted(
        (entry for entry in entries if entry["workload"] == "throughput-64KiB"),
        key=lambda entry: (entry["platform"], entry["library"]),
    )
    for entry in throughput:
        lines.append(
            f"| {entry['platform']} | {entry['library']} | {entry['value']:.1f} |"
        )
    lines += [
        "",
        "Charts are published with the `benchmark-report` workflow artifact.",
        "",
        "![latency](latency.png)",
        "",
        "![throughput](throughput.png)",
        "",
    ]
    return "\n".join(lines)


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "bench-results"
    output_dir = pathlib.Path(sys.argv[2] if len(sys.argv) > 2 else "bench-report")
    output_dir.mkdir(parents=True, exist_ok=True)

    entries = load(results_dir)
    if not entries:
        print("no benchmark results found")
        return 1

    grouped_chart(
        entries,
        "latency-64B",
        output_dir / "latency.png",
        "64 B roundtrip latency (lower is better)",
        "latency (µs/op)",
        1.0 / 1000.0,
    )
    grouped_chart(
        entries,
        "throughput-64KiB",
        output_dir / "throughput.png",
        "64 KiB throughput (higher is better)",
        "throughput (MiB/s)",
        1.0,
    )
    summary = markdown(entries)
    (output_dir / "summary.md").write_text(summary)
    print(f"wrote {output_dir / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
