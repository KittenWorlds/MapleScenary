#!/usr/bin/env python3
"""Aggregate benchmark JSON files and render comparison charts.

Usage:
    bench_chart.py <results-dir> <output-dir> [--platforms ubuntu-latest,macos-latest,windows-latest]

The report is only written when every required platform produced a complete,
failure-free result set. Missing data exits non-zero so CI does not publish a
half-empty comparison.
"""

import json
import pathlib
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

LIBRARY_ORDER = [
    "maple-scenary",
    "maple-scenary (reserve)",
    "maple-scenary (stream)",
    "maple-scenary (2 proc)",
    "ipc-channel",
]
PALETTE = ["#2f6feb", "#3fb950", "#a371f7", "#f0883e", "#db61a2"]
WORKLOADS = ["latency-64B", "throughput-64KiB"]


def fail(message):
    print(f"error: {message}", file=sys.stderr)
    return 1


def load(results_dir, required):
    entries = []
    problems = []
    for platform in required:
        path = pathlib.Path(results_dir) / f"{platform}.json"
        if not path.exists():
            problems.append(f"missing results for {platform} ({path})")
            continue
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError as error:
            problems.append(f"{path}: {error}")
            continue
        failures = data.get("failures") or []
        if failures:
            problems.extend(f"{platform}: {failure}" for failure in failures)
        results = data.get("results") or []
        present = {(item["library"], item["workload"]) for item in results}
        for library in LIBRARY_ORDER:
            for workload in WORKLOADS:
                if (library, workload) not in present:
                    problems.append(f"{platform}: missing {library} {workload}")
        for item in results:
            entries.append(
                {
                    "platform": platform,
                    "library": item["library"],
                    "workload": item["workload"],
                    "value": float(item["value"]),
                    "stddev": float(item.get("stddev") or 0.0),
                    "unit": item.get("unit", ""),
                }
            )
    return entries, problems


def grouped_chart(entries, workload, output, title, ylabel, scale):
    rows = [entry for entry in entries if entry["workload"] == workload]
    platforms = sorted({entry["platform"] for entry in rows})
    libraries = [name for name in LIBRARY_ORDER if any(r["library"] == name for r in rows)]
    lookup = {(entry["platform"], entry["library"]): entry for entry in rows}

    width = 0.8 / max(len(libraries), 1)
    figure, axes = plt.subplots(figsize=(max(9.0, 2.4 * len(platforms) + 3.0), 5.0))
    positions = list(range(len(platforms)))
    for index, library in enumerate(libraries):
        heights = [
            lookup[(platform, library)]["value"] * scale
            if (platform, library) in lookup
            else 0.0
            for platform in platforms
        ]
        errors = [
            lookup[(platform, library)]["stddev"] * scale
            if (platform, library) in lookup
            else 0.0
            for platform in platforms
        ]
        offsets = [
            position + (index - (len(libraries) - 1) / 2) * width for position in positions
        ]
        bars = axes.bar(
            offsets,
            heights,
            width,
            yerr=errors,
            capsize=2,
            error_kw={"elinewidth": 0.8, "alpha": 0.7},
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


def row_markdown(entries, workload, formatter):
    lines = ["| Platform | Library | median | stddev |", "|----------|---------|------:|------:|"]
    rows = sorted(
        (entry for entry in entries if entry["workload"] == workload),
        key=lambda entry: (
            entry["platform"],
            LIBRARY_ORDER.index(entry["library"])
            if entry["library"] in LIBRARY_ORDER
            else len(LIBRARY_ORDER),
        ),
    )
    for entry in rows:
        lines.append(
            f"| {entry['platform']} | {entry['library']} | "
            f"{formatter(entry['value'])} | {formatter(entry['stddev'])} |"
        )
    return lines


def markdown(entries):
    lines = [
        "## IPC benchmark comparison",
        "",
        "Medians with standard deviation over the CI matrix. Lower latency and "
        "higher throughput are better. `ipc-channel` is measured with `Vec<u8>` "
        "payloads, so its numbers include serde/postcard serialization and "
        "deserialization; MapleScenary sends raw bytes. `reserve` is the "
        "zero-copy API, `2 proc` is a real child process reached through "
        "`Listener`/`connect` (while every other row runs both endpoints in one "
        "process). The receiver always copies or checksums the payload. Runner "
        "hardware differs per platform, so compare libraries inside a row, not "
        "absolute numbers across rows.",
        "",
        "### Latency (64 B roundtrip)",
        "",
    ]
    lines += row_markdown(entries, "latency-64B", lambda value: f"{value / 1000:.3f} µs")
    lines += [
        "",
        "### Throughput (64 KiB messages)",
        "",
    ]
    lines += row_markdown(entries, "throughput-64KiB", lambda value: f"{value:.1f} MiB/s")
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
    args = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
    required = []
    for arg in sys.argv[1:]:
        if arg.startswith("--platforms="):
            required = [name for name in arg.split("=", 1)[1].split(",") if name]
    if not required:
        return fail("pass --platforms=name,name,name so missing data cannot pass")
    results_dir = args[0] if args else "bench-results"
    output_dir = pathlib.Path(args[1] if len(args) > 1 else "bench-report")
    output_dir.mkdir(parents=True, exist_ok=True)

    entries, problems = load(results_dir, required)
    if problems:
        for problem in problems:
            print(f"error: {problem}", file=sys.stderr)
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
