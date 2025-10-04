# ruff: noqa: LOG015, S404, S603, S607, RUF003, RUF001

"""Script to run and analyze benchmarks, and create MD table.

```bash
uv run bench.py
```
"""

import json
import logging
import subprocess
from pathlib import Path

# ---
# Setup logging
# ---

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

# ---
# Paths
# ---

bench_json = Path("bench_output.json")
bench_md = Path("BENCH_RESULTS.md")

# ---
# Run benches
# ---

logging.info("Running Divan benches...")

result = subprocess.run(
    ["cargo", "bench", "--", "--save-json", str(bench_json)],
    capture_output=True,
    text=True,
)

if result.returncode != 0:
    logging.error("Benchmarks failed!")
    logging.error(result.stdout)
    logging.error(result.stderr)

    exit(1)

logging.info("Benchmarks completed and JSON saved.")

# ---
# Load result
# ---

logging.info("Parsing benchmark JSON...")

with bench_json.open() as f:
    data = json.load(f)

# ---
# Compute result
# ---

bench_stats = []

for bench in data["benches"]:
    name = bench["name"]

    times_us = sorted(s["time_ns"] / 1000 for s in bench["samples"])  # ns -> µs
    mean = sum(times_us) / len(times_us)

    p50 = times_us[int(0.50 * len(times_us))]
    p95 = times_us[int(0.95 * len(times_us))]
    p99 = times_us[int(0.99 * len(times_us))]

    throughput = 1_000_000 / mean

    bench_stats.append(
        {
            "name": name,
            "mean": mean,
            "p50": p50,
            "p95": p95,
            "p99": p99,
            "throughput": throughput,
        }
    )

# ---
# Fastest and slowest from res
# ---

fastest = max(bench_stats, key=lambda b: b["throughput"])["throughput"]
slowest = min(bench_stats, key=lambda b: b["throughput"])["throughput"]

# ---
# Gen Markdown
# ---

md_lines = [
    "| Operation | Mean (µs) | p50 (µs) | p95 (µs) | p99 (µs) | Throughput (ops/s) |",
    "|:---------:|:---------:|:--------:|:--------:|:--------:|:------------------:|",
]

for b in bench_stats:
    if b["throughput"] == fastest:
        tp_display = f"⚡💛 {b['throughput']:.0f}"

    elif b["throughput"] == slowest:
        tp_display = f"💙 {b['throughput']:.0f}"

    else:
        tp_display = f"{b['throughput']:.0f}"

    md_lines.append(
        f"| {b['name']:<10} | {b['mean']:>8.3f} | {b['p50']:>7.3f} | "
        f"{b['p95']:>7.3f} | {b['p99']:>7.3f} | {tp_display:>15} |"
    )

md_content = "\n".join(md_lines)

# ---
# Save result
# ---

bench_md.write_text(md_content)
logging.info(f"Markdown table saved to {bench_md}")

print(md_content)
