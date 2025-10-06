# ruff: noqa

"""Script to run and analyze benchmarks, and create MD table.

```bash
python3 run_bench.py --sample 64
```
"""

import argparse
import json
import logging
import math
import subprocess
from pathlib import Path

# ---
# Logging setup
# ---

logging.basicConfig(level=logging.DEBUG, format="[%(levelname)s] %(message)s")

# ---
# Paths
# ---

bench_md = Path("BENCH_RESULTS.md")

BENCHES = {
    "set": Path("target/criterion/set/variable_kv/new/sample.json"),
    "get": Path("target/criterion/get/rng_hit_miss/new/sample.json"),
    "del": Path("target/criterion/del/rng_hit_miss/new/sample.json"),
}

# ---
# Utils
# ---


def percentile(data, pct):
    if not data:
        return 0
    k = (len(data) - 1) * (pct / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data[int(k)]
    return data[f] * (c - k) + data[c] * (k - f)


def mean(data):
    return sum(data) / len(data) if data else 0


def stddev(data, m=None):
    if not data:
        return 0
    if m is None:
        m = mean(data)
    var = sum((x - m) ** 2 for x in data) / len(data)
    return math.sqrt(var)


# ---
# Args
# ---

parser = argparse.ArgumentParser(description="Run and process TurboCache benchmarks.")
parser.add_argument("--sample", type=int, default=64, help="Criterion sample size")
parser.add_argument("--debug", type=bool, default=False, help="Is debug mode")
args = parser.parse_args()

# ---
# Run benches
# ---

logging.info(f"Running benches with sample size {args.sample}...")

result = subprocess.run(
    ["cargo", "bench", "--bench", "bench", "--", f"--sample-size={args.sample}"],
    capture_output=True,
    text=True,
)

if result.returncode != 0:
    logging.error("Benchmarks failed!")
    logging.error(result.stdout)
    logging.error(result.stderr)

    exit(1)

logging.info("Benchmarks completed!")

# ---
# Parse benchmarks
# ---

logging.info("Parsing benchmark output files...")
bench_stats = []

for name, path in BENCHES.items():
    if not path.exists():
        logging.warning(f"Missing: {path}")
        continue

    data = json.loads(path.read_text())

    times = data["times"]
    iters = data["iters"]

    # Convert total time to per-iteration µs
    latencies = [(t / i) / 1000 for t, i in zip(times, iters)]
    latencies.sort()
    m = mean(latencies)

    stats = {
        "name": name,
        "mean": m,
        "p50": percentile(latencies, 50),
        "p90": percentile(latencies, 90),
        "p99": percentile(latencies, 99),
        "stddev": stddev(latencies, m),
        "throughput": 1_000_000 / m if m > 0 else 0,
    }

    bench_stats.append(stats)

# ---
# Gen Markdown
# ---

md_lines = [
    "| Operation | Mean (µs) | p50 (µs) | p90 (µs) | p99 (µs) | StdDev (µs) | Throughput (ops/s) |",
    "|:---------:|:---------:|:--------:|:--------:|:--------:|:------------:|:------------------:|",
]

for b in bench_stats:
    md_lines.append(
        f"| {b['name']:<10} | {b['mean']:>8.3f} | {b['p50']:>7.3f} | {b['p90']:>7.3f} | {b['p99']:>7.3f} | {b['stddev']:>10.3f} | {b['throughput']:>15.0f} |"
    )

md = "\n".join(md_lines)

# ---
# Save result
# ---

if not args.debug:
    bench_md.write_text(md)
    logging.info(f"Markdown table saved to {bench_md}")

logging.debug(md)
