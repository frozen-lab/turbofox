# ruff: noqa: LOG015, S404, S607, RUF003, RUF001, D103

"""Script to run and analyze benchmarks, and create MD table.

```bash
uv run bench.py
```
"""

import logging
import re
import subprocess
from pathlib import Path

# ---
# Setup logging
# ---


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


# ---
# Paths
# ---


bench_md = Path("BENCH_RESULTS.md")


# ---
# Run benches
# ---


logging.info("Running Divan benches...")

result = subprocess.run(
    ["cargo", "bench", "--bench", "bench", "--quiet"],
    capture_output=True,
    text=True,
)

if result.returncode != 0:
    logging.error("Benchmarks failed!")
    logging.error(result.stdout)
    logging.error(result.stderr)
    exit(1)

logging.info("Benchmarks completed.")

stdout_lines = result.stdout.splitlines()


# ---
# Parse results
# ---


def parse_value(s: str) -> float:
    if "ns" in s:
        return float(s.replace("ns", "").strip()) / 1000  # ns → µs

    if "µs" in s:
        return float(s.replace("µs", "").strip())  # µs stays

    return float(s.strip())


pattern = re.compile(
    r"[├╰]─\s*(\w+)\s+([\d\.]+ [µn]s)\s+│\s*([\d\.]+ [µn]s)\s+│\s*([\d\.]+ [µn]s)\s+│\s*([\d\.]+ [µn]s)"
)

bench_stats = []
logging.info("Parsing benchmark JSON...")

for line in stdout_lines:
    match = pattern.search(line)
    if match:
        name = match.group(1)
        fastest = parse_value(match.group(2))
        slowest = parse_value(match.group(3))
        median = parse_value(match.group(4))
        mean = parse_value(match.group(5))
        throughput = 1_000_000 / mean

        bench_stats.append(
            {
                "name": name,
                "mean": mean,
                "p50": median,
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
    "| Operation | Mean (µs) | p50 (µs) | Throughput (ops/s) |",
    "|:---------:|:---------:|:--------:|:------------------:|",
]

for b in bench_stats:
    tp_display = f"{b['throughput']:.0f}"

    md_lines.append(
        f"| {b['name']:<10} | {b['mean']:>8.3f} | {b['p50']:>7.3f} | {tp_display:>15} |"
    )

md_content = "\n".join(md_lines)

# ---
# Save result
# ---

bench_md.write_text(md_content)
logging.info(f"Markdown table saved to {bench_md}")

print(md_content)
