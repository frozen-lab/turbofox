import json
import math


def percentile(data, pct):
    """Compute percentile (e.g. 50, 90, 99) for a sorted list."""
    if not data:
        return None
    k = (len(data) - 1) * (pct / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data[int(k)]
    return data[int(f)] * (c - k) + data[int(c)] * (k - f)


def mean(data):
    return sum(data) / len(data) if data else 0.0


def stddev(data, m=None):
    if not data:
        return 0.0
    if m is None:
        m = mean(data)
    var = sum((x - m) ** 2 for x in data) / len(data)
    return math.sqrt(var)


# ---- Load Criterion sample.json ----
with open("target/criterion/get/hit80_miss20/new/sample.json") as f:
    data = json.load(f)

times = data["times"]
iters = data["iters"]

# Compute per-operation latency in microseconds
latencies_us = [(t / i) / 1000 for t, i in zip(times, iters)]

# Sort for percentile calculation
latencies_us.sort()

# ---- Compute stats ----
m = mean(latencies_us)
stats = {
    "mean": m,
    "p50": percentile(latencies_us, 50),
    "p90": percentile(latencies_us, 90),
    "p99": percentile(latencies_us, 99),
    "stddev": stddev(latencies_us, m),
    "throughput": 1_000_000 / m if m > 0 else 0.0,
}

# ---- Print results ----
for k, v in stats.items():
    print(f"{k:10s}: {v:.3f}")
