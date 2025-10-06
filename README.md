# TurboCache

A persistant and embedded KV Database built for on-device caching.

## Benchmarks

| Operation  | Mean (µs) | p50 (µs) | p90 (µs) | p99 (µs) | StdDev (µs)  | Throughput (ops/s) |
|:----------:|:---------:|:--------:|:--------:|:--------:|:------------:|:------------------:|
| set        |    25.590 |   24.425 |   29.081 |   38.993 |        3.659 |              39078 |
| get        |    26.197 |   26.106 |   27.235 |   28.082 |        0.860 |              38173 |
| del        |    26.055 |   26.109 |   27.477 |   31.208 |        1.854 |              38380 |

