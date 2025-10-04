# TurboCache

A persistant and embedded KV Database built for on-device caching.

## Benchmarks

| Operation  | Mean (µs) | p50 (µs) | Throughput (ops/s) |
|:----------:|:---------:|:--------:|:------------------:|
| del_hit    |     0.137 |    0.131 |            7288630 |
| del_miss   |     0.950 |    0.186 |            1052189 |
| get_hit    |     0.672 |    0.628 |            1488982 |
| get_miss   |     0.908 |    0.186 |            1100715 |
| set_large  |     6.315 |    5.027 |             158353 |
| set_small  |     3.237 |    1.489 |             308928 |

