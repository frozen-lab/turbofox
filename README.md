# TurboCache

A persistant and embedded KV Database built for on-device caching.

## Benchmarks

| Operation  | Mean (µs) | p50 (µs) | p90 (µs) | p99 (µs) | StdDev (µs)  | Throughput (ops/s) |
|:----------:|:---------:|:--------:|:--------:|:--------:|:------------:|:------------------:|
| set        |    31.959 |   30.727 |   36.012 |   37.224 |        2.734 |              31290 |
| get        |   127.445 |  102.141 |  169.226 |  171.128 |       33.368 |               7847 |
| del        |   110.991 |   97.655 |  122.284 |  217.316 |       39.283 |               9010 |
