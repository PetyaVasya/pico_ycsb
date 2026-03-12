# pico_ycsb

YCSB-style benchmark suite for [Tarantool](https://www.tarantool.io/) Vinyl,
the LSM-tree storage engine.

Used to produce the benchmark results in
[Как мы пересобрали сборку мусора в Vinyl](https://kostja.github.io/tarantool/vinyl/2026/03/11/vinyl-compaction-scheduler.html).

## Workloads

| Workload | Pattern | Purpose |
|----------|---------|---------|
| **A** | 50% read / 50% update, Zipfian | Hot-key contention, overlapping runs, bloom filter stress |
| **B** | 50% sequential insert / 50% read | Time-series append, non-overlapping run detection |
| **C** | 30% delete / 20% insert / 30% scan / 20% read | Tombstone accumulation, read-amp driven compaction |

## Quick start

```bash
./run_all.sh /path/to/tarantool my_test
```

Results go to `/var/opt/bench/my_test_{a,b,c}/`.

## Usage

```
./run_all.sh <tarantool_binary> <prefix> [scale]
```

- **tarantool_binary** -- path to the Tarantool executable
- **prefix** -- name for the result directory
- **scale** -- data size multiplier (default: 10)

| Scale | Keys | Data size |
|-------|------|-----------|
| 1     | 2M   | ~200 MB   |
| 10    | 20M  | ~2 GB     |
| 100   | 200M | ~20 GB    |

Set `VINYL_CACHE` to control the Vinyl page cache size (default: 0).

## Running individual workloads

```bash
tarantool vinyl_workload_a.lua [minutes] [scale] [batch_size]
tarantool vinyl_workload_b.lua [minutes] [scale]
tarantool vinyl_workload_c.lua [minutes] [scale]
```

## Metrics

Every 10 seconds the benchmarks report:

- **Throughput** (RPS)
- **Write amplification** (bytes written to disk / bytes dumped from memory)
- **Space amplification** (disk usage / live data)
- **Read amplification** (bytes read from disk / bytes returned to user)
- Compaction I/O, bloom filter hit rates, cache statistics

## Requirements

- Tarantool with Vinyl support (2.11+ recommended)
- Enough disk space for the chosen scale factor

## License

BSD-2-Clause
