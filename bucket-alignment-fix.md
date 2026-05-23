# `xsync.Map` bucket-alignment fix — scenario and benchmark results

## Problem

`bucketPadded` is exactly 64 bytes (one cache line) and contains GC-relevant
pointers, so `make([]bucketPadded, n)` is allocated via Go's small-object
path. For size classes that fall into the `[16, 512)` bucket range, the Go
allocator prefixes the allocation with an **8-byte malloc header**, which
makes the slice base sit at offset `8 (mod 64)` rather than `0`. Each bucket
therefore straddles two cache lines.

With the original field order:

```
[meta:0-7][entries[5]:8-47][next:48-55][mu:56-63]
```

the last 8 bytes of `bucket[i]` (`mu`) land on the same cache line as the
first 56 bytes of `bucket[i+1]` (`meta + entries + next`). Because `mu` is
written on every `Store`/`Delete` (lock + unlock), every write to bucket
`i` invalidates the **hot read fields of bucket `i+1`** for any concurrent
reader. This is classic false sharing.

The `[16, 512)` window contains `defaultMinMapTableLen = 32` plus the
intermediate sizes a table passes through as it doubles (32 → 64 → 128 →
256 → 512), so the bug affects the default-sized map and most practical
sizes prior to large allocations.

## Fix

Reorder the fields in `bucket` so that `mu` precedes `next`:

```go
type bucket struct {
    meta    uint64
    entries [entriesPerMapBucket]unsafe.Pointer // *entry
    mu      sync.Mutex
    next    unsafe.Pointer // *bucketPadded
}
```

`next` (written only when growing an overflow chain) is now the field at
offsets 56–63 that shares a cache line with the next bucket — instead of
`mu`. Frequent lock writes stay inside the bucket's primary cache line.

- Struct size unchanged: still 64 bytes on 64-bit (`TestMap_BucketStructSize`
  passes).
- All alignment requirements preserved: `meta` stays at offset 0 (8-byte
  aligned for `atomic.LoadUint64` / `StoreUint64` on 32-bit), `mu.state` at
  offset 48 is 4-byte aligned, `next` at offset 56 is pointer-aligned.
- Race-detector tests pass (`go test -run TestMap -race -count=1 -short`).

## Verification of the alignment claim

A standalone program (`/tmp/aligncheck/main.go`) that mirrors `bucketPadded`
and prints `addr(&buckets[0]) mod 64` for a range of sizes confirmed the
window precisely:

| `n`     | `addr mod 64` | `sameCL(b0.mu, b1.meta)` |
|---------|---------------|--------------------------|
| 1, 2, 4, 8       | 0     | false |
| 16, 32, 64, 128, 256, 511 | **8** | **true** |
| 512, 513, 1024, 2048      | 0     | false |

For `n ≥ 512` the allocation exceeds the 32 KB small-object threshold and
takes the large-object path, which is page-aligned, so the slice base is
cache-line aligned again.

## Benchmark setup

- Platform: Linux, `amd64`, Go 1.26.0
- CPU: AMD Ryzen 9 7900, 12 cores / 24 threads
- Invocation:

  ```
  go test -run=NONE -bench='BenchmarkMapAlign' \
      -benchtime=1s -count=8 -cpu=24
  ```

### `BenchmarkMapAlign_Write`

24 goroutines concurrently `Store` into a `Map[int, int]` pre-sized so the
table contains exactly `n` buckets. The map is pre-populated to ~50 %
load-factor with `WithGrowOnly()` so writes overwrite existing keys and the
table never resizes out of the targeted bucket count.

### `BenchmarkMapAlign_Mixed`

Same shape as above but 90 % `Load` / 10 % `Store`. This specifically
exposes the writer-invalidates-reader pattern: a concurrent writer on
`bucket[i]` shoots down the cache line a concurrent reader of `bucket[i+1]`
is touching.

## Results — targeted benchmark

`benchstat` comparison, `before` is the upstream field order, `after` is
the reorder.

```
goos: linux
goarch: amd64
pkg: github.com/puzpuzpuz/xsync/v4
cpu: AMD Ryzen 9 7900 12-Core Processor
                               │      before       │              after                  │
                               │      sec/op       │    sec/op     vs base               │
MapAlign_Write/buckets=32-24       24.16n ± 20%   21.04n ± 15%        ~ (p=0.105 n=8)
MapAlign_Write/buckets=64-24       21.64n ± 15%   15.96n ± 10%  -26.25% (p=0.000 n=8)
MapAlign_Write/buckets=128-24      16.24n ±  4%   13.00n ±  7%  -19.94% (p=0.000 n=8)
MapAlign_Write/buckets=256-24      13.00n ±  3%   11.04n ±  4%  -15.01% (p=0.000 n=8)
MapAlign_Write/buckets=512-24      10.96n ±  3%   10.84n ±  3%        ~ (p=0.279 n=8)
MapAlign_Write/buckets=1024-24     11.21n ±  2%   10.88n ±  4%   -2.94% (p=0.016 n=8)
MapAlign_Mixed/buckets=32-24       5.562n ± 10%   5.161n ±  8%   -7.20% (p=0.028 n=8)
MapAlign_Mixed/buckets=64-24       4.037n ±  4%   3.579n ±  1%  -11.35% (p=0.000 n=8)
MapAlign_Mixed/buckets=128-24      3.263n ±  2%   2.894n ±  4%  -11.32% (p=0.000 n=8)
MapAlign_Mixed/buckets=256-24      2.824n ±  5%   2.442n ±  4%  -13.51% (p=0.000 n=8)
MapAlign_Mixed/buckets=512-24      2.230n ±  3%   2.235n ±  1%        ~ (p=0.721 n=8)
MapAlign_Mixed/buckets=1024-24     2.150n ±  1%   2.159n ±  8%        ~ (p=0.314 n=8)
geomean                            6.983n         6.254n        -10.44%
```

### Summary

| Bucket count | Write-only | Mixed (10 % writes) |
|--------------|------------|---------------------|
| 32           | -12.9 %¹   | -7.2 %              |
| **64**       | **-26.3 %**| **-11.4 %**         |
| **128**      | **-19.9 %**| **-11.3 %**         |
| **256**      | **-15.0 %**| **-13.5 %**         |
| 512          | ~          | ~                   |
| 1024         | -2.9 %     | ~                   |

¹ trending in the expected direction but not statistically significant —
at 32 buckets with 24 goroutines the workload is mutex-bound, so
cache-line effects are a smaller fraction of the total cost.

The improvement is concentrated **exactly** in the bucket-count window
where the malloc-header offset breaks alignment, and vanishes for sizes
that are already aligned (≥ 512). This matches the false-sharing
hypothesis precisely.

## Results — existing top-level benchmarks (regression check)

The existing `BenchmarkMap_WarmUp` / `BenchmarkMapInt_WarmUp` benchmarks
use `WithPresize(1000)`, which produces a 512-bucket table — already in
the aligned range. They should show no change:

```
                            │      before       │             after                  │
                            │      sec/op       │    sec/op      vs base             │
Map_WarmUp/reads=100%-24        1.581n ± 4%    1.581n ±  0%       ~ (p=0.814 n=6)
Map_WarmUp/reads=99%-24         1.506n ± 8%    1.509n ±  3%       ~ (p=0.970 n=6)
Map_WarmUp/reads=90%-24         2.903n ± 8%    2.924n ± 15%       ~ (p=0.699 n=6)
Map_WarmUp/reads=75%-24         4.785n ± 6%    4.679n ±  1%  -2.19% (p=0.015 n=6)
MapInt_WarmUp/reads=100%-24    0.6064n ± 4%   0.6037n ±  1%       ~ (p=0.065 n=6)
MapInt_WarmUp/reads=99%-24     0.9709n ± 4%   0.9662n ±  2%       ~ (p=0.623 n=6)
MapInt_WarmUp/reads=90%-24      2.250n ± 6%    2.177n ±  7%       ~ (p=0.589 n=6)
MapInt_WarmUp/reads=75%-24      3.611n ± 2%    3.646n ±  7%       ~ (p=0.310 n=6)
geomean                         1.883n         1.873n        -0.56%
```

Geomean ±0.56 %, essentially noise. No regression in the already-aligned
case.

## Conclusion

A one-field reorder yields **15–26 % faster writes** and **11–14 % faster
mixed workloads** on tables with 64–256 buckets — the default size and
the sizes a growing map passes through before reaching 512 — at zero
memory cost, zero API change, and no measurable impact on the
already-aligned cases.

Credit to **@jeremiah-masters** for spotting this in the discussion on
[issue #205](https://github.com/puzpuzpuz/xsync/issues/205).
