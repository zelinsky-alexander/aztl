# AZTL

**Alexander Zelinsky Template Library** — a small, dependency-free C++23 library for focused low-level templates and utilities.

The first milestone is intentionally narrow: three concurrency primitives that are easy to use incorrectly and therefore worth implementing, testing, documenting, and benchmarking carefully.

- `aztl::spsc_ring_buffer<T, Capacity>` — bounded single-producer/single-consumer ring buffer.
- `aztl::mpmc_ring_buffer<T, Capacity>` — bounded multi-producer/multi-consumer ring buffer.
- `aztl::triple_buffer<T>` — single-writer/single-reader latest-value handoff using three buffers.

The repository is currently an **implementation scaffold**. Public APIs, correctness tests, benchmark drivers, build flow, and design notes are provided; the lock-free algorithms themselves are deliberately left as exercises.

## Goals

- C++23, header-only library code.
- No runtime allocation in steady-state ring-buffer operations.
- Explicit thread-ownership and memory-ordering documentation.
- Correct object lifetime handling, including move-only and non-default-constructible values where the API permits it.
- Correctness/stress tests before performance claims.
- Reproducible microbenchmarks with results treated as measurements, not guarantees.

## Build

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The scaffold keeps implementation-dependent correctness suites skipped until each primitive's `implemented` flag is set to `true`. Once you implement a primitive, flip that flag and the relevant tests become active.

Build benchmarks:

```bash
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DAZTL_BUILD_BENCHMARKS=ON
cmake --build build-bench -j
./build-bench/benchmarks/aztl_benchmarks
```

## Suggested implementation order

1. SPSC ring buffer: establish ownership, storage lifetime, acquire/release publication, wrap-around, and false-sharing strategy.
2. Triple buffer: reason about latest-frame handoff and preventing writer/reader collision.
3. MPMC ring buffer: only after the testing/benchmark infrastructure is trustworthy; document the algorithm and every synchronization edge.

See [`docs/concurrency.md`](docs/concurrency.md) for the intended contracts and memory-model questions to answer while implementing.

## Project status

This is an educational/open-source implementation project. No performance or lock-freedom claims should be made until the corresponding implementation, stress tests, sanitizer runs, and benchmarks are complete.

## License

MIT. See [`LICENSE`](LICENSE).
