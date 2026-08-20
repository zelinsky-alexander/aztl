# Concurrency design notes

This document defines the contracts the implementations should satisfy. It intentionally does not contain a finished lock-free algorithm.

## 1. SPSC ring buffer

**Threads:** exactly one producer and one consumer.

**Target properties:** bounded capacity, FIFO, no dynamic allocation in steady state, non-blocking `try_push`/`try_pop`, correct construction/destruction of `T`.

Questions the implementation should answer explicitly:

- Which index is exclusively written by the producer? Which by the consumer?
- What data must become visible before the producer publishes a new write position?
- Which load must acquire that publication before the consumer reads the object?
- Which operations can safely be `memory_order_relaxed`, and why?
- How are full and empty distinguished at wrap-around?
- Are producer/consumer indices separated to reduce false sharing?
- What is the precise validity of `empty()` when another thread is active?

A typical proof shape is a release publication by one side paired with an acquire observation by the other. Do not copy memory orders mechanically: document the happens-before edge your implementation needs.

## 2. MPMC ring buffer

**Threads:** multiple producers and multiple consumers.

The MPMC implementation is intentionally later because merely replacing SPSC indices with `fetch_add` is not sufficient. Producers and consumers must coordinate ownership/publication of individual slots as well as global progress.

Before coding, write down:

- How a thread claims a slot without another producer/consumer using it simultaneously.
- How a slot transitions between free, being-written, ready, being-read, and reusable states.
- How wrap-around avoids confusing a new generation of a slot with an old one.
- Whether the chosen algorithm is lock-free, wait-free, or merely non-blocking in some paths; use the standard terminology precisely.
- Whether `T` construction/destruction can throw and what guarantee the container provides if it does.

A bounded per-slot sequence/generation design is one reasonable family of algorithms to research from primary literature/specifications, but the implementation here should be independently written and its invariants documented.

## 3. Triple buffer

**Threads:** one writer and one reader.

The intended use case is a latest-frame/latest-state handoff: the writer must keep making progress without modifying the buffer currently owned by the reader, and the reader should acquire the newest completely published value available when it asks.

API scaffold:

```cpp
T& producer_buffer();
void publish();
const T& consume_latest();
```

Define these semantics before implementation:

- Writer exclusively modifies only the producer buffer returned to it.
- `publish()` makes all completed writes to that buffer visible to a later successful consumer acquisition.
- Reader never observes a partially updated object.
- Writer never reuses the buffer currently held by the reader.
- State clearly how long the reference from `consume_latest()` remains valid.

## Correctness test strategy

The initial suites cover:

- basic FIFO behavior;
- empty behavior and capacity;
- ordered SPSC transfer under thread contention;
- MPMC loss/duplication detection using disjoint producer value ranges;
- latest-value triple-buffer publication.

Extend them with wrap-around, full-capacity behavior, move-only values, object lifetime counters, repeated randomized stress, and shutdown/destruction cases as implementations appear.

Run sanitizer configurations separately. ThreadSanitizer is especially valuable for accidental data races, but a clean TSan run is not a proof that a lock-free algorithm is correct under the C++ memory model.

## Benchmark policy

Performance measurements should come only after correctness tests pass. At minimum record compiler/version, optimization level, CPU/OS, queue capacity, producer/consumer count, operation count, and repeated-run distribution. Compare against a simple mutex-backed bounded queue as a practical baseline.

Do not claim universal speedups from CI-hosted measurements; shared CI hardware is best used for regressions, not authoritative latency numbers.
