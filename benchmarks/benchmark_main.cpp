#include <aztl/mpmc_ring_buffer.hpp>
#include <aztl/spsc_ring_buffer.hpp>
#include <aztl/triple_buffer.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>

namespace {

using clock_type = std::chrono::steady_clock;

void print_result(const char* name, std::size_t operations, clock_type::duration elapsed) {
    const double seconds = std::chrono::duration<double>(elapsed).count();
    const double ops_per_second = static_cast<double>(operations) / seconds;
    std::cout << std::left << std::setw(30) << name
              << std::right << std::fixed << std::setprecision(0)
              << ops_per_second << " ops/s\n";
}

template <class Queue>
void benchmark_spsc(const char* name, std::size_t count) {
    Queue q;
    const auto start = clock_type::now();

    std::jthread producer([&] {
        for (std::size_t value = 0; value < count;) {
            if (q.try_push(value)) {
                ++value;
            } else {
                std::this_thread::yield();
            }
        }
    });

    std::jthread consumer([&] {
        for (std::size_t received = 0; received < count;) {
            if (q.try_pop()) {
                ++received;
            } else {
                std::this_thread::yield();
            }
        }
    });

    producer.join();
    consumer.join();
    print_result(name, count, clock_type::now() - start);
}

void benchmark_mutex_queue(std::size_t count) {
    std::deque<std::size_t> queue;
    std::mutex mutex;
    const auto start = clock_type::now();

    std::jthread producer([&] {
        for (std::size_t value = 0; value < count; ++value) {
            for (;;) {
                std::scoped_lock lock(mutex);
                if (queue.size() < 1024) {
                    queue.push_back(value);
                    break;
                }
            }
        }
    });

    std::jthread consumer([&] {
        for (std::size_t received = 0; received < count;) {
            std::scoped_lock lock(mutex);
            if (!queue.empty()) {
                queue.pop_front();
                ++received;
            }
        }
    });

    producer.join();
    consumer.join();
    print_result("mutex + deque baseline", count, clock_type::now() - start);
}

template <class Queue>
void benchmark_mpmc(const char* name, std::size_t per_producer) {
    constexpr std::size_t workers = 4;
    Queue q;
    std::atomic<std::size_t> consumed{0};
    const std::size_t total = workers * per_producer;
    const auto start = clock_type::now();
    std::jthread threads[workers * 2];

    for (std::size_t p = 0; p < workers; ++p) {
        threads[p] = std::jthread([&, p] {
            for (std::size_t i = 0; i < per_producer;) {
                if (q.try_push(p * per_producer + i)) {
                    ++i;
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (std::size_t c = 0; c < workers; ++c) {
        threads[workers + c] = std::jthread([&] {
            while (consumed.load(std::memory_order_relaxed) < total) {
                if (q.try_pop()) {
                    consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    print_result(name, total, clock_type::now() - start);
}

template <class Buffer>
void benchmark_triple_buffer(const char* name, std::size_t count) {
    Buffer buffer;
    std::atomic<bool> writer_done{false};
    std::atomic<std::size_t> observed{0};
    const auto start = clock_type::now();

    std::jthread writer([&] {
        for (std::size_t value = 1; value <= count; ++value) {
            buffer.producer_buffer() = value;
            buffer.publish();
        }
        writer_done.store(true, std::memory_order_release);
    });

    std::jthread reader([&] {
        std::size_t last = 0;
        while (!writer_done.load(std::memory_order_acquire) || last < count) {
            const auto current = buffer.consume_latest();
            if (current > last) {
                last = current;
                observed.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    writer.join();
    reader.join();
    print_result(name, count, clock_type::now() - start);
    std::cout << "  reader observed " << observed.load(std::memory_order_relaxed)
              << " distinct publications; dropped intermediate values are expected\n";
}

} // namespace

int main() {
    constexpr std::size_t count = 2'000'000;
    std::cout << "AZTL microbenchmark scaffold\n"
              << "Run on an otherwise idle machine; compare repeated runs, not one-off numbers.\n\n";

    benchmark_mutex_queue(count);

    if constexpr (aztl::spsc_ring_buffer<std::size_t, 1024>::implemented) {
        benchmark_spsc<aztl::spsc_ring_buffer<std::size_t, 1024>>("aztl SPSC ring", count);
    } else {
        std::cout << "SKIP aztl SPSC ring: implementation pending\n";
    }

    if constexpr (aztl::mpmc_ring_buffer<std::size_t, 1024>::implemented) {
        benchmark_mpmc<aztl::mpmc_ring_buffer<std::size_t, 1024>>("aztl MPMC ring (4P/4C)", count / 4);
    } else {
        std::cout << "SKIP aztl MPMC ring: implementation pending\n";
    }

    if constexpr (aztl::triple_buffer<std::size_t>::implemented) {
        benchmark_triple_buffer<aztl::triple_buffer<std::size_t>>("aztl triple buffer", count);
    } else {
        std::cout << "SKIP aztl triple buffer: implementation pending\n";
    }
}
