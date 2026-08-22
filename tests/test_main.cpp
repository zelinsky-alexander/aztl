#include <aztl/mpmc_ring_buffer.hpp>
#include <aztl/spsc_ring_buffer.hpp>
#include <aztl/triple_buffer.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>
#include <iostream>
#include <format>
#include <cstdint>


namespace {

int failures = 0;

void check(bool condition, const char* expression, const char* test) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL [" << test << "]: " << expression << '\n';
    }
}

#define AZTL_CHECK(expr) check(static_cast<bool>(expr), #expr, __func__)

template <class Queue>
void test_basic_fifo() {

    std::cout << "Start test_basic_fifo" << std::endl;

    Queue q;
    
    AZTL_CHECK(q.capacity() == 8);
    AZTL_CHECK(q.try_push(10));
    AZTL_CHECK(q.try_push(20));
    auto a = q.try_pop();
    auto b = q.try_pop();
    AZTL_CHECK(a && *a == 10);
    AZTL_CHECK(b && *b == 20);
    AZTL_CHECK(!q.try_pop());
}

void test_spsc_concurrent_order() {

    constexpr int bsize = 1000;
    constexpr int count = 100'000;

    std::cout << "\nStart test_spsc_concurrent_order (producer) count=" << count << " buffer size=" << bsize << std::endl;

    aztl::spsc_ring_buffer<int, bsize> q;
    std::atomic<bool> start{false};
    std::atomic<bool> ordered{true};

    std::jthread producer([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int value = 0; value < count;) {
            if (q.try_push(value)) {
                ++value;
            } else {
                std::this_thread::yield();
            }
        }
    });

    std::cout << "=== start consumer thread" << std::endl;

    std::jthread consumer([&] {
        start.store(true, std::memory_order_release);
        for (int expected = 0; expected < count;) {
            if (auto value = q.try_pop()) {
                if (*value != expected) {
                    ordered.store(false, std::memory_order_relaxed);
                }
                ++expected;
            } else {
                std::this_thread::yield();
            }
        }
    });

    std::cout << "=== wait for producer and consumer" << std::endl;
    producer.join();
    consumer.join();
    std::cout << "=== producer and consumer joined, q size is " << q.count() << std::endl;
    AZTL_CHECK(ordered.load(std::memory_order_relaxed));
    AZTL_CHECK(q.count() == 0);
    AZTL_CHECK(q.empty());
}

void test_mpmc_exactly_once() {
    constexpr int producers = 4;
    constexpr int consumers = 4;
    constexpr int per_producer = 25'000;
    constexpr int total = producers * per_producer;

    aztl::mpmc_ring_buffer<int, 1024> q;
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};
    std::vector<std::vector<int>> outputs(static_cast<std::size_t>(consumers));
    std::vector<std::jthread> threads;

    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&, p] {
            const int begin = p * per_producer;
            const int end = begin + per_producer;
            for (int value = begin; value < end;) {
                if (q.try_push(value)) {
                    ++value;
                    produced.fetch_add(1, std::memory_order_relaxed);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (int c = 0; c < consumers; ++c) {
        threads.emplace_back([&, c] {
            auto& out = outputs[static_cast<std::size_t>(c)];
            while (consumed.load(std::memory_order_relaxed) < total) {
                if (auto value = q.try_pop()) {
                    out.push_back(*value);
                    consumed.fetch_add(1, std::memory_order_relaxed);
                } else if (produced.load(std::memory_order_relaxed) == total) {
                    if (consumed.load(std::memory_order_relaxed) == total) {
                        break;
                    }
                    std::this_thread::yield();
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::vector<int> all;
    all.reserve(total);
    for (auto& out : outputs) {
        all.insert(all.end(), out.begin(), out.end());
    }
    std::sort(all.begin(), all.end());

    AZTL_CHECK(static_cast<int>(all.size()) == total);
    AZTL_CHECK(std::adjacent_find(all.begin(), all.end()) == all.end());
    if (static_cast<int>(all.size()) == total) {
        AZTL_CHECK(all.front() == 0);
        AZTL_CHECK(all.back() == total - 1);
    }
}

void test_triple_buffer_latest_value() {
    aztl::triple_buffer<int> buffer;
    buffer.producer_buffer() = 7;
    buffer.publish();
    AZTL_CHECK(buffer.consume_latest() == 7);

    buffer.producer_buffer() = 11;
    buffer.publish();
    buffer.producer_buffer() = 13;
    buffer.publish();
    AZTL_CHECK(buffer.consume_latest() == 13);
}

} // namespace

int main() {
    if constexpr (aztl::spsc_ring_buffer<int, 8>::implemented) {
        test_basic_fifo<aztl::spsc_ring_buffer<int, 8>>();
        test_spsc_concurrent_order();
    } else {
        std::cout << "SKIP SPSC correctness tests: implementation pending\n";
    }

    if constexpr (aztl::mpmc_ring_buffer<int, 8>::implemented) {
        test_basic_fifo<aztl::mpmc_ring_buffer<int, 8>>();
        test_mpmc_exactly_once();
    } else {
        std::cout << "SKIP MPMC correctness tests: implementation pending\n";
    }

    if constexpr (aztl::triple_buffer<int>::implemented) {
        test_triple_buffer_latest_value();
    } else {
        std::cout << "SKIP triple-buffer correctness tests: implementation pending\n";
    }

    if (failures == 0) {
        std::cout << "All enabled AZTL tests passed\n";
    }
    return failures == 0 ? 0 : 1;
}
