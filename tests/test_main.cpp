#include <aztl/mpmc_ring_buffer.hpp>
#include <aztl/spsc_ring_buffer.hpp>
#include <aztl/triple_buffer.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>
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

struct test_value {
    int id;
    std::string name;

    test_value(int i, std::string n)
        : id(i), name(std::move(n)) {}
};

void test_spsc_emplace() {
    aztl::spsc_ring_buffer<test_value, 4> q;

    AZTL_CHECK(q.try_emplace(7, "alpha"));

    auto value = q.try_pop();

    AZTL_CHECK(value.has_value());
    AZTL_CHECK(value->id == 7);
    AZTL_CHECK(value->name == "alpha");
}

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

    std::cout << "Start test_spsc_concurrent_order (producer) count=" << count << " buffer size=" << bsize << std::endl;

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

    std::cout << " -> start consumer thread" << std::endl;

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

    std::cout << " -> wait for producer and consumer" << std::endl;
    producer.join();
    consumer.join();
    std::cout << " -> producer and consumer joined, q size is " << q.count() << std::endl;
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
    std::cout << "Start test_triple_buffer_latest_value" << std::endl;
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

void test_triple_buffer_retains_current_without_publish() {
    std::cout << "Start test_triple_buffer_retains_current_without_publish" << std::endl;
    aztl::triple_buffer<int> buffer;
    buffer.producer_buffer() = 42;
    buffer.publish();

    AZTL_CHECK(buffer.consume_latest() == 42);
    AZTL_CHECK(buffer.consume_latest() == 42);
    AZTL_CHECK(buffer.consume_latest() == 42);
}

void test_triple_buffer_held_reference_is_stable() {
    std::cout << "Start test_triple_buffer_held_reference_is_stable" << std::endl;
    aztl::triple_buffer<int> buffer;
    buffer.producer_buffer() = 1;
    buffer.publish();

    std::atomic<bool> reader_has_reference{false};
    std::atomic<bool> writer_finished{false};
    int value_after_writer_finished = 0;

    std::jthread reader([&] {
        const int& held = buffer.consume_latest();
        reader_has_reference.store(true, std::memory_order_release);

        while (!writer_finished.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        value_after_writer_finished = held;
    });

    std::jthread writer([&] {
        while (!reader_has_reference.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        buffer.producer_buffer() = 2;
        buffer.publish();
        buffer.producer_buffer() = 3;
        buffer.publish();
        writer_finished.store(true, std::memory_order_release);
    });

    reader.join();
    writer.join();

    AZTL_CHECK(value_after_writer_finished == 1);
    AZTL_CHECK(buffer.consume_latest() == 3);
}

void test_triple_buffer_concurrent_latest_value() {
    std::cout << "Start test_triple_buffer_concurrent_latest_value count 10,000" << std::endl;
    constexpr int count = 10'000;

    aztl::triple_buffer<int> buffer;
    std::atomic<bool> start{false};
    std::atomic<bool> writer_finished{false};
    int observed = 0;

    std::jthread writer([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        for (int value = 1; value <= count; ++value) {
            buffer.producer_buffer() = value;
            buffer.publish();
        }
        writer_finished.store(true, std::memory_order_release);
    });

    std::jthread reader([&] {
        start.store(true, std::memory_order_release);
        while (!writer_finished.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        observed = buffer.consume_latest();
    });

    writer.join();
    reader.join();

    AZTL_CHECK(observed == count);
}

struct triple_buffer_snapshot {
    std::uint64_t sequence{};
    std::array<std::uint64_t, 32> values{};
};

bool is_coherent(const triple_buffer_snapshot& snapshot) {
    return std::all_of(snapshot.values.begin(), snapshot.values.end(),
                       [&](std::uint64_t value) {
                           return value == snapshot.sequence;
                       });
}

void test_triple_buffer_concurrent_coherent_snapshots() {
    std::cout << "Start test_triple_buffer_concurrent_coherent_snapshots count 20,000" << std::endl;
    constexpr std::uint64_t count = 20'000;

    aztl::triple_buffer<triple_buffer_snapshot> buffer;
    std::atomic<bool> start{false};
    std::atomic<bool> writer_finished{false};
    bool coherent = true;
    bool monotonic = true;
    std::uint64_t final_sequence = 0;

    std::jthread writer([&] {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        for (std::uint64_t sequence = 1; sequence <= count; ++sequence) {
            auto& snapshot = buffer.producer_buffer();
            snapshot.sequence = sequence;
            snapshot.values.fill(sequence);
            buffer.publish();

            if ((sequence & 0xffU) == 0U) {
                std::this_thread::yield();
            }
        }
        writer_finished.store(true, std::memory_order_release);
    });

    std::jthread reader([&] {
        std::uint64_t previous = 0;
        start.store(true, std::memory_order_release);

        while (!writer_finished.load(std::memory_order_acquire)) {
            const triple_buffer_snapshot snapshot = buffer.consume_latest();
            coherent = coherent && is_coherent(snapshot);
            monotonic = monotonic && snapshot.sequence >= previous;
            previous = snapshot.sequence;
            std::this_thread::yield();
        }

        const triple_buffer_snapshot snapshot = buffer.consume_latest();
        coherent = coherent && is_coherent(snapshot);
        monotonic = monotonic && snapshot.sequence >= previous;
        final_sequence = snapshot.sequence;
    });

    writer.join();
    reader.join();

    AZTL_CHECK(coherent);
    AZTL_CHECK(monotonic);
    AZTL_CHECK(final_sequence == count);
}

void test_spsc_emplace_full() {
    aztl::spsc_ring_buffer<int, 2> q;

    AZTL_CHECK(q.try_emplace(1));
    AZTL_CHECK(q.try_emplace(2));
    AZTL_CHECK(!q.try_emplace(3));

    AZTL_CHECK(q.count() == 2);
}

void test_spsc_emplace_move_only() {
    aztl::spsc_ring_buffer<std::unique_ptr<int>, 2> q;

    AZTL_CHECK(q.try_emplace(std::make_unique<int>(42)));

    auto value = q.try_pop();

    AZTL_CHECK(value);
    AZTL_CHECK(**value == 42);
}

} // namespace

int main() {
    if constexpr (aztl::spsc_ring_buffer<int, 8>::implemented) {
        std::cout << "*** Start spsc_ring_buffer tests ***" << std::endl;
        test_basic_fifo<aztl::spsc_ring_buffer<int, 8>>();
        test_spsc_concurrent_order();
        test_spsc_emplace_full();
        test_spsc_emplace_move_only();
        test_spsc_emplace();
    } else {
        std::cout << "SKIP SPSC correctness tests: implementation pending\n";
    }

    if constexpr (aztl::mpmc_ring_buffer<int, 8>::implemented) {
        std::cout << "*** Start mpmc_ring_buffer tests ***" << std::endl;
        test_basic_fifo<aztl::mpmc_ring_buffer<int, 8>>();
        test_mpmc_exactly_once();
    } else {
        std::cout << "SKIP MPMC correctness tests: implementation pending\n";
    }

    if constexpr (aztl::triple_buffer<int>::implemented) {
        std::cout << "*** Start triple_buffer tests ***" << std::endl;
        test_triple_buffer_latest_value();
        test_triple_buffer_retains_current_without_publish();
        test_triple_buffer_held_reference_is_stable();
        test_triple_buffer_concurrent_latest_value();
        test_triple_buffer_concurrent_coherent_snapshots();
    } else {
        std::cout << "SKIP triple-buffer correctness tests: implementation pending\n";
    }

    if (failures == 0) {
        std::cout << "All enabled AZTL tests passed\n";
    }
    return failures == 0 ? 0 : 1;
}
