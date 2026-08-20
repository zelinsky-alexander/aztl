#pragma once

#include <cstddef>
#include <optional>
#include <utility>

namespace aztl {

template <class T, std::size_t Capacity>
class spsc_ring_buffer {
    static_assert(Capacity >= 2, "SPSC capacity must be at least 2");

public:
    using value_type = T;
    static constexpr std::size_t capacity_value = Capacity;
    static constexpr bool implemented = false;

    constexpr spsc_ring_buffer() = default;
    spsc_ring_buffer(const spsc_ring_buffer&) = delete;
    spsc_ring_buffer& operator=(const spsc_ring_buffer&) = delete;

    [[nodiscard]] constexpr std::size_t capacity() const noexcept { return Capacity; }

    [[nodiscard]] bool try_push(const T&) {
        // TODO: producer-only enqueue path.
        return false;
    }

    [[nodiscard]] bool try_push(T&&) {
        // TODO: producer-only enqueue path.
        return false;
    }

    template <class... Args>
    [[nodiscard]] bool try_emplace(Args&&...) {
        // TODO: construct directly in unoccupied ring storage.
        return false;
    }

    [[nodiscard]] std::optional<T> try_pop() {
        // TODO: consumer-only dequeue path.
        return std::nullopt;
    }

    [[nodiscard]] bool empty() const noexcept {
        // TODO: observational helper; document its concurrency semantics.
        return true;
    }
};

} // namespace aztl
