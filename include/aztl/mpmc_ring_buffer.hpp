#pragma once

#include <cstddef>
#include <optional>
#include <utility>

namespace aztl {

template <class T, std::size_t Capacity>
class mpmc_ring_buffer {
    static_assert(Capacity >= 2, "MPMC capacity must be at least 2");

public:
    using value_type = T;
    static constexpr std::size_t capacity_value = Capacity;
    static constexpr bool implemented = false;

    constexpr mpmc_ring_buffer() = default;
    mpmc_ring_buffer(const mpmc_ring_buffer&) = delete;
    mpmc_ring_buffer& operator=(const mpmc_ring_buffer&) = delete;

    [[nodiscard]] constexpr std::size_t capacity() const noexcept { return Capacity; }

    [[nodiscard]] bool try_push(const T&) {
        // TODO: bounded multi-producer enqueue path.
        return false;
    }

    [[nodiscard]] bool try_push(T&&) {
        // TODO: bounded multi-producer enqueue path.
        return false;
    }

    template <class... Args>
    [[nodiscard]] bool try_emplace(Args&&...) {
        // TODO: claim a slot, construct T, then publish it safely.
        return false;
    }

    [[nodiscard]] std::optional<T> try_pop() {
        // TODO: bounded multi-consumer dequeue path.
        return std::nullopt;
    }
};

} // namespace aztl
