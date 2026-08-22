#pragma once

#include <cstddef>
#include <optional>
#include <utility>
#include <atomic>
#include <array>
#include <algorithm>
#include <memory>
#include <concepts>

namespace aztl {

template <typename T> 
struct slot {
    alignas(T) std::byte storage[sizeof(T)];
};

template <class T, std::size_t Capacity>
class spsc_ring_buffer {
    static_assert(Capacity >= 2, "SPSC capacity must be at least 2");

public:
    using value_type = T;
    static constexpr std::size_t capacity_value = Capacity;
    static constexpr bool implemented = true;

    constexpr spsc_ring_buffer() = default;
    spsc_ring_buffer(const spsc_ring_buffer&) = delete;
    spsc_ring_buffer& operator=(const spsc_ring_buffer&) = delete;
    spsc_ring_buffer(spsc_ring_buffer&&) = delete;
    spsc_ring_buffer& operator=(spsc_ring_buffer&&) = delete;

    ~spsc_ring_buffer() {
        auto head = _head.load(std::memory_order_relaxed);
        auto tail = _tail.load(std::memory_order_relaxed);
        for (auto i = tail; i != head; ++i) {
            std::destroy_at(slot2ptr(i % Capacity));
        }
    }

    [[nodiscard]] constexpr std::size_t capacity() const noexcept { return Capacity; }

    [[nodiscard]] bool try_push(const T& t) {
        return try_emplace(t);
    }

    [[nodiscard]] bool try_push(T&& t) {
        return try_emplace(std::move(t));
    }

    template <class... Args>
    requires std::constructible_from<T, Args...>
    [[nodiscard]] bool try_emplace(Args&&... args) {
        auto head = _head.load(std::memory_order_relaxed);
        auto tail = _tail.load(std::memory_order_acquire);
        if (head - tail == Capacity) {
            // full
            return false;
        }
        const auto index = head % Capacity;
        T* ptr = slot2ptr(index);
        std::construct_at(ptr, std::forward<Args>(args)...);
        _head.store(head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] std::optional<T> try_pop() {
        auto head = _head.load(std::memory_order_acquire);
        auto tail = _tail.load(std::memory_order_relaxed);
        if (head == tail) {
            // empty
            return std::nullopt;
        }
        auto index = tail % Capacity;
        T* ptr = slot2ptr(index);
        T value = std::move(*ptr);
        std::destroy_at(ptr);
        _tail.store(tail + 1, std::memory_order_release);
        return value;        
    }

    [[nodiscard]] bool empty() const noexcept {
        auto head = _head.load(std::memory_order_relaxed);
        auto tail = _tail.load(std::memory_order_relaxed);
        return (head == tail);
    }

    [[nodiscard]] size_t count() const noexcept {
        auto head = _head.load(std::memory_order_relaxed);
        auto tail = _tail.load(std::memory_order_relaxed);
        return head - tail;
    }


private:
    std::array<slot<T>, Capacity> _buffer;
    std::atomic<size_t> _head = {0};
    std::atomic<size_t> _tail = {0};

    T* slot2ptr(std::size_t i) {return std::launder(reinterpret_cast<T*>(_buffer[i].storage));}
};

} // namespace aztl
