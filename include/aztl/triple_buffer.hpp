#pragma once

#include <cstddef>
#include <utility>
#include <iostream>
#include <atomic>
#include <array>

namespace aztl {

template <class T>
class triple_buffer {
public:
    using value_type = T;
    static constexpr std::size_t buffer_count = 3;
    static constexpr bool implemented = true;

    triple_buffer() = default;
    triple_buffer(const triple_buffer&) = delete;
    triple_buffer& operator=(const triple_buffer&) = delete;

    [[nodiscard]] T& producer_buffer() noexcept {
        return _slots[_producer];
    }

    void publish() noexcept {
        const packed_state prev_state = _ready.exchange(pack(_producer, true), std::memory_order_acq_rel);
        _producer = unpack_index(prev_state);
    }

    [[nodiscard]] T consume_latest() noexcept {
        packed_state observed = _ready.load(std::memory_order_acquire);
        while (unpack_fresh(observed)) {
            const packed_state replacement = pack(_consumer, false);
            if (_ready.compare_exchange_weak(
                observed, 
                replacement, 
                std::memory_order_acq_rel,
                std::memory_order_acquire)) 
            {
                _consumer = unpack_index(observed);
                break;
            }
        }
        return _slots[_consumer];
    }

private:
    using packed_state = std::size_t;

    std::array<T, buffer_count> _slots;
    size_t _producer = { 0 };
    size_t _consumer = { 1 };
    std::atomic<packed_state> _ready = { pack(2, false) };

    static constexpr packed_state pack(std::size_t index, bool fresh) noexcept {
        return (index << 1) | static_cast<packed_state>(fresh);
    }
    static constexpr std::size_t unpack_index(packed_state state) noexcept {
        return state >> 1;
    }
    static constexpr bool unpack_fresh(packed_state state) noexcept {
        return (state & packed_state{1}) != 0;
    }    

    static_assert(std::atomic<packed_state>::is_always_lock_free);
};

} // namespace aztl

