#pragma once

#include <cstddef>
#include <utility>

namespace aztl {

template <class T>
class triple_buffer {
public:
using value_type = T;
static constexpr std::size_t buffer_count = 3;
static constexpr bool implemented = false;

triple_buffer() = default;
triple_buffer(const triple_buffer&) = delete;
triple_buffer& operator=(const triple_buffer&) = delete;

[[nodiscard]] T& producer_buffer() noexcept {
// TODO: return the buffer exclusively owned by the writer.
return placeholder_;
}

void publish() noexcept {
// TODO: atomically publish the completed producer buffer.
}

[[nodiscard]] const T& consume_latest() noexcept {
size_t ready = _ready.load(std::mem_order_aquire););
if (ready == nullopt) throw::exception_not_ready();
return _bufs[ready];
}

private:
    std::array<T, buffer_count> _bufs;
    std::atomic<size_t> _pr {0};
    std::atomic<size_t> _cns {nullopt};
    std;;atomic<size_t> _ready {nullopt};
};

} // namespace aztl

