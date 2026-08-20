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
        // TODO: acquire the newest published buffer without colliding with writer ownership.
        return placeholder_;
    }

private:
    // Temporary scaffold storage only. Replace this with the actual three-buffer state.
    T placeholder_{};
};

} // namespace aztl
