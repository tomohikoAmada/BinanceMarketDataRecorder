#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>

namespace {

using Bytes = std::span<const unsigned char>;

struct CanonicalCursor final {
    Bytes input;
    std::size_t pos{0};

    [[nodiscard]] bool read_arg(unsigned info, std::uint64_t& value) noexcept {
        if (info < 24U) {
            value = info;
            return true;
        }
        if (info >= 28U) {
            return false;
        }
        const std::size_t width = std::size_t{1} << (info - 24U);
        if (width > input.size() - pos) {
            return false;
        }
        std::uint64_t acc = 0;
        for (std::size_t i = 0; i < width; ++i) {
            acc = (acc << 8U) | input[pos++];
        }
        constexpr std::uint64_t kMinimums[4] = {24ULL, 256ULL, 65536ULL, 4294967296ULL};
        value = acc;
        return acc >= kMinimums[info - 24U];
    }

    [[nodiscard]] bool check_item(unsigned depth) noexcept {
        if (depth > 16U || pos >= input.size()) {
            return false;
        }
        const unsigned tag = input[pos++];
        const unsigned major = tag >> 5U;
        const unsigned info = tag & 31U;

        if (major == 7U) {
            // Only canonical bool and null simple values take the zero-allocation fast path.
            // Floats or unusual simple values delegate cleanly to the Python cbor2 strict decoder.
            return tag == 0xf4U || tag == 0xf5U || tag == 0xf6U;
        }
        std::uint64_t arg = 0;
        if (!read_arg(info, arg)) {
            return false;
        }
        if (major < 2U) {
            return true;
        }
        if (arg > input.size() - pos) {
            return false;
        }
        if (major == 2U || major == 3U) {
            pos += static_cast<std::size_t>(arg);
            return true;
        }
        if (major != 4U && major != 5U) {
            return false;
        }
        Bytes previous_key{};
        for (std::uint64_t i = 0; i < arg; ++i) {
            if (major == 5U) {
                const std::size_t key_start = pos;
                if (pos >= input.size() || (input[pos] >> 5U) != 3U || !check_item(depth + 1U)) {
                    return false;
                }
                const Bytes key = input.subspan(key_start, pos - key_start);
                if (i > 0U) {
                    if (key.size() < previous_key.size()) {
                        return false;
                    }
                    if (key.size() == previous_key.size() &&
                        !std::lexicographical_compare(previous_key.begin(), previous_key.end(),
                                                      key.begin(), key.end())) {
                        return false;
                    }
                }
                previous_key = key;
            }
            if (!check_item(depth + 1U)) {
                return false;
            }
        }
        return true;
    }
};

} // namespace

extern "C" {

// Returns 1 if the CBOR byte slice is proven canonical inside the fast subset,
// or 0 to delegate strict canonical re-encoding verification to Python cbor2.
int bmcp_canonical_cbor_fast(const unsigned char* bytes, std::size_t size) noexcept {
    if (bytes == nullptr) {
        return 0;
    }
    CanonicalCursor cursor{Bytes{bytes, size}};
    if (!cursor.check_item(0U) || cursor.pos != size) {
        return 0;
    }
    return 1;
}

} // extern "C"
