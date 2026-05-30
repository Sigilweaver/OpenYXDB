#include "e2/E2Snappy.h"

#include <cstring>

namespace Alteryx { namespace OpenYXDB { namespace e2 {

uint64_t SnappyReadVarint(const uint8_t* data, size_t size, size_t* pos) {
    uint64_t result = 0;
    int shift = 0;
    while (true) {
        if (*pos >= size)
            throw SnappyError("snappy: truncated varint");
        uint8_t b = data[(*pos)++];
        result |= static_cast<uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0)
            return result;
        shift += 7;
        if (shift > 63)
            throw SnappyError("snappy: varint overflow");
    }
}

void SnappyDecompressRaw(const uint8_t* data, size_t size, std::vector<uint8_t>& out) {
    size_t pos = 0;
    uint64_t expected = SnappyReadVarint(data, size, &pos);
    if (expected > (size_t(1) << 32))
        throw SnappyError("snappy: declared length exceeds 4 GiB");

    out.clear();
    out.reserve(static_cast<size_t>(expected));

    while (pos < size) {
        uint8_t tag = data[pos++];
        uint8_t kind = tag & 0x03;

        if (kind == 0) {
            // Literal
            size_t lit_len;
            uint8_t hi = (tag >> 2);
            if (hi < 60) {
                lit_len = static_cast<size_t>(hi) + 1;
            } else {
                size_t n_extra = static_cast<size_t>(hi) - 59;  // 1..4
                if (pos + n_extra > size)
                    throw SnappyError("snappy: truncated literal length");
                uint32_t raw = 0;
                for (size_t i = 0; i < n_extra; ++i)
                    raw |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
                pos += n_extra;
                lit_len = static_cast<size_t>(raw) + 1;
            }
            if (pos + lit_len > size)
                throw SnappyError("snappy: truncated literal data");
            out.insert(out.end(), data + pos, data + pos + lit_len);
            pos += lit_len;
        } else if (kind == 1) {
            // copy-1: 2-byte back-reference
            //   length = ((tag >> 2) & 7) + 4    in [4, 11]
            //   offset = ((tag >> 5) << 8) | data[pos]
            if (pos >= size)
                throw SnappyError("snappy: truncated copy-1");
            size_t length = static_cast<size_t>((tag >> 2) & 0x07) + 4;
            size_t offset = (static_cast<size_t>(tag >> 5) << 8) | data[pos];
            pos += 1;
            if (offset == 0 || offset > out.size())
                throw SnappyError("snappy: copy-1 offset out of range");
            size_t start = out.size() - offset;
            for (size_t i = 0; i < length; ++i)
                out.push_back(out[start + i]);  // overlap intentional
        } else if (kind == 2) {
            // copy-2: 3-byte back-reference
            //   length = (tag >> 2) + 1   in [1, 64]
            //   offset = data[pos..pos+2] as u16 LE
            if (pos + 2 > size)
                throw SnappyError("snappy: truncated copy-2");
            size_t length = static_cast<size_t>(tag >> 2) + 1;
            size_t offset = static_cast<size_t>(data[pos]) |
                            (static_cast<size_t>(data[pos + 1]) << 8);
            pos += 2;
            if (offset == 0 || offset > out.size())
                throw SnappyError("snappy: copy-2 offset out of range");
            size_t start = out.size() - offset;
            for (size_t i = 0; i < length; ++i)
                out.push_back(out[start + i]);
        } else {
            // copy-4: 5-byte back-reference
            //   length = (tag >> 2) + 1
            //   offset = data[pos..pos+4] as u32 LE
            if (pos + 4 > size)
                throw SnappyError("snappy: truncated copy-4");
            size_t length = static_cast<size_t>(tag >> 2) + 1;
            size_t offset =
                static_cast<size_t>(data[pos]) |
                (static_cast<size_t>(data[pos + 1]) << 8) |
                (static_cast<size_t>(data[pos + 2]) << 16) |
                (static_cast<size_t>(data[pos + 3]) << 24);
            pos += 4;
            if (offset == 0 || offset > out.size())
                throw SnappyError("snappy: copy-4 offset out of range");
            size_t start = out.size() - offset;
            for (size_t i = 0; i < length; ++i)
                out.push_back(out[start + i]);
        }
    }

    if (out.size() != expected)
        throw SnappyError("snappy: decompressed length mismatch");
}

}}}  // namespace Alteryx::OpenYXDB::e2
