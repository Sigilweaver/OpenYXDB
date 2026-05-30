// E2 YXDB - raw Snappy block decoder.
//
// This is a from-scratch implementation of the Google Snappy *raw block*
// decompressor as described at
// https://github.com/google/snappy/blob/main/format_description.txt
//
// "Raw block" means the input stream is:
//     [varint uncompressed_length] [ snappy commands ... ]
// with no framing format / no checksums / no magic bytes.
//
// The decoder is intentionally minimal: it only supports decompression,
// the four element types (literal, copy-1, copy-2, copy-4) and the
// varint length prefix. We do not link libsnappy because the existing
// LZF dependency is also vendored - this keeps the build self-contained.

#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace Alteryx { namespace OpenYXDB { namespace e2 {

class SnappyError : public std::runtime_error {
public:
    explicit SnappyError(const char* msg) : std::runtime_error(msg) {}
    explicit SnappyError(const std::string& msg) : std::runtime_error(msg) {}
};

// Decode the varint at `data[*pos]` and return it; advances `*pos`.
// Throws SnappyError on truncation or overflow.
uint64_t SnappyReadVarint(const uint8_t* data, size_t size, size_t* pos);

// Decompress a Snappy raw block into `out`.
//
// `out` is resized to the decompressed length declared at the start of the
// block. Throws SnappyError on any decode error.
void SnappyDecompressRaw(const uint8_t* data, size_t size, std::vector<uint8_t>& out);

}}}  // namespace Alteryx::OpenYXDB::e2
