// Unit tests for the vendored Snappy raw decompressor.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <string>
#include <vector>

#include "e2/E2Snappy.h"

using Alteryx::OpenYXDB::e2::SnappyDecompressRaw;
using Alteryx::OpenYXDB::e2::SnappyError;

namespace {

std::vector<uint8_t> dec(const std::vector<uint8_t>& in)
{
    std::vector<uint8_t> out;
    SnappyDecompressRaw(in.data(), in.size(), out);
    return out;
}

}  // namespace

TEST_CASE("Snappy decodes a single short literal", "[e2][snappy]")
{
    // length=3 -> varint 0x03; literal tag for 3 bytes (n-1=2) -> 0x08; "ABC".
    std::vector<uint8_t> input{0x03, 0x08, 'A', 'B', 'C'};
    auto out = dec(input);
    REQUIRE(out.size() == 3);
    REQUIRE(out[0] == 'A');
    REQUIRE(out[1] == 'B');
    REQUIRE(out[2] == 'C');
}

TEST_CASE("Snappy decodes a copy-1 back-reference", "[e2][snappy]")
{
    // length=6 -> 0x06; literal "AB" (n-1=1 -> tag 0x04); copy-1 length=4 offset=2.
    // copy-1: tag bits 00..00 01; len_bits=(4-4)=0 -> 000; high offset bits=0
    // -> byte = (000 << 5) | (000 << 2) | 01 = 0x01; offset_low = 2.
    std::vector<uint8_t> input{0x06, 0x04, 'A', 'B', 0x01, 0x02};
    auto out = dec(input);
    REQUIRE(out.size() == 6);
    REQUIRE(std::string(out.begin(), out.end()) == "ABABAB");
}

TEST_CASE("Snappy decodes an overlapping copy", "[e2][snappy]")
{
    // length=5 -> 0x05; literal 'A' (n-1=0 -> tag 0x00); copy-1 length=4 offset=1.
    // copy-1 byte: len_bits=0 (=>4-4), offset_high=0 -> 0x01; offset_low=1.
    std::vector<uint8_t> input{0x05, 0x00, 'A', 0x01, 0x01};
    auto out = dec(input);
    REQUIRE(out.size() == 5);
    REQUIRE(std::string(out.begin(), out.end()) == "AAAAA");
}

TEST_CASE("Snappy decodes a copy-2 back-reference", "[e2][snappy]")
{
    // length=8 -> 0x08; literal "ABCD" (n-1=3 -> tag 0x0C); copy-2 length=4
    // offset=4. copy-2 tag = ((len-1)<<2)|02 = (3<<2)|2 = 0x0E. offset stored
    // little-endian as 04 00.
    std::vector<uint8_t> input{0x08, 0x0C, 'A', 'B', 'C', 'D', 0x0E, 0x04, 0x00};
    auto out = dec(input);
    REQUIRE(std::string(out.begin(), out.end()) == "ABCDABCD");
}

TEST_CASE("Snappy decodes a 2-byte length-prefix literal", "[e2][snappy]")
{
    // 70-byte literal (tag 60 means 1-byte length follows, value=n-1=69).
    std::vector<uint8_t> input;
    input.push_back(70);  // varint output length
    input.push_back(60 << 2);  // tag 0xF0
    input.push_back(69);
    for (int i = 0; i < 70; ++i) input.push_back('Z');
    auto out = dec(input);
    REQUIRE(out.size() == 70);
    for (auto b : out) REQUIRE(b == 'Z');
}

TEST_CASE("Snappy multi-byte varint length", "[e2][snappy]")
{
    // 200 bytes of 'X': varint = 0xC8 0x01.
    std::vector<uint8_t> input;
    input.push_back(0xC8);
    input.push_back(0x01);
    input.push_back(60 << 2);
    input.push_back(199);
    for (int i = 0; i < 200; ++i) input.push_back('X');
    auto out = dec(input);
    REQUIRE(out.size() == 200);
}

TEST_CASE("Snappy throws on truncated input", "[e2][snappy]")
{
    std::vector<uint8_t> input{0x10, 0x08, 'A'};  // promises 16 bytes, literal short
    REQUIRE_THROWS_AS(dec(input), SnappyError);
}
