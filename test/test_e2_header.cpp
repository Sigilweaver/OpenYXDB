// Tests for E2 magic-byte sniff and field-type name mapping.

#include <catch2/catch_test_macros.hpp>
#include <cstring>
#include <string>

#include "e2/E2Reader.h"

using namespace Alteryx::OpenYXDB::e2;

TEST_CASE("LooksLikeE2 accepts the canonical magic", "[e2][header]")
{
    REQUIRE(LooksLikeE2(reinterpret_cast<const uint8_t*>(kE2MagicString),
                        E2_MAGIC_PREFIX_LEN));
}

TEST_CASE("LooksLikeE2 rejects E1 file header bytes", "[e2][header]")
{
    // E1 starts with "AlteryxYXDB" rather than "Alteryx e2 Database file".
    const char* e1 = "AlteryxYXDB ___________";
    REQUIRE_FALSE(LooksLikeE2(reinterpret_cast<const uint8_t*>(e1),
                              E2_MAGIC_PREFIX_LEN));
}

TEST_CASE("LooksLikeE2 rejects short buffers", "[e2][header]")
{
    const char* partial = "Alteryx e";
    REQUIRE_FALSE(LooksLikeE2(reinterpret_cast<const uint8_t*>(partial), 9));
}

TEST_CASE("E2FieldType name roundtrip", "[e2][header]")
{
    for (auto t : {E2FieldType::Bool, E2FieldType::Byte, E2FieldType::Int16,
                   E2FieldType::Int32, E2FieldType::Int64, E2FieldType::Float,
                   E2FieldType::Double, E2FieldType::String,
                   E2FieldType::WString, E2FieldType::V_String,
                   E2FieldType::V_WString, E2FieldType::Date, E2FieldType::Time,
                   E2FieldType::DateTime, E2FieldType::Blob,
                   E2FieldType::SpatialObj, E2FieldType::FixedDecimal}) {
        std::string name = E2FieldTypeName(t);
        REQUIRE(!name.empty());
        auto back = E2FieldTypeFromName(name);
        REQUIRE(back.has_value());
        REQUIRE(*back == t);
    }
}

TEST_CASE("E2FieldTypeFromName rejects garbage", "[e2][header]")
{
    REQUIRE_FALSE(E2FieldTypeFromName("NotARealType").has_value());
}
