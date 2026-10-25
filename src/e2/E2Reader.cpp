// E2 YXDB reader implementation.
//
// Layout of an E2 file:
//   [100-byte header]
//   [UTF-8 XML metadata, length given in header bytes 96..100]
//   [data section: one or more blocks]
//   [optional null sentinel] [footer ending in "YXE2"]
//
// Block types in the data section:
//   0x00  end-of-stream sentinel
//   0x01  blob block: stores one large variable-length value
//   0x02  record block: Snappy-compressed records
//   0x03  per-block spatial index (skipped)
//   0x04  global spatial index    (skipped)
//
// Field encoding is compact and variable-length; see E2DecodeField below.

#include "e2/E2Reader.h"
#include "e2/E2Snappy.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>

#if defined(_WIN32)
    #include <io.h>
    #define READ ::_read
    #define CLOSE ::_close
    #define LSEEK ::_lseeki64
#else
    #include <unistd.h>
    #define READ ::read
    #define CLOSE ::close
    #define LSEEK ::lseek
    #ifndef O_BINARY
        #define O_BINARY 0
    #endif
#endif

namespace Alteryx { namespace OpenYXDB { namespace e2 {

const char kE2MagicString[] = "Alteryx e2 Database file";

bool LooksLikeE2(const uint8_t* buf, size_t len) {
    if (len < E2_MAGIC_PREFIX_LEN) return false;
    return std::memcmp(buf, kE2MagicString, E2_MAGIC_PREFIX_LEN) == 0;
}

const char* E2FieldTypeName(E2FieldType t) {
    switch (t) {
        case E2FieldType::Bool:         return "Bool";
        case E2FieldType::Byte:         return "Byte";
        case E2FieldType::Int16:        return "Int16";
        case E2FieldType::Int32:        return "Int32";
        case E2FieldType::Int64:        return "Int64";
        case E2FieldType::FixedDecimal: return "FixedDecimal";
        case E2FieldType::Float:        return "Float";
        case E2FieldType::Double:       return "Double";
        case E2FieldType::String:       return "String";
        case E2FieldType::WString:      return "WString";
        case E2FieldType::V_String:     return "V_String";
        case E2FieldType::V_WString:    return "V_WString";
        case E2FieldType::Date:         return "Date";
        case E2FieldType::Time:         return "Time";
        case E2FieldType::DateTime:     return "DateTime";
        case E2FieldType::Blob:         return "Blob";
        case E2FieldType::SpatialObj:   return "SpatialObj";
    }
    return "Unknown";
}

std::optional<E2FieldType> E2FieldTypeFromName(const std::string& n) {
    if (n == "Bool")         return E2FieldType::Bool;
    if (n == "Byte")         return E2FieldType::Byte;
    if (n == "Int16")        return E2FieldType::Int16;
    if (n == "Int32")        return E2FieldType::Int32;
    if (n == "Int64")        return E2FieldType::Int64;
    if (n == "FixedDecimal") return E2FieldType::FixedDecimal;
    if (n == "Float")        return E2FieldType::Float;
    if (n == "Double")       return E2FieldType::Double;
    if (n == "String")       return E2FieldType::String;
    if (n == "WString")      return E2FieldType::WString;
    if (n == "V_String")     return E2FieldType::V_String;
    if (n == "V_WString")    return E2FieldType::V_WString;
    if (n == "Date")         return E2FieldType::Date;
    if (n == "Time")         return E2FieldType::Time;
    if (n == "DateTime")     return E2FieldType::DateTime;
    if (n == "Blob")         return E2FieldType::Blob;
    if (n == "SpatialObj")   return E2FieldType::SpatialObj;
    return std::nullopt;
}

// ===========================================================================
//                            Helpers (XML + dates)
// ===========================================================================

namespace {

[[noreturn]] void Fail(const std::string& msg) { throw E2FormatError(msg); }

uint32_t LoadU32LE(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) |
           (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}
uint16_t LoadU16LE(const uint8_t* p) {
    return static_cast<uint16_t>(p[0]) | (static_cast<uint16_t>(p[1]) << 8);
}
uint64_t LoadU64LE(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
    return v;
}

// Minimal XML attribute extractor: scans for name="..." inside a tag.
std::string GetAttr(const std::string& tag, const std::string& attr) {
    std::string key = attr + "=\"";
    size_t p = 0;
    for (;;) {
        p = tag.find(key, p);
        if (p == std::string::npos) return {};
        // Match whole attribute names only: "name" must not match "rename".
        if (p == 0 || tag[p - 1] == ' ' || tag[p - 1] == '\t' || tag[p - 1] == '\n' ||
            tag[p - 1] == '\r')
            break;
        p += key.size();
    }
    p += key.size();
    size_t end = tag.find('"', p);
    if (end == std::string::npos) return {};
    return tag.substr(p, end - p);
}

void DecodeXmlEntities(std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ) {
        if (s[i] == '&') {
            if (s.compare(i, 5, "&amp;") == 0) { out.push_back('&'); i += 5; continue; }
            if (s.compare(i, 4, "&lt;") == 0)  { out.push_back('<'); i += 4; continue; }
            if (s.compare(i, 4, "&gt;") == 0)  { out.push_back('>'); i += 4; continue; }
            if (s.compare(i, 6, "&quot;") == 0){ out.push_back('"'); i += 6; continue; }
            if (s.compare(i, 6, "&apos;") == 0){ out.push_back('\''); i += 6; continue; }
        }
        out.push_back(s[i++]);
    }
    s.swap(out);
}

// Parse the <Field .../> elements inside <RecordInfo>.
void ParseMetaXml(const std::string& xml, std::vector<E2FieldMeta>& fields) {
    size_t pos = 0;
    while (pos < xml.size()) {
        size_t lt = xml.find('<', pos);
        if (lt == std::string::npos) break;
        size_t gt = xml.find('>', lt);
        if (gt == std::string::npos) break;
        std::string tag = xml.substr(lt, gt - lt + 1);
        pos = gt + 1;
        if (tag.size() < 6 || tag.compare(1, 5, "Field") != 0) continue;
        // skip closing tags, only consume <Field ... /> or <Field ...>
        if (tag[1] == '/') continue;
        // Must start with "<Field " (space) to avoid matching e.g. <Fields>
        if (tag[6] != ' ' && tag[6] != '\t' && tag[6] != '\n') continue;

        E2FieldMeta m;
        m.name = GetAttr(tag, "name");
        DecodeXmlEntities(m.name);
        std::string type_str = GetAttr(tag, "type");
        std::string size_str = GetAttr(tag, "size");
        std::string scale_str = GetAttr(tag, "scale");
        if (m.name.empty() || type_str.empty())
            Fail("E2 metadata: <Field> element " + std::to_string(fields.size() + 1) +
                 " has no name or no type");
        auto ft = E2FieldTypeFromName(type_str);
        if (!ft) Fail("E2 metadata: unknown field type '" + type_str + "'");
        m.type = *ft;
        if (!size_str.empty())  m.size  = std::atoi(size_str.c_str());
        if (!scale_str.empty()) m.scale = std::atoi(scale_str.c_str());
        fields.push_back(std::move(m));
    }
    if (fields.empty())
        Fail("E2 metadata: no <Field> elements found");
}

// Days from 1899-12-30 (OLE epoch) to 1970-01-01.
constexpr int64_t kOleEpochOffset = 25569;

// Hinnant civil-from-days, with day 0 = 1970-01-01.
void DaysToCivil(int64_t days, int& y, int& m, int& d) {
    int64_t z = days + 719468;
    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    uint32_t doe = static_cast<uint32_t>(z - era * 146097);
    uint32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int64_t y0 = static_cast<int64_t>(yoe) + era * 400;
    uint32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    uint32_t mp = (5*doy + 2)/153;
    d = static_cast<int>(doy - (153*mp + 2)/5 + 1);
    m = static_cast<int>(mp < 10 ? mp + 3 : mp - 9);
    y = static_cast<int>(m <= 2 ? y0 + 1 : y0);
}

std::string FormatDate(int64_t day_serial) {
    int64_t unix_days = day_serial - kOleEpochOffset;
    int y, mo, d;
    DaysToCivil(unix_days, y, mo, d);
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, mo, d);
    return std::string(buf);
}

// Time of day is stored in centiseconds. The output has whole-second
// resolution, so a fraction or a value of 24 hours or more is an error instead
// of being cut off or printed as an hour that does not exist. Decoders only
// record the value (E2Cell::integer); the check runs once a row has been
// accepted, because decoding is also used to probe candidate layouts and a
// failure there means "not this layout", not "bad file".
void CheckTimeOfDay(uint64_t centi, const char* what) {
    if (centi >= 24ull * 3600ull * 100ull)
        Fail(std::string("E2 ") + what + ": time of day " + std::to_string(centi) +
             " centiseconds is 24 hours or more");
    if (centi % 100 != 0)
        Fail(std::string("E2 ") + what + ": time of day " + std::to_string(centi) +
             " centiseconds has a sub-second part, which is not supported");
}

std::string FormatDateTimePacked(uint64_t raw) {
    uint64_t centi = raw & 0xFFFFFFull;
    int64_t day_serial = static_cast<int64_t>((raw >> 24) & 0xFFFFFFull);
    int64_t unix_days = day_serial - kOleEpochOffset;
    int y, mo, d;
    DaysToCivil(unix_days, y, mo, d);
    uint64_t total_seconds = centi / 100;
    int h = static_cast<int>(total_seconds / 3600);
    int mi = static_cast<int>((total_seconds % 3600) / 60);
    int se = static_cast<int>(total_seconds % 60);
    char buf[40];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                  y, mo, d, h, mi, se);
    return std::string(buf);
}

std::string FormatTimeCenti(uint64_t centi) {
    uint64_t total_seconds = centi / 100;
    int h = static_cast<int>(total_seconds / 3600);
    int mi = static_cast<int>((total_seconds % 3600) / 60);
    int se = static_cast<int>(total_seconds % 60);
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d", h, mi, se);
    return std::string(buf);
}

// ===========================================================================
//                       Compact field decoders
// ===========================================================================
//
// All decoders take the decompressed record bytes plus a current offset
// and return the consumed byte count via *consumed. Decoders never read
// past `len`. On error they throw.

constexpr uint8_t kNullBool   = 0x43;
constexpr uint8_t kNullByte   = 0x47;
constexpr uint8_t kNullInt16  = 0x45;
constexpr uint8_t kNullInt32  = 0x49;
constexpr uint8_t kNullInt64  = 0x4A;
constexpr uint8_t kNullFloat  = 0x4B;
constexpr uint8_t kNullDouble = 0x48;
constexpr uint8_t kNullDoubleAlt = 0x4C;
constexpr uint8_t kNullDate   = 0x4D;
constexpr uint8_t kNullDateTime = 0x4E;
constexpr uint8_t kNullTime   = 0x4F;
constexpr uint8_t kNullString = 0x41;
constexpr uint8_t kNullFixedDecimal = 0x4C;
constexpr uint8_t kNullSpatial = 0x43;

void NeedBytes(size_t need, size_t have, const char* what) {
    if (have < need) Fail(std::string("E2 decoder: truncated ") + what);
}

E2Cell DecodeBool(const uint8_t* p, size_t len, size_t* consumed) {
    NeedBytes(1, len, "Bool");
    *consumed = 1;
    switch (p[0]) {
        case 0x14: return E2Cell::MakeBool(false);
        case 0x15: return E2Cell::MakeBool(true);
        case kNullBool: return E2Cell::MakeNull();
    }
    Fail("E2 Bool: invalid prefix");
}

// Compact int with configurable base (6 for byte/int16/int32/int64).
E2Cell DecodeCompactInt(const uint8_t* p, size_t len, size_t* consumed,
                        uint8_t base, uint8_t null_byte, int max_bytes,
                        bool is_unsigned = false) {
    NeedBytes(1, len, "Int");
    uint8_t prefix = p[0];
    if (prefix == null_byte) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix < base) Fail("E2 Int: invalid prefix");
    int n = prefix - base;
    if (n == 0) { *consumed = 1; return E2Cell::MakeInt(0); }
    if (n > max_bytes) Fail("E2 Int: prefix exceeds type width");
    NeedBytes(1 + n, len, "Int payload");
    uint64_t raw = 0;
    for (int i = 0; i < n; ++i)
        raw |= static_cast<uint64_t>(p[1 + i]) << (8 * i);
    // Sign-extend from `n` bytes into int64. For sub-8-byte sources we
    // sign-extend so e.g. Int32 -1 (`0x0A FF FF FF FF`) becomes -1.
    int64_t value;
    if (n == max_bytes && max_bytes < 8 && !is_unsigned) {
        // sign-extend
        uint64_t sign_bit = 1ull << (n * 8 - 1);
        if (raw & sign_bit) raw |= ~((1ull << (n * 8)) - 1);
        value = static_cast<int64_t>(raw);
    } else if (n == 8) {
        value = static_cast<int64_t>(raw);
    } else {
        // partial-width positive value, never sign-extend
        value = static_cast<int64_t>(raw);
    }
    *consumed = 1 + n;
    return E2Cell::MakeInt(value);
}

E2Cell DecodeFloat(const uint8_t* p, size_t len, size_t* consumed) {
    NeedBytes(1, len, "Float");
    uint8_t prefix = p[0];
    if (prefix == kNullFloat) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix < 0x07) Fail("E2 Float: invalid prefix");
    if (prefix == 0x07)       { *consumed = 1; return E2Cell::MakeDouble(0.0); }
    int n = prefix - 0x07;
    if (n < 1 || n > 4) Fail("E2 Float: invalid prefix");
    NeedBytes(1 + n, len, "Float payload");
    uint8_t buf[4] = {0,0,0,0};
    std::memcpy(buf, p + 1, n);
    float f;
    std::memcpy(&f, buf, 4);
    *consumed = 1 + n;
    return E2Cell::MakeDouble(static_cast<double>(f));
}

E2Cell DecodeDouble(const uint8_t* p, size_t len, size_t* consumed) {
    NeedBytes(1, len, "Double");
    uint8_t prefix = p[0];
    if (prefix == kNullDouble || prefix == kNullDoubleAlt) {
        *consumed = 1; return E2Cell::MakeNull();
    }
    if (prefix < 0x06) Fail("E2 Double: invalid prefix");
    if (prefix == 0x06) { *consumed = 1; return E2Cell::MakeDouble(0.0); }
    int n = prefix - 0x04;
    if (n < 3 || n > 8) Fail("E2 Double: invalid prefix");
    NeedBytes(1 + n, len, "Double payload");
    uint8_t buf[8] = {0,0,0,0,0,0,0,0};
    std::memcpy(buf, p + 1, n);
    double d;
    std::memcpy(&d, buf, 8);
    *consumed = 1 + n;
    return E2Cell::MakeDouble(d);
}

// String / V_String / V_WString / String share a single prefix scheme.
//
// Returns one of:
//   - Null cell (consumed = 1)
//   - Text cell (inline)
//   - Internal Reference cell carrying an eight-byte blob reference;
//     the caller resolves it against blob_blocks.
//
// `null_byte` is the type-specific null marker (0x41 for strings).
E2Cell DecodeStringLike(const uint8_t* p, size_t len, size_t* consumed,
                        uint8_t null_byte, uint8_t long_inline_prefix,
                        uint8_t blob_ref_prefix) {
    NeedBytes(1, len, "String prefix");
    uint8_t prefix = p[0];
    if (prefix == 0x00 || prefix == null_byte) {
        *consumed = 1; return E2Cell::MakeNull();
    }
    if (prefix & 0x80) {
        size_t n = prefix & 0x7F;
        NeedBytes(1 + n, len, "short string");
        *consumed = 1 + n;
        return E2Cell::MakeText(std::string(reinterpret_cast<const char*>(p + 1), n));
    }
    if (prefix == long_inline_prefix) {
        NeedBytes(3, len, "long string length");
        size_t n = LoadU16LE(p + 1);
        NeedBytes(3 + n, len, "long string payload");
        *consumed = 3 + n;
        return E2Cell::MakeText(std::string(reinterpret_cast<const char*>(p + 3), n));
    }
    if (prefix == blob_ref_prefix) {
        // Preserve the type class separately from the eight-byte payload so
        // arbitrary inline bytes can never collide with a reference marker.
        // type_class 0x01 -> string ref (u32 offset + u32 length inline)
        // type_class 0x02/0x03 -> file-offset reference (u64 LE)
        NeedBytes(9, len, "blob ref");
        std::vector<uint8_t> payload(p + 1, p + 9);
        *consumed = 9;
        return E2Cell::MakeReference(blob_ref_prefix & 0x0F, std::move(payload));
    }
    Fail("E2 String-like: invalid prefix");
}

E2Cell DecodeDate(const uint8_t* p, size_t len, size_t* consumed) {
    NeedBytes(1, len, "Date");
    uint8_t prefix = p[0];
    if (prefix == kNullDate) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix < 0x0A) Fail("E2 Date: invalid prefix");
    if (prefix == 0x0A) { *consumed = 1; return E2Cell::MakeText(FormatDate(0)); }
    int n = prefix - 0x0A;
    if (n < 1 || n > 4) Fail("E2 Date: invalid prefix");
    NeedBytes(1 + n, len, "Date payload");
    uint8_t buf[4] = {0,0,0,0};
    std::memcpy(buf, p + 1, n);
    uint32_t serial = LoadU32LE(buf);
    *consumed = 1 + n;
    return E2Cell::MakeText(FormatDate(static_cast<int64_t>(serial)));
}

E2Cell DecodeDateTime(const uint8_t* p, size_t len, size_t* consumed) {
    NeedBytes(1, len, "DateTime");
    uint8_t prefix = p[0];
    if (prefix == kNullDateTime) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix < 0x08) Fail("E2 DateTime: invalid prefix");
    if (prefix == 0x08) {
        *consumed = 1; { E2Cell c = E2Cell::MakeText(FormatDateTimePacked(0)); c.integer = 0; return c; }
    }
    int n = prefix - 0x08;
    if (n < 1 || n > 6) Fail("E2 DateTime: invalid prefix");
    NeedBytes(1 + n, len, "DateTime payload");
    uint8_t buf[8] = {0,0,0,0,0,0,0,0};
    std::memcpy(buf, p + 1, n);
    uint64_t raw = LoadU64LE(buf);
    *consumed = 1 + n;
    { E2Cell c = E2Cell::MakeText(FormatDateTimePacked(raw)); c.integer = static_cast<int64_t>(raw & 0xFFFFFFull); return c; }
}

E2Cell DecodeTime(const uint8_t* p, size_t len, size_t* consumed) {
    NeedBytes(1, len, "Time");
    uint8_t prefix = p[0];
    if (prefix == kNullTime) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix < 0x0C) Fail("E2 Time: invalid prefix");
    if (prefix == 0x0C) { *consumed = 1; return E2Cell::MakeText("00:00:00"); }
    int n = prefix - 0x0C;
    if (n < 1 || n > 4) Fail("E2 Time: invalid prefix");
    NeedBytes(1 + n, len, "Time payload");
    uint8_t buf[4] = {0,0,0,0};
    std::memcpy(buf, p + 1, n);
    uint64_t centi = LoadU32LE(buf);
    *consumed = 1 + n;
    { E2Cell c = E2Cell::MakeText(FormatTimeCenti(centi)); c.integer = static_cast<int64_t>(centi); return c; }
}

// FixedDecimal - packed BCD with explicit scale and sign per value.
//   [0x04] [prefix] [sign|scale] [BCD bytes...]
// data_bytes = prefix/2 + 1; sig_digits = prefix + 1.
E2Cell DecodeFixedDecimal(const uint8_t* p, size_t len, size_t* consumed) {
    NeedBytes(1, len, "FixedDecimal");
    uint8_t marker = p[0];
    if (marker == 0x00 || marker == kNullFixedDecimal) {
        *consumed = 1; return E2Cell::MakeNull();
    }
    if (marker != 0x04) Fail("E2 FixedDecimal: unexpected marker");
    NeedBytes(3, len, "FixedDecimal header");
    uint8_t prefix = p[1];
    uint8_t sign_scale = p[2];
    int data_bytes = prefix / 2 + 1;
    int sig_digits = static_cast<int>(prefix) + 1;
    NeedBytes(static_cast<size_t>(3 + data_bytes), len, "FixedDecimal payload");
    // Decode the first `sig_digits` BCD nibbles (high nibble first).
    std::string digits;
    digits.reserve(static_cast<size_t>(sig_digits));
    for (int i = 0; i < sig_digits; ++i) {
        uint8_t byte = p[3 + (i / 2)];
        uint8_t nib = (i % 2 == 0) ? (byte >> 4) : (byte & 0x0F);
        if (nib > 9) Fail("E2 FixedDecimal: invalid BCD digit");
        digits.push_back(static_cast<char>('0' + nib));
    }
    bool negative = (sign_scale & 0x80) != 0;
    int scale = sign_scale & 0x7F;

    // Strip leading zeros (keep at least one digit).
    size_t first_nz = digits.find_first_not_of('0');
    if (first_nz == std::string::npos) {
        digits = "0";
    } else if (first_nz > 0) {
        digits.erase(0, first_nz);
    }

    std::string out;
    if (negative && digits != "0") out.push_back('-');
    if (scale <= 0) {
        out += digits;
    } else {
        // Insert decimal point so that `scale` digits are to the right.
        if (static_cast<int>(digits.size()) <= scale) {
            out += "0.";
            out.append(static_cast<size_t>(scale) - digits.size(), '0');
            out += digits;
        } else {
            size_t cut = digits.size() - static_cast<size_t>(scale);
            out.append(digits, 0, cut);
            out.push_back('.');
            out.append(digits, cut, std::string::npos);
        }
    }
    *consumed = 3 + static_cast<size_t>(data_bytes);
    return E2Cell::MakeText(out);
}

// Blob / SpatialObj inline-or-ref decoding.
E2Cell DecodeBlobOrSpatial(const uint8_t* p, size_t len, size_t* consumed,
                           uint8_t long_inline_prefix,
                           uint8_t blob_ref_prefix,
                           uint8_t null_byte) {
    NeedBytes(1, len, "Blob/Spatial");
    uint8_t prefix = p[0];
    if (prefix == 0x00 || prefix == null_byte) {
        *consumed = 1; return E2Cell::MakeNull();
    }
    if (prefix & 0x80) {
        size_t n = prefix & 0x7F;
        NeedBytes(1 + n, len, "short blob");
        *consumed = 1 + n;
        return E2Cell::MakeBytes(std::vector<uint8_t>(p + 1, p + 1 + n));
    }
    if (prefix == long_inline_prefix || prefix == 0x01) {
        NeedBytes(3, len, "long blob length");
        size_t n = LoadU16LE(p + 1);
        NeedBytes(3 + n, len, "long blob payload");
        *consumed = 3 + n;
        return E2Cell::MakeBytes(std::vector<uint8_t>(p + 3, p + 3 + n));
    }
    if (prefix == blob_ref_prefix || prefix == 0x11) {
        // 0x11 = string-style (u32 offset + u32 length).
        // 0x12 = blob file offset (u64).
        // 0x13 = spatial file offset (u64).
        NeedBytes(9, len, "blob ref");
        std::vector<uint8_t> payload(p + 1, p + 9);
        *consumed = 9;
        return E2Cell::MakeReference(prefix & 0x0F, std::move(payload));
    }
    Fail("E2 Blob/Spatial: invalid prefix");
}

// ===========================================================================
//                                 Impl
// ===========================================================================

#if defined(_WIN32)
// _open() takes a narrow, ANSI-codepage path and is limited to the legacy
// MAX_PATH (260 char) path length, so it can't reliably reach UNC shares
// (\\server\share\...) or \\?\ long-path-escaped paths. _wopen() with a
// UTF-16 path goes through CreateFileW instead, avoiding both limitations;
// this mirrors the E1 reader's approach in Open_AlteryxYXDB.cpp.
std::wstring Utf8PathToWide(const std::string& utf8) {
    std::wstring result;
    size_t i = 0;
    while (i < utf8.size()) {
        uint32_t cp;
        unsigned char c = static_cast<unsigned char>(utf8[i]);
        if (c < 0x80) {
            cp = c;
            i += 1;
        } else if ((c & 0xE0) == 0xC0 && i + 1 < utf8.size()) {
            cp = static_cast<uint32_t>(c & 0x1F) << 6
               | (static_cast<unsigned char>(utf8[i + 1]) & 0x3F);
            i += 2;
        } else if ((c & 0xF0) == 0xE0 && i + 2 < utf8.size()) {
            cp = static_cast<uint32_t>(c & 0x0F) << 12
               | (static_cast<uint32_t>(static_cast<unsigned char>(utf8[i + 1]) & 0x3F) << 6)
               | (static_cast<unsigned char>(utf8[i + 2]) & 0x3F);
            i += 3;
        } else if ((c & 0xF8) == 0xF0 && i + 3 < utf8.size()) {
            cp = static_cast<uint32_t>(c & 0x07) << 18
               | (static_cast<uint32_t>(static_cast<unsigned char>(utf8[i + 1]) & 0x3F) << 12)
               | (static_cast<uint32_t>(static_cast<unsigned char>(utf8[i + 2]) & 0x3F) << 6)
               | (static_cast<unsigned char>(utf8[i + 3]) & 0x3F);
            i += 4;
        } else {
            cp = 0xFFFD;  // replacement character for invalid sequences
            i += 1;
        }
        if (cp <= 0xFFFF) {
            result.push_back(static_cast<wchar_t>(cp));
        } else {
            cp -= 0x10000;
            result.push_back(static_cast<wchar_t>(0xD800 + (cp >> 10)));
            result.push_back(static_cast<wchar_t>(0xDC00 + (cp & 0x3FF)));
        }
    }
    return result;
}

int OpenPathForRead(const std::string& utf8Path, int flags) {
    return ::_wopen(Utf8PathToWide(utf8Path).c_str(), flags);
}
#else
int OpenPathForRead(const std::string& utf8Path, int flags) {
    return ::open(utf8Path.c_str(), flags);
}
#endif

class FileReader {
public:
    int fd{-1};
    int64_t pos{0};
    int64_t size{0};

    ~FileReader() { if (fd >= 0) CLOSE(fd); }
    void Open(const std::string& path) {
        fd = OpenPathForRead(path, O_RDONLY | O_BINARY);
        if (fd < 0) Fail("E2: cannot open file: " + path);
        struct stat st{};
        if (fstat(fd, &st) != 0) Fail("E2: stat failed");
        size = st.st_size;
    }
    void Read(void* buf, size_t n, const char* what) {
        size_t got = 0;
        uint8_t* dst = static_cast<uint8_t*>(buf);
        while (got < n) {
            auto r = READ(fd, dst + got, static_cast<unsigned>(n - got));
            if (r <= 0) Fail(std::string("E2: short read on ") + what);
            got += static_cast<size_t>(r);
        }
        pos += static_cast<int64_t>(n);
    }
    void Seek(int64_t off) {
        if (LSEEK(fd, off, SEEK_SET) < 0) Fail("E2: seek failed");
        pos = off;
    }
    // Advance past `n` bytes without transferring them. `what` names the
    // region being skipped so a truncated file reports where it ran out.
    void Skip(int64_t n, const char* what) {
        if (n < 0 || n > size - pos)
            Fail(std::string("E2: short read on ") + what);
        Seek(pos + n);
    }
};

}  // anonymous namespace

// After the 0x00 end-of-data marker the file carries a trailer that ends in the
// ASCII bytes "YXE2". Its inner layout is not interpreted, but a file that
// stops before it was cut short.
static void CheckTrailer(FileReader& f) {
    const int64_t remaining = f.size - f.pos;
    if (remaining < 4)
        Fail("E2: file ends right after the end-of-data marker (missing trailer)");
    f.Seek(f.size - 4);
    uint8_t tail[4];
    f.Read(tail, 4, "trailer");
    if (std::memcmp(tail, "YXE2", 4) != 0)
        Fail("E2: file does not end with the 'YXE2' trailer");
}

struct E2Reader::Impl {
    FileReader file;
    // Resolved blob payloads. Keyed by the file offset where the
    // type 0x01 block's type byte starts.
    std::unordered_map<uint64_t, std::vector<uint8_t>> blobBlocks;
    // For 0x11 (string-style) refs we use the most recent blob block
    // as the concatenated source.
    std::vector<uint8_t>* lastBlob{nullptr};

    bool hasDateFlag{false};
    bool dateFlagDetected{false};
    bool hasInt32{false};
    uint8_t int32Base{6};
};

E2Reader::~E2Reader() = default;

E2Reader::E2Reader(const std::string& path) : m_impl(std::make_unique<Impl>()) {
    m_impl->file.Open(path);

    if (m_impl->file.size < 100)
        Fail("E2: file too small (< 100 bytes)");

    uint8_t hdr[100];
    m_impl->file.Read(hdr, sizeof(hdr), "header");
    if (!LooksLikeE2(hdr, sizeof(hdr)))
        Fail("E2: file does not start with 'Alteryx e2 Database file'");
    uint32_t file_id = LoadU32LE(hdr + 64);
    if (file_id != 0x00440208u) {
        char msg[80];
        std::snprintf(msg, sizeof(msg),
                      "E2: unexpected file id 0x%08X (expected 0x00440208)",
                      file_id);
        Fail(msg);
    }
    uint32_t meta_size = LoadU32LE(hdr + 96);
    if (meta_size == 0 || meta_size > 64u * 1024u * 1024u)
        Fail("E2: metadata size out of range");

    m_metaXml.assign(meta_size, '\0');
    m_impl->file.Read(m_metaXml.data(), meta_size, "metadata");

    ParseMetaXml(m_metaXml, m_schema);
    m_impl->hasInt32 = std::any_of(
        m_schema.begin(), m_schema.end(), [](const E2FieldMeta& field) {
            return field.type == E2FieldType::Int32;
        });
    // Record count is computed lazily on demand.
}

// ---------------------------------------------------------------------------
//                          Record decoding
// ---------------------------------------------------------------------------

namespace {

// Try to decode a record with a given date-flag setting; return bytes
// consumed before any error. Used by the date-flag auto-detector.
size_t TryDecodeRecord(const std::vector<E2FieldMeta>& fields,
                       const uint8_t* p, size_t len, bool has_date_flag,
                       uint8_t int32_base) {
    size_t off = 0;
    bool first_date = true;
    for (const auto& f : fields) {
        bool is_date = (f.type == E2FieldType::Date);
        try {
            if (is_date && first_date && has_date_flag) {
                if (off >= len || p[off] != 0x00) return off;
                off += 1;
            }
            size_t c = 0;
            switch (f.type) {
                case E2FieldType::Bool:
                    DecodeBool(p + off, len - off, &c); break;
                case E2FieldType::Byte:
                    DecodeCompactInt(p + off, len - off, &c, 6, kNullByte, 1, true); break;
                case E2FieldType::Int16:
                    DecodeCompactInt(p + off, len - off, &c, 6, kNullInt16, 2); break;
                case E2FieldType::Int32:
                    DecodeCompactInt(p + off, len - off, &c, int32_base, kNullInt32, 4); break;
                case E2FieldType::Int64:
                    DecodeCompactInt(p + off, len - off, &c, 6, kNullInt64, 8); break;
                case E2FieldType::Float:
                    DecodeFloat(p + off, len - off, &c); break;
                case E2FieldType::Double:
                    DecodeDouble(p + off, len - off, &c); break;
                case E2FieldType::String:
                case E2FieldType::WString:
                case E2FieldType::V_String:
                case E2FieldType::V_WString:
                    DecodeStringLike(p + off, len - off, &c, kNullString, 0x01, 0x11); break;
                case E2FieldType::Date:
                    DecodeDate(p + off, len - off, &c); break;
                case E2FieldType::DateTime:
                    DecodeDateTime(p + off, len - off, &c); break;
                case E2FieldType::Time:
                    DecodeTime(p + off, len - off, &c); break;
                case E2FieldType::FixedDecimal:
                    DecodeFixedDecimal(p + off, len - off, &c); break;
                case E2FieldType::Blob:
                    DecodeBlobOrSpatial(p + off, len - off, &c, 0x02, 0x12, kNullString); break;
                case E2FieldType::SpatialObj:
                    DecodeBlobOrSpatial(p + off, len - off, &c, 0x03, 0x13, kNullSpatial); break;
            }
            off += c;
            if (is_date) first_date = false;
        } catch (...) {
            return off;
        }
    }
    return off;
}

E2Cell DecodeOneField(const E2FieldMeta& f,
                      const uint8_t* p, size_t len, size_t* consumed,
                      uint8_t int32_base) {
    switch (f.type) {
        case E2FieldType::Bool:
            return DecodeBool(p, len, consumed);
        case E2FieldType::Byte:
            return DecodeCompactInt(p, len, consumed, 6, kNullByte, 1, true);
        case E2FieldType::Int16:
            return DecodeCompactInt(p, len, consumed, 6, kNullInt16, 2);
        case E2FieldType::Int32:
            return DecodeCompactInt(p, len, consumed, int32_base, kNullInt32, 4);
        case E2FieldType::Int64:
            return DecodeCompactInt(p, len, consumed, 6, kNullInt64, 8);
        case E2FieldType::Float:
            return DecodeFloat(p, len, consumed);
        case E2FieldType::Double:
            return DecodeDouble(p, len, consumed);
        case E2FieldType::String:
        case E2FieldType::WString:
        case E2FieldType::V_String:
        case E2FieldType::V_WString:
            return DecodeStringLike(p, len, consumed, kNullString, 0x01, 0x11);
        case E2FieldType::Date:
            return DecodeDate(p, len, consumed);
        case E2FieldType::DateTime:
            return DecodeDateTime(p, len, consumed);
        case E2FieldType::Time:
            return DecodeTime(p, len, consumed);
        case E2FieldType::FixedDecimal:
            return DecodeFixedDecimal(p, len, consumed);
        case E2FieldType::Blob:
            return DecodeBlobOrSpatial(p, len, consumed, 0x02, 0x12, kNullString);
        case E2FieldType::SpatialObj:
            return DecodeBlobOrSpatial(p, len, consumed, 0x03, 0x13, kNullSpatial);
    }
    Fail("E2: unhandled field type");
}

// Decode using one file-wide Int32 prefix base and require exact framing.
bool DecodeRecordUniform(const std::vector<E2FieldMeta>& fields,
                         const uint8_t* p, size_t len, bool has_date_flag,
                         uint8_t int32_base, std::vector<E2Cell>& output,
                         bool* saw_ambiguous_prefix = nullptr) {
    output.clear();
    output.reserve(fields.size());
    if (saw_ambiguous_prefix) *saw_ambiguous_prefix = false;
    size_t off = 0;
    bool first_date = true;
    try {
        for (const auto& field : fields) {
            const bool is_date = field.type == E2FieldType::Date;
            if (is_date && first_date && has_date_flag) {
                if (off >= len || p[off] != 0x00) return false;
                ++off;
            }
            if (saw_ambiguous_prefix && field.type == E2FieldType::Int32 &&
                off < len && p[off] == 0x09)
                *saw_ambiguous_prefix = true;
            size_t consumed = 0;
            output.push_back(DecodeOneField(
                field, p + off, len - off, &consumed, int32_base));
            off += consumed;
            if (is_date) first_date = false;
        }
    } catch (...) {
        output.clear();
        return false;
    }
    if (off != len) {
        output.clear();
        return false;
    }
    return true;
}

enum class MixedDecodeResult { None, Unique, Ambiguous, SearchLimit };

bool SameCell(const E2Cell& left, const E2Cell& right) {
    if (left.kind != right.kind) return false;
    switch (left.kind) {
        case E2Cell::Kind::Null: return true;
        case E2Cell::Kind::Bool: return left.boolean == right.boolean;
        case E2Cell::Kind::Int64: return left.integer == right.integer;
        case E2Cell::Kind::Double:
            return std::memcmp(&left.real, &right.real, sizeof(double)) == 0;
        case E2Cell::Kind::Text: return left.text == right.text;
        case E2Cell::Kind::Bytes: return left.bytes == right.bytes;
        case E2Cell::Kind::Reference:
            return left.referenceTag == right.referenceTag &&
                   left.bytes == right.bytes;
    }
    return false;
}

bool SameRow(const std::vector<E2Cell>& left, const std::vector<E2Cell>& right) {
    if (left.size() != right.size()) return false;
    for (size_t i = 0; i < left.size(); ++i)
        if (!SameCell(left[i], right[i])) return false;
    return true;
}

// Some corpus producers mix the two observed Int32 prefix bases within one
// record. Count complete parses, capped at two, then reconstruct the sole
// path. Layered dynamic programming prevents recursion and exponential
// backtracking; the state budget also bounds adversarial wide records.
MixedDecodeResult DecodeRecordMixedUnique(
        const std::vector<E2FieldMeta>& fields,
        const uint8_t* p, size_t len, bool has_date_flag,
        uint8_t preferred_int32_base, std::vector<E2Cell>& output) {
    constexpr size_t kMaxSearchStates = 8192;
    const uint8_t alternate_int32_base =
        preferred_int32_base == 5 ? uint8_t{6} : uint8_t{5};
    struct Node {
        uint8_t paths{0};
        size_t previous{0};
        uint8_t int32_base{6};
    };
    struct Candidate {
        E2Cell cell;
        size_t consumed{0};
        uint8_t int32_base{6};
    };
    std::vector<std::unordered_map<size_t, Node>> layers(fields.size() + 1);
    layers[0].emplace(0, Node{1, 0, preferred_int32_base});
    size_t states = 1;

    auto field_offset = [&](size_t field_index, size_t off, size_t* adjusted) {
        *adjusted = off;
        if (!has_date_flag || fields[field_index].type != E2FieldType::Date)
            return true;
        bool first_date = true;
        for (size_t i = 0; i < field_index; ++i) {
            if (fields[i].type == E2FieldType::Date) {
                first_date = false;
                break;
            }
        }
        if (!first_date) return true;
        if (*adjusted >= len || p[*adjusted] != 0x00) return false;
        ++*adjusted;
        return true;
    };

    auto candidates = [&](size_t field_index, size_t off,
                          std::array<Candidate, 2>& decoded) {
        const auto& field = fields[field_index];
        const uint8_t bases[2] = {preferred_int32_base, alternate_int32_base};
        const size_t base_count = field.type == E2FieldType::Int32 &&
                                  off < len && p[off] == 0x09 ? 2 : 1;
        size_t count = 0;
        for (size_t i = 0; i < base_count; ++i) {
            size_t consumed = 0;
            try {
                E2Cell cell = DecodeOneField(
                    field, p + off, len - off, &consumed, bases[i]);
                bool duplicate = false;
                for (size_t j = 0; j < count; ++j)
                    duplicate = duplicate || decoded[j].consumed == consumed;
                if (!duplicate)
                    decoded[count++] = {std::move(cell), consumed, bases[i]};
            } catch (...) {
                // This base is not valid at this byte offset.
            }
        }
        return count;
    };

    for (size_t field_index = 0; field_index < fields.size(); ++field_index) {
        auto& next = layers[field_index + 1];
        for (const auto& entry : layers[field_index]) {
            size_t adjusted = 0;
            if (!field_offset(field_index, entry.first, &adjusted)) continue;
            std::array<Candidate, 2> decoded;
            const size_t count = candidates(field_index, adjusted, decoded);
            for (size_t i = 0; i < count; ++i) {
                const size_t next_offset = adjusted + decoded[i].consumed;
                if (next_offset > len) continue;
                auto inserted = next.emplace(
                    next_offset,
                    Node{entry.second.paths, entry.first,
                         decoded[i].int32_base});
                if (inserted.second) {
                    if (++states > kMaxSearchStates)
                        return MixedDecodeResult::SearchLimit;
                } else {
                    inserted.first->second.paths = static_cast<uint8_t>(
                        std::min<int>(2, inserted.first->second.paths +
                                            entry.second.paths));
                }
            }
        }
        if (next.empty()) return MixedDecodeResult::None;
    }

    auto complete = layers.back().find(len);
    if (complete == layers.back().end()) return MixedDecodeResult::None;
    if (complete->second.paths > 1) return MixedDecodeResult::Ambiguous;

    output.resize(fields.size());
    size_t off = len;
    for (size_t field_index = fields.size(); field_index-- > 0;) {
        const auto node = layers[field_index + 1].at(off);
        size_t adjusted = 0;
        if (!field_offset(field_index, node.previous, &adjusted))
            return MixedDecodeResult::None;
        size_t consumed = 0;
        output[field_index] = DecodeOneField(
            fields[field_index], p + adjusted, len - adjusted, &consumed,
            node.int32_base);
        off = node.previous;
    }
    return off == 0 ? MixedDecodeResult::Unique : MixedDecodeResult::None;
}

// Frame variable-length records inside a decompressed block.
// Returns spans (offset,length) into `block`.
struct Span { size_t offset; size_t length; };

void FrameRecords(const std::vector<uint8_t>& block, std::vector<Span>& out) {
    if (block.size() < 12) Fail("E2: decompressed block < 12 bytes");
    uint32_t rec_count = LoadU32LE(block.data() + 4);
    uint32_t first_size = LoadU32LE(block.data() + 8) & 0x7FFFFFFFu;
    if (rec_count == 0) return;
    const size_t max_records = 1 + (block.size() - 12) / 4;
    if (rec_count > max_records)
        Fail("E2: record count exceeds framed block capacity");
    out.reserve(rec_count);
    size_t pos = 12;
    if (pos + first_size > block.size())
        Fail("E2: first record exceeds block size");
    out.push_back({pos, first_size});
    pos += first_size;
    for (uint32_t i = 1; i < rec_count; ++i) {
        if (pos + 4 > block.size()) Fail("E2: missing record size prefix");
        uint32_t sz = LoadU32LE(block.data() + pos) & 0x7FFFFFFFu;
        pos += 4;
        if (pos + sz > block.size()) Fail("E2: record exceeds block size");
        out.push_back({pos, sz});
        pos += sz;
    }
    if (pos != block.size())
        Fail("E2: " + std::to_string(block.size() - pos) +
             " unread bytes after the last record of a block");
}

uint8_t DetectInt32Base(const std::vector<E2FieldMeta>& fields,
                        const std::vector<uint8_t>& block,
                        const std::vector<Span>& spans,
                        bool has_date_flag) {
    if (std::none_of(fields.begin(), fields.end(), [](const E2FieldMeta& field) {
            return field.type == E2FieldType::Int32;
        })) {
        return 6;
    }

    auto score = [&](uint8_t base) {
        size_t complete = 0;
        size_t consumed_total = 0;
        const size_t sample_size = std::min<size_t>(spans.size(), 128);
        for (size_t i = 0; i < sample_size; ++i) {
            const auto& span = spans[i];
            const size_t consumed = TryDecodeRecord(
                fields, block.data() + span.offset, span.length, has_date_flag, base);
            complete += consumed == span.length;
            consumed_total += consumed;
        }
        return std::make_pair(complete, consumed_total);
    };

    return score(5) > score(6) ? 5 : 6;
}

}  // anonymous namespace

void E2Reader::ReadAllColumns(std::vector<std::vector<E2Cell>>& columns) {
    std::vector<size_t> all_indices(m_schema.size());
    for (size_t i = 0; i < all_indices.size(); ++i) all_indices[i] = i;
    ReadColumnsSubset(all_indices, 0, -1, columns);
}

int64_t E2Reader::NumRecords() {
    if (m_recordCount >= 0) return m_recordCount;

    int64_t data_start = 100 + static_cast<int64_t>(m_metaXml.size());
    m_impl->file.Seek(data_start);

    int64_t total = 0;
    std::vector<uint8_t> compressed;
    std::vector<uint8_t> decompressed;
    std::vector<Span> spans;

    bool saw_end = false;
    while (m_impl->file.pos < m_impl->file.size) {
        uint8_t type_byte;
        m_impl->file.Read(&type_byte, 1, "block type");

        if (type_byte == 0x00) {
            CheckTrailer(m_impl->file);
            saw_end = true;
            break;
        }

        if (type_byte == 0x01) {
            uint8_t size_buf[4];
            m_impl->file.Read(size_buf, 4, "blob block size");
            uint32_t block_size = LoadU32LE(size_buf);
            uint8_t hdr20[20];
            m_impl->file.Read(hdr20, 20, "blob block header");
            m_impl->file.Skip(block_size, "blob skip");
            continue;
        }

        if (type_byte == 0x03 || type_byte == 0x04) {
            uint8_t size_buf[4];
            m_impl->file.Read(size_buf, 4, "spatial size");
            uint32_t block_size = LoadU32LE(size_buf);
            uint8_t inner[4];
            m_impl->file.Read(inner, 4, "spatial inner");
            m_impl->file.Skip(block_size, "spatial skip");
            continue;
        }

        if (type_byte != 0x02) Fail("E2: unknown block type");

        uint8_t size_buf[4];
        m_impl->file.Read(size_buf, 4, "block size");
        uint32_t block_size = LoadU32LE(size_buf);
        compressed.assign(block_size, 0);
        m_impl->file.Read(compressed.data(), block_size, "block payload");
        if (compressed.empty() || compressed[0] != 0x0A)
            Fail("E2: record block missing 0x0A marker");
        decompressed.clear();
        SnappyDecompressRaw(compressed.data() + 1, compressed.size() - 1,
                            decompressed);
        spans.clear();
        FrameRecords(decompressed, spans);
        if (spans.size() > static_cast<size_t>(
                std::numeric_limits<int64_t>::max() - total))
            Fail("E2: record count overflow");
        total += static_cast<int64_t>(spans.size());
    }
    if (!saw_end)
        Fail("E2: file ends without an end-of-data marker (truncated?)");
    m_recordCount = total;
    return total;
}

void E2Reader::ReadColumnsSubset(const std::vector<size_t>& projection,
                                 int64_t offset,
                                 int64_t limit,
                                 std::vector<std::vector<E2Cell>>& columns) {
    columns.clear();
    columns.resize(projection.size());

    // Restart from the start of the data section.
    int64_t data_start = 100 + static_cast<int64_t>(m_metaXml.size());
    m_impl->file.Seek(data_start);
    m_impl->blobBlocks.clear();
    m_impl->lastBlob = nullptr;

    int64_t produced = 0;
    int64_t skipped = 0;
    bool reached_limit = false;

    std::vector<uint8_t> compressed;
    std::vector<uint8_t> decompressed;
    std::vector<Span> spans;
    // Reused across records so the decode loop does not reallocate per record.
    std::vector<E2Cell> row;

    bool saw_end = false;
    while (!reached_limit && m_impl->file.pos < m_impl->file.size) {
        uint8_t type_byte;
        m_impl->file.Read(&type_byte, 1, "block type");
        int64_t block_pos = m_impl->file.pos - 1;

        if (type_byte == 0x00) {  // end of stream
            CheckTrailer(m_impl->file);
            saw_end = true;
            break;
        }

        if (type_byte == 0x01) {
            // Blob block. We capture it for later 0x12/0x13 lookups by its
            // file offset (the position of the type byte).
            uint8_t size_buf[4];
            m_impl->file.Read(size_buf, 4, "blob block size");
            uint32_t block_size = LoadU32LE(size_buf);
            uint8_t hdr20[20];
            m_impl->file.Read(hdr20, 20, "blob block header");
            compressed.assign(block_size, 0);
            m_impl->file.Read(compressed.data(), block_size, "blob payload");
            if (compressed.empty() || compressed[0] != 0x0A)
                Fail("E2: blob block missing 0x0A marker");
            decompressed.clear();
            SnappyDecompressRaw(compressed.data() + 1, compressed.size() - 1,
                                decompressed);
            auto& slot = m_impl->blobBlocks[static_cast<uint64_t>(block_pos)];
            slot = std::move(decompressed);
            m_impl->lastBlob = &slot;
            continue;
        }

        if (type_byte == 0x03 || type_byte == 0x04) {
            // Spatial index block - read sizes, skip payload.
            uint8_t size_buf[4];
            m_impl->file.Read(size_buf, 4, "spatial size");
            uint32_t block_size = LoadU32LE(size_buf);
            uint8_t inner[4];
            m_impl->file.Read(inner, 4, "spatial inner");
            m_impl->file.Skip(block_size, "spatial payload");
            continue;
        }

        if (type_byte != 0x02) {
            // Unknown - bail.
            Fail("E2: unknown block type");
        }

        uint8_t size_buf[4];
        m_impl->file.Read(size_buf, 4, "block size");
        uint32_t block_size = LoadU32LE(size_buf);
        compressed.assign(block_size, 0);
        m_impl->file.Read(compressed.data(), block_size, "block payload");
        if (compressed.empty() || compressed[0] != 0x0A)
            Fail("E2: record block missing 0x0A marker");
        decompressed.clear();
        SnappyDecompressRaw(compressed.data() + 1, compressed.size() - 1,
                            decompressed);

        spans.clear();
        FrameRecords(decompressed, spans);

        // Auto-detect the date-flag byte on the first non-empty record block.
        if (!m_impl->dateFlagDetected && !spans.empty()) {
            bool any_date = false;
            for (const auto& f : m_schema)
                if (f.type == E2FieldType::Date) { any_date = true; break; }
            if (any_date) {
                const auto& span = spans[0];
                const uint8_t* p = decompressed.data() + span.offset;
                size_t without = TryDecodeRecord(
                    m_schema, p, span.length, false, m_impl->int32Base);
                size_t with = TryDecodeRecord(
                    m_schema, p, span.length, true, m_impl->int32Base);
                m_impl->hasDateFlag = with > without;
            }
            m_impl->int32Base = DetectInt32Base(
                m_schema, decompressed, spans, m_impl->hasDateFlag);
            if (any_date) {
                const auto& span = spans[0];
                const uint8_t* p = decompressed.data() + span.offset;
                size_t without = TryDecodeRecord(
                    m_schema, p, span.length, false, m_impl->int32Base);
                size_t with = TryDecodeRecord(
                    m_schema, p, span.length, true, m_impl->int32Base);
                m_impl->hasDateFlag = with > without;
            }
            m_impl->dateFlagDetected = true;
        }

        for (const auto& span : spans) {
            if (skipped < offset) { ++skipped; continue; }
            if (limit >= 0 && produced >= limit) { reached_limit = true; break; }

            const uint8_t* base = decompressed.data() + span.offset;
            if (m_impl->hasInt32) {
                const uint8_t alternate_base = m_impl->int32Base == 5 ? 6 : 5;
                std::vector<E2Cell> alternate_row;
                bool preferred_saw_09 = false;
                bool alternate_saw_09 = false;
                const bool preferred_exact = DecodeRecordUniform(
                    m_schema, base, span.length, m_impl->hasDateFlag,
                    m_impl->int32Base, row, &preferred_saw_09);
                const bool alternate_exact = DecodeRecordUniform(
                    m_schema, base, span.length, m_impl->hasDateFlag,
                    alternate_base, alternate_row, &alternate_saw_09);

                // Prefix 0x09 is the only corpus-observed per-field width
                // ambiguity, so mixed searches branch only there. Complete
                // uniform parses under both bases must still agree on values.
                if (preferred_exact && alternate_exact &&
                    !preferred_saw_09 && !alternate_saw_09) {
                    if (!SameRow(row, alternate_row))
                        Fail("E2: ambiguous Int32 encoding in record " +
                             std::to_string(skipped + produced));
                    // Both encodings produce the same row. Keep `row`.
                } else {
                    std::vector<E2Cell> preferred_mixed;
                    std::vector<E2Cell> alternate_mixed;
                    const auto preferred_result = DecodeRecordMixedUnique(
                        m_schema, base, span.length, m_impl->hasDateFlag,
                        m_impl->int32Base, preferred_mixed);
                    const auto alternate_result = DecodeRecordMixedUnique(
                        m_schema, base, span.length, m_impl->hasDateFlag,
                        alternate_base, alternate_mixed);
                    if (preferred_result == MixedDecodeResult::SearchLimit ||
                        alternate_result == MixedDecodeResult::SearchLimit)
                        Fail("E2: Int32 ambiguity search limit exceeded in record " +
                             std::to_string(skipped + produced));
                    if (preferred_result == MixedDecodeResult::Ambiguous ||
                        alternate_result == MixedDecodeResult::Ambiguous ||
                        (preferred_result == MixedDecodeResult::Unique &&
                         alternate_result == MixedDecodeResult::Unique &&
                         !SameRow(preferred_mixed, alternate_mixed)))
                        Fail("E2: ambiguous Int32 encoding in record " +
                             std::to_string(skipped + produced));
                    if (preferred_result == MixedDecodeResult::Unique) {
                        row = std::move(preferred_mixed);
                    } else if (alternate_result == MixedDecodeResult::Unique) {
                        row = std::move(alternate_mixed);
                    } else {
                        Fail("E2: failed to decode record " +
                             std::to_string(skipped + produced) +
                             ": no exact match for the declared schema");
                    }
                }
            } else if (!DecodeRecordUniform(
                           m_schema, base, span.length, m_impl->hasDateFlag,
                           m_impl->int32Base, row)) {
                Fail("E2: failed to decode record " +
                     std::to_string(skipped + produced) +
                     ": no exact match for the declared schema");
            }
            // Resolve any blob refs in projected fields.
            for (size_t pi = 0; pi < projection.size(); ++pi) {
                size_t idx = projection[pi];
                E2Cell& cell = row[idx];
                if (cell.kind == E2Cell::Kind::Text) {
                    if (m_schema[idx].type == E2FieldType::Time)
                        CheckTimeOfDay(static_cast<uint64_t>(cell.integer), "Time");
                    else if (m_schema[idx].type == E2FieldType::DateTime)
                        CheckTimeOfDay(static_cast<uint64_t>(cell.integer), "DateTime");
                }
                if (cell.kind == E2Cell::Kind::Reference) {
                        uint8_t tag = cell.referenceTag;
                        const uint8_t* payload = cell.bytes.data();
                        const auto& ft = m_schema[idx].type;
                        if (tag == 0x01) {
                            // string-style ref: u32 offset + u32 length into lastBlob
                            uint32_t boff = LoadU32LE(payload);
                            uint32_t blen = LoadU32LE(payload + 4);
                            // The reference does not say which blob block it
                            // points into; the most recent one is assumed. With
                            // more than one block seen that assumption cannot
                            // be checked, so refuse rather than guess.
                            if (m_impl->blobBlocks.size() > 1)
                                Fail("E2: inline blob reference in record " +
                                     std::to_string(skipped + produced) +
                                     " could point into any of " +
                                     std::to_string(m_impl->blobBlocks.size()) + " blob blocks");
                            if (m_impl->lastBlob &&
                                static_cast<size_t>(boff) + blen <= m_impl->lastBlob->size()) {
                                const uint8_t* d = m_impl->lastBlob->data() + boff;
                                if (ft == E2FieldType::Blob || ft == E2FieldType::SpatialObj) {
                                    cell = E2Cell::MakeBytes(
                                        std::vector<uint8_t>(d, d + blen));
                                } else {
                                    cell = E2Cell::MakeText(std::string(
                                        reinterpret_cast<const char*>(d), blen));
                                }
                            } else {
                                Fail("E2: unresolved inline blob reference in record " +
                                     std::to_string(skipped + produced));
                            }
                        } else if (tag == 0x02 || tag == 0x03) {
                            // file-offset ref: u64 LE
                            uint64_t foff = LoadU64LE(payload);
                            auto it = m_impl->blobBlocks.find(foff);
                            if (it != m_impl->blobBlocks.end()) {
                                cell = E2Cell::MakeBytes(it->second);
                            } else {
                                Fail("E2: unresolved file-offset blob reference in record " +
                                     std::to_string(skipped + produced));
                            }
                        } else {
                            Fail("E2: invalid blob reference tag in record " +
                                 std::to_string(skipped + produced));
                        }
                }
                columns[pi].push_back(std::move(cell));
            }
            ++produced;
        }
    }
    if (!saw_end && !reached_limit)
        Fail("E2: file ends without an end-of-data marker (truncated?)");
    // A limited read stops early, so it does not establish the file's total.
    if (limit < 0) m_recordCount = produced + skipped;
}

}}}  // namespace Alteryx::OpenYXDB::e2
