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

[[noreturn]] void Fail(const std::string& msg) { throw std::runtime_error(msg); }

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
    size_t p = tag.find(key);
    if (p == std::string::npos) return {};
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
        if (m.name.empty() || type_str.empty()) continue;
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
                        uint8_t base, uint8_t null_byte, int max_bytes) {
    NeedBytes(1, len, "Int");
    uint8_t prefix = p[0];
    if (prefix == null_byte) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix < base)       { *consumed = 1; return E2Cell::MakeNull(); }
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
    if (n == max_bytes && max_bytes < 8) {
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
    if (prefix < 0x07)        { *consumed = 1; return E2Cell::MakeNull(); }
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
    if (prefix < 0x06) { *consumed = 1; return E2Cell::MakeNull(); }
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
//   - Bytes cell with a fixed 9-byte payload representing a blob ref;
//     the caller resolves it against blob_blocks. We tag blob refs with
//     a special leading byte (0xFF, type_class) to identify them.
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
        // Encode as Bytes with 9-byte marker:
        //   [type_class] [8 bytes payload]
        // type_class 0x01 -> string ref (u32 offset + u32 length inline)
        // type_class 0x02/0x03 -> file-offset reference (u64 LE)
        NeedBytes(9, len, "blob ref");
        std::vector<uint8_t> tag;
        tag.reserve(9);
        tag.push_back(blob_ref_prefix & 0x0F);
        tag.insert(tag.end(), p + 1, p + 9);
        *consumed = 9;
        return E2Cell::MakeBytes(std::move(tag));
    }
    Fail("E2 String-like: invalid prefix");
}

E2Cell DecodeDate(const uint8_t* p, size_t len, size_t* consumed) {
    NeedBytes(1, len, "Date");
    uint8_t prefix = p[0];
    if (prefix == kNullDate) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix < 0x0A) { *consumed = 1; return E2Cell::MakeNull(); }
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
    if (prefix < 0x08) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix == 0x08) {
        *consumed = 1; return E2Cell::MakeText(FormatDateTimePacked(0));
    }
    int n = prefix - 0x08;
    if (n < 1 || n > 6) Fail("E2 DateTime: invalid prefix");
    NeedBytes(1 + n, len, "DateTime payload");
    uint8_t buf[8] = {0,0,0,0,0,0,0,0};
    std::memcpy(buf, p + 1, n);
    uint64_t raw = LoadU64LE(buf);
    *consumed = 1 + n;
    return E2Cell::MakeText(FormatDateTimePacked(raw));
}

E2Cell DecodeTime(const uint8_t* p, size_t len, size_t* consumed) {
    NeedBytes(1, len, "Time");
    uint8_t prefix = p[0];
    if (prefix == kNullTime) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix < 0x0C) { *consumed = 1; return E2Cell::MakeNull(); }
    if (prefix == 0x0C) { *consumed = 1; return E2Cell::MakeText("00:00:00"); }
    int n = prefix - 0x0C;
    if (n < 1 || n > 4) Fail("E2 Time: invalid prefix");
    NeedBytes(1 + n, len, "Time payload");
    uint8_t buf[4] = {0,0,0,0};
    std::memcpy(buf, p + 1, n);
    uint64_t centi = LoadU32LE(buf);
    *consumed = 1 + n;
    return E2Cell::MakeText(FormatTimeCenti(centi));
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
        if (nib > 9) {
            // Some encoders pad with high nibbles; we treat anything >9 as 0.
            nib = 0;
        }
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
        std::vector<uint8_t> tag;
        tag.reserve(9);
        tag.push_back(prefix & 0x0F);
        tag.insert(tag.end(), p + 1, p + 9);
        *consumed = 9;
        return E2Cell::MakeBytes(std::move(tag));
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
};

}  // anonymous namespace

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
    // Record count is computed lazily on demand.
}

// ---------------------------------------------------------------------------
//                          Record decoding
// ---------------------------------------------------------------------------

namespace {

// Try to decode a record with a given date-flag setting; return bytes
// consumed before any error. Used by the date-flag auto-detector.
size_t TryDecodeRecord(const std::vector<E2FieldMeta>& fields,
                       const uint8_t* p, size_t len, bool has_date_flag) {
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
                    DecodeCompactInt(p + off, len - off, &c, 6, kNullByte, 1); break;
                case E2FieldType::Int16:
                    DecodeCompactInt(p + off, len - off, &c, 6, kNullInt16, 2); break;
                case E2FieldType::Int32:
                    DecodeCompactInt(p + off, len - off, &c, 6, kNullInt32, 4); break;
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
                      const uint8_t* p, size_t len, size_t* consumed) {
    switch (f.type) {
        case E2FieldType::Bool:
            return DecodeBool(p, len, consumed);
        case E2FieldType::Byte:
            return DecodeCompactInt(p, len, consumed, 6, kNullByte, 1);
        case E2FieldType::Int16:
            return DecodeCompactInt(p, len, consumed, 6, kNullInt16, 2);
        case E2FieldType::Int32:
            return DecodeCompactInt(p, len, consumed, 6, kNullInt32, 4);
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

// Frame variable-length records inside a decompressed block.
// Returns spans (offset,length) into `block`.
struct Span { size_t offset; size_t length; };

void FrameRecords(const std::vector<uint8_t>& block, std::vector<Span>& out) {
    if (block.size() < 12) Fail("E2: decompressed block < 12 bytes");
    uint32_t rec_count = LoadU32LE(block.data() + 4);
    uint32_t first_size = LoadU32LE(block.data() + 8) & 0x7FFFFFFFu;
    if (rec_count == 0) return;
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

    while (m_impl->file.pos < m_impl->file.size) {
        uint8_t type_byte;
        m_impl->file.Read(&type_byte, 1, "block type");

        if (type_byte == 0x00) break;

        if (type_byte == 0x01) {
            uint8_t size_buf[4];
            m_impl->file.Read(size_buf, 4, "blob block size");
            uint32_t block_size = LoadU32LE(size_buf);
            uint8_t hdr20[20];
            m_impl->file.Read(hdr20, 20, "blob block header");
            std::vector<uint8_t> skip(block_size);
            if (block_size) m_impl->file.Read(skip.data(), block_size, "blob skip");
            continue;
        }

        if (type_byte == 0x03 || type_byte == 0x04) {
            uint8_t size_buf[4];
            m_impl->file.Read(size_buf, 4, "spatial size");
            uint32_t block_size = LoadU32LE(size_buf);
            uint8_t inner[4];
            m_impl->file.Read(inner, 4, "spatial inner");
            std::vector<uint8_t> skip(block_size);
            if (block_size) m_impl->file.Read(skip.data(), block_size, "spatial skip");
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
        if (decompressed.size() < 8) Fail("E2: short decompressed block");
        uint32_t rec_count = LoadU32LE(decompressed.data() + 4);
        total += rec_count;
    }
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

    int64_t produced = 0;
    int64_t skipped = 0;
    bool reached_limit = false;

    std::vector<uint8_t> compressed;
    std::vector<uint8_t> decompressed;
    std::vector<Span> spans;

    while (!reached_limit && m_impl->file.pos < m_impl->file.size) {
        uint8_t type_byte;
        m_impl->file.Read(&type_byte, 1, "block type");
        int64_t block_pos = m_impl->file.pos - 1;

        if (type_byte == 0x00) break;  // end of stream

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
            slot = decompressed;
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
            std::vector<uint8_t> skip(block_size);
            if (block_size) m_impl->file.Read(skip.data(), block_size, "spatial payload");
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
                size_t without = TryDecodeRecord(m_schema, p, span.length, false);
                size_t with    = TryDecodeRecord(m_schema, p, span.length, true);
                m_impl->hasDateFlag = with > without;
            }
            m_impl->dateFlagDetected = true;
        }

        for (const auto& span : spans) {
            if (skipped < offset) { ++skipped; continue; }
            if (limit >= 0 && produced >= limit) { reached_limit = true; break; }

            const uint8_t* base = decompressed.data() + span.offset;
            size_t off = 0;
            bool first_date = true;

            // Decode each field of the schema, then pick projected ones.
            std::vector<E2Cell> row;
            row.reserve(m_schema.size());
            bool ok = true;
            for (const auto& f : m_schema) {
                if (f.type == E2FieldType::Date && first_date && m_impl->hasDateFlag) {
                    if (off >= span.length || base[off] != 0x00) { ok = false; break; }
                    off += 1;
                }
                size_t consumed = 0;
                E2Cell cell;
                try {
                    cell = DecodeOneField(f, base + off, span.length - off, &consumed);
                } catch (const std::exception&) {
                    ok = false;
                    break;
                }
                off += consumed;
                if (f.type == E2FieldType::Date) first_date = false;
                row.push_back(std::move(cell));
            }
            if (!ok) {
                // Emit nulls for the projected columns so row counts stay
                // consistent. Matches the Rust impl's behaviour.
                for (size_t pi = 0; pi < projection.size(); ++pi)
                    columns[pi].push_back(E2Cell::MakeNull());
            } else {
                // Resolve any blob refs in projected fields.
                for (size_t pi = 0; pi < projection.size(); ++pi) {
                    size_t idx = projection[pi];
                    E2Cell& cell = row[idx];
                    if (cell.kind == E2Cell::Kind::Bytes && cell.bytes.size() == 9) {
                        uint8_t tag = cell.bytes[0];
                        const uint8_t* payload = cell.bytes.data() + 1;
                        const auto& ft = m_schema[idx].type;
                        if (tag == 0x01) {
                            // string-style ref: u32 offset + u32 length into lastBlob
                            uint32_t boff = LoadU32LE(payload);
                            uint32_t blen = LoadU32LE(payload + 4);
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
                                cell = E2Cell::MakeNull();
                            }
                        } else if (tag == 0x02 || tag == 0x03) {
                            // file-offset ref: u64 LE
                            uint64_t foff = LoadU64LE(payload);
                            auto it = m_impl->blobBlocks.find(foff);
                            if (it != m_impl->blobBlocks.end()) {
                                cell = E2Cell::MakeBytes(it->second);
                            } else {
                                cell = E2Cell::MakeNull();
                            }
                        } else {
                            cell = E2Cell::MakeNull();
                        }
                    }
                    columns[pi].push_back(std::move(cell));
                }
            }
            ++produced;
        }
    }
    m_recordCount = produced + skipped;
}

}}}  // namespace Alteryx::OpenYXDB::e2
