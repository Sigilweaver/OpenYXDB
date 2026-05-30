// E2 YXDB reader - the "AMP engine" variant of the YXDB binary format.
//
// E2 files differ from the original (E1) format in essentially every
// physical detail: a 100-byte header (vs 512), UTF-8 XML metadata
// (vs UTF-16), Snappy block compression (vs LZF), and a compact
// variable-length record encoding (vs fixed-width).
//
// Because of this, E2 is implemented as a parallel read-only reader
// rather than threaded into Open_AlteryxYXDB. The Python binding
// layer detects the file format from the magic string and dispatches
// to the appropriate reader.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace Alteryx { namespace OpenYXDB { namespace e2 {

// Mirrors the names exposed by FieldInfo from the Python binding.
enum class E2FieldType {
    Bool,
    Byte,
    Int16,
    Int32,
    Int64,
    FixedDecimal,
    Float,
    Double,
    String,
    WString,
    V_String,
    V_WString,
    Date,
    Time,
    DateTime,
    Blob,
    SpatialObj,
};

const char* E2FieldTypeName(E2FieldType t);
std::optional<E2FieldType> E2FieldTypeFromName(const std::string& name);

struct E2FieldMeta {
    std::string name;
    E2FieldType type;
    int size{0};
    int scale{0};
};

// Tagged-union representation of a single decoded cell.
//
// We use plain C++ variants rather than feeding values through
// FieldBase / RecordData because the existing FieldBase hierarchy is
// hard-wired to the E1 wire format and trying to round-trip E2 bytes
// through it would be more complex than worth.
struct E2Cell {
    enum class Kind {
        Null,
        Bool,
        Int64,    // covers Byte, Int16, Int32, Int64
        Double,   // covers Float, Double
        // String values: UTF-8.
        // Strings are also used for date/time and FixedDecimal,
        // matching how E1 surfaces these to Python (ISO strings).
        Text,
        Bytes,    // Blob, SpatialObj
    };
    Kind kind{Kind::Null};
    bool boolean{false};
    int64_t integer{0};
    double real{0.0};
    std::string text;
    std::vector<uint8_t> bytes;

    static E2Cell MakeNull() { return E2Cell{}; }
    static E2Cell MakeBool(bool v) { E2Cell c; c.kind = Kind::Bool; c.boolean = v; return c; }
    static E2Cell MakeInt(int64_t v) { E2Cell c; c.kind = Kind::Int64; c.integer = v; return c; }
    static E2Cell MakeDouble(double v) { E2Cell c; c.kind = Kind::Double; c.real = v; return c; }
    static E2Cell MakeText(std::string v) { E2Cell c; c.kind = Kind::Text; c.text = std::move(v); return c; }
    static E2Cell MakeBytes(std::vector<uint8_t> v) { E2Cell c; c.kind = Kind::Bytes; c.bytes = std::move(v); return c; }
};

// File-format magic, exposed for the dispatcher in the Python binding.
constexpr int E2_MAGIC_PREFIX_LEN = 24;          // "Alteryx e2 Database file"
extern const char kE2MagicString[];               // null-terminated, 24 chars

// Does the buffer (length >= E2_MAGIC_PREFIX_LEN) start with the E2 magic?
bool LooksLikeE2(const uint8_t* buf, size_t len);

class E2Reader {
public:
    // Open and parse the file header + metadata. Throws std::runtime_error
    // (with a descriptive message) on any structural problem. Block data
    // is loaded on demand by ReadAllColumns / ReadColumnsSubset.
    explicit E2Reader(const std::string& path);
    ~E2Reader();

    E2Reader(const E2Reader&) = delete;
    E2Reader& operator=(const E2Reader&) = delete;

    const std::vector<E2FieldMeta>& Schema() const { return m_schema; }
    // Total record count. Computed lazily by walking each Snappy block's
    // 4-byte record-count header (decompresses each record block exactly
    // once; the result is cached).
    int64_t NumRecords();
    const std::string& MetaXml() const { return m_metaXml; }

    // Decode every record. Output is a column-major vector of vectors:
    //   columns[col_index][record_index]
    // Order matches Schema().
    void ReadAllColumns(std::vector<std::vector<E2Cell>>& columns);

    // Decode a projected subset. `projection` indexes into Schema().
    // Returns one column per projected field, in the requested order.
    // `offset` skips records from the start; `limit < 0` means unlimited.
    void ReadColumnsSubset(const std::vector<size_t>& projection,
                           int64_t offset,
                           int64_t limit,
                           std::vector<std::vector<E2Cell>>& columns);

    // When `true`, fields whose decoder has not been validated against
    // real corpus data (Time, WString) will attempt a best-effort decode
    // instead of raising at open time. Off by default.
    void SetAllowUnverifiedTypes(bool allow) { m_allowUnverified = allow; }

private:
    struct Impl;
    std::unique_ptr<Impl> m_impl;

    std::vector<E2FieldMeta> m_schema;
    std::string m_metaXml;
    int64_t m_recordCount{0};
    bool m_allowUnverified{false};
};

}}}  // namespace Alteryx::OpenYXDB::e2
