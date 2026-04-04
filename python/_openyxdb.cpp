#include "SrcLib_Replacement.h"

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/optional.h>

#include "Open_AlteryxYXDB.h"
#include "FieldType.h"
#include "RecordLib/FieldBase.h"
#include "RecordLib/RecordInfo.h"

namespace nb = nanobind;
using namespace nb::literals;
using namespace Alteryx::OpenYXDB;
using namespace SRC;

// Convert U16unit (char16_t) string to std::string (UTF-8)
static std::string u16_to_utf8(const U16unit* s, size_t len) {
    std::string result;
    result.reserve(len);
    for (size_t i = 0; i < len; ++i) {
        uint32_t c = static_cast<uint16_t>(s[i]);
        // Handle UTF-16 surrogate pairs
        if (c >= 0xD800 && c <= 0xDBFF && i + 1 < len) {
            uint16_t trail = static_cast<uint16_t>(s[i + 1]);
            if (trail >= 0xDC00 && trail <= 0xDFFF) {
                c = 0x10000 + ((c - 0xD800) << 10) + (trail - 0xDC00);
                ++i; // consume trail surrogate
            }
        }
        if (c < 0x80) {
            result.push_back(static_cast<char>(c));
        } else if (c < 0x800) {
            result.push_back(static_cast<char>(0xC0 | (c >> 6)));
            result.push_back(static_cast<char>(0x80 | (c & 0x3F)));
        } else if (c < 0x10000) {
            // Lone surrogates -> U+FFFD replacement character
            if (c >= 0xD800 && c <= 0xDFFF) {
                result.push_back(static_cast<char>(0xEF));
                result.push_back(static_cast<char>(0xBF));
                result.push_back(static_cast<char>(0xBD));
            } else {
                result.push_back(static_cast<char>(0xE0 | (c >> 12)));
                result.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
                result.push_back(static_cast<char>(0x80 | (c & 0x3F)));
            }
        } else {
            result.push_back(static_cast<char>(0xF0 | (c >> 18)));
            result.push_back(static_cast<char>(0x80 | ((c >> 12) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (c & 0x3F)));
        }
    }
    return result;
}

// Convert WString to std::string (UTF-8)
static std::string wstring_to_utf8(const WString& ws) {
    const U16unit* p = ws.c_str();
    size_t len = 0;
    while (p[len] != 0) ++len;
    return u16_to_utf8(p, len);
}

// Convert std::string (UTF-8) to WString (UTF-16)
static WString utf8_to_wstring(const std::string& s) {
    std::vector<U16unit> buf;
    buf.reserve(s.size());
    size_t i = 0;
    while (i < s.size()) {
        uint32_t cp;
        unsigned char c = static_cast<unsigned char>(s[i]);
        if (c < 0x80) {
            cp = c;
            i += 1;
        } else if ((c & 0xE0) == 0xC0) {
            cp = (c & 0x1F) << 6;
            if (i + 1 < s.size()) cp |= (static_cast<unsigned char>(s[i+1]) & 0x3F);
            i += 2;
        } else if ((c & 0xF0) == 0xE0) {
            cp = (c & 0x0F) << 12;
            if (i + 1 < s.size()) cp |= (static_cast<unsigned char>(s[i+1]) & 0x3F) << 6;
            if (i + 2 < s.size()) cp |= (static_cast<unsigned char>(s[i+2]) & 0x3F);
            i += 3;
        } else {
            // 4-byte UTF-8 -> surrogate pair (BMP only for now)
            cp = '?';
            i += 4;
        }
        buf.push_back(static_cast<U16unit>(cp));
    }
    buf.push_back(0);
    return WString(buf.data());
}

// Field metadata exposed to Python
struct FieldInfo {
    std::string name;
    std::string type;
    int size;
    int scale;
};

static std::string field_type_name(E_FieldType ft) {
    switch (ft) {
        case E_FT_Bool:         return "Bool";
        case E_FT_Byte:         return "Byte";
        case E_FT_Int16:        return "Int16";
        case E_FT_Int32:        return "Int32";
        case E_FT_Int64:        return "Int64";
        case E_FT_FixedDecimal: return "FixedDecimal";
        case E_FT_Float:        return "Float";
        case E_FT_Double:       return "Double";
        case E_FT_String:       return "String";
        case E_FT_WString:      return "WString";
        case E_FT_V_String:     return "V_String";
        case E_FT_V_WString:    return "V_WString";
        case E_FT_Date:         return "Date";
        case E_FT_Time:         return "Time";
        case E_FT_DateTime:     return "DateTime";
        case E_FT_Blob:         return "Blob";
        case E_FT_SpatialObj:   return "SpatialObj";
        default:                return "Unknown";
    }
}

static E_FieldType field_type_from_name(const std::string& name) {
    if (name == "Bool")         return E_FT_Bool;
    if (name == "Byte")         return E_FT_Byte;
    if (name == "Int16")        return E_FT_Int16;
    if (name == "Int32")        return E_FT_Int32;
    if (name == "Int64")        return E_FT_Int64;
    if (name == "FixedDecimal") return E_FT_FixedDecimal;
    if (name == "Float")        return E_FT_Float;
    if (name == "Double")       return E_FT_Double;
    if (name == "String")       return E_FT_String;
    if (name == "WString")      return E_FT_WString;
    if (name == "V_String")     return E_FT_V_String;
    if (name == "V_WString")    return E_FT_V_WString;
    if (name == "Date")         return E_FT_Date;
    if (name == "Time")         return E_FT_Time;
    if (name == "DateTime")     return E_FT_DateTime;
    if (name == "Blob")         return E_FT_Blob;
    if (name == "SpatialObj")   return E_FT_SpatialObj;
    throw std::invalid_argument("Unknown field type: " + name);
}

// Extract a field value as a Python object
static nb::object field_to_python(const FieldBase* field, const RecordData* rec) {
    if (field->GetNull(rec))
        return nb::none();

    switch (field->m_ft) {
        case E_FT_Bool: {
            auto val = field->GetAsBool(rec);
            return nb::cast(val.value);
        }
        case E_FT_Byte:
        case E_FT_Int16:
        case E_FT_Int32: {
            auto val = field->GetAsInt32(rec);
            return nb::cast(val.value);
        }
        case E_FT_Int64: {
            auto val = field->GetAsInt64(rec);
            return nb::cast(val.value);
        }
        case E_FT_Float:
        case E_FT_Double:
        case E_FT_FixedDecimal: {
            auto val = field->GetAsDouble(rec);
            return nb::cast(val.value);
        }
        case E_FT_String:
        case E_FT_V_String:
        case E_FT_Date:
        case E_FT_Time:
        case E_FT_DateTime: {
            auto val = field->GetAsAString(rec);
            return nb::cast(std::string(val.value.pValue, val.value.nLength));
        }
        case E_FT_WString:
        case E_FT_V_WString: {
            auto val = field->GetAsWString(rec);
            return nb::cast(u16_to_utf8(val.value.pValue, val.value.nLength));
        }
        case E_FT_Blob:
        case E_FT_SpatialObj: {
            auto val = field->GetAsBlob(rec);
            return nb::bytes(reinterpret_cast<const char*>(val.value.pValue), val.value.nLength);
        }
        default:
            return nb::none();
    }
}

// Python-facing Reader class
class YXDBReader {
    Open_AlteryxYXDB m_db;
    std::vector<const FieldBase*> m_fields;
    std::vector<FieldInfo> m_schema;
    bool m_open = false;

public:
    YXDBReader(const std::string& path) {
        m_db.Open(utf8_to_wstring(path));
        m_open = true;

        unsigned nFields = m_db.m_recordInfo.NumFields();
        m_fields.reserve(nFields);
        m_schema.reserve(nFields);
        for (unsigned i = 0; i < nFields; ++i) {
            const FieldBase* f = m_db.m_recordInfo[i];
            m_fields.push_back(f);
            FieldInfo info;
            info.name = wstring_to_utf8(WString(f->GetFieldName().c_str()));
            info.type = field_type_name(f->m_ft);
            info.size = f->m_nSize;
            info.scale = f->m_nScale;
            m_schema.push_back(info);
        }
    }

    ~YXDBReader() { close(); }

    void close() {
        if (m_open) {
            m_db.Close();
            m_open = false;
        }
    }

    int64_t num_records() { return m_db.GetNumRecords(); }

    const std::vector<FieldInfo>& schema() const { return m_schema; }

    // Read all records as a list of dicts (column-name -> value)
    nb::list read_records() {
        nb::list rows;
        m_db.GoRecord(0);
        int64_t n = m_db.GetNumRecords();
        for (int64_t i = 0; i < n; ++i) {
            const RecordData* rec = m_db.ReadRecord();
            if (!rec) break;
            nb::dict row;
            for (size_t f = 0; f < m_fields.size(); ++f) {
                row[nb::cast(m_schema[f].name)] = field_to_python(m_fields[f], rec);
            }
            rows.append(row);
        }
        return rows;
    }

    // Read all records in columnar format: dict of column-name -> list of values
    nb::dict read_columns() {
        int64_t n = m_db.GetNumRecords();
        size_t nFields = m_fields.size();

        // Pre-allocate column lists
        std::vector<nb::list> columns(nFields);

        m_db.GoRecord(0);
        for (int64_t i = 0; i < n; ++i) {
            const RecordData* rec = m_db.ReadRecord();
            if (!rec) break;
            for (size_t f = 0; f < nFields; ++f) {
                columns[f].append(field_to_python(m_fields[f], rec));
            }
        }

        nb::dict result;
        for (size_t f = 0; f < nFields; ++f) {
            result[nb::cast(m_schema[f].name)] = columns[f];
        }
        return result;
    }

    // Context manager support
    YXDBReader& enter() { return *this; }
};

// Python-facing Writer class
class YXDBWriter {
    Open_AlteryxYXDB m_db;
    RecordInfo m_recordInfo;
    SmartPointerRefObj<Record> m_record;
    std::vector<const FieldBase*> m_fields;
    std::vector<std::string> m_fieldNames;
    bool m_open = false;

    void build_schema_xml(const std::vector<FieldInfo>& schema, WString& xml) {
        std::string s = "<RecordInfo>";
        for (const auto& fi : schema) {
            s += "<Field name=\"";
            s += fi.name;
            s += "\" type=\"";
            s += fi.type;
            s += "\"";
            if (fi.size > 0) {
                s += " size=\"";
                s += std::to_string(fi.size);
                s += "\"";
            }
            if (fi.scale > 0) {
                s += " scale=\"";
                s += std::to_string(fi.scale);
                s += "\"";
            }
            s += " />";
        }
        s += "</RecordInfo>";
        xml = utf8_to_wstring(s);
    }

public:
    YXDBWriter(const std::string& path, const std::vector<FieldInfo>& schema) {
        WString xml;
        build_schema_xml(schema, xml);
        m_db.Create(utf8_to_wstring(path), xml.c_str());
        m_open = true;

        unsigned nFields = m_db.m_recordInfo.NumFields();
        m_fields.reserve(nFields);
        m_fieldNames.reserve(nFields);
        for (unsigned i = 0; i < nFields; ++i) {
            const FieldBase* f = m_db.m_recordInfo[i];
            m_fields.push_back(f);
            m_fieldNames.push_back(wstring_to_utf8(WString(f->GetFieldName().c_str())));
        }
        m_record = m_db.m_recordInfo.CreateRecord();
    }

    ~YXDBWriter() { close(); }

    void close() {
        if (m_open) {
            m_db.Close();
            m_open = false;
        }
    }

    void write_record(const nb::dict& row) {
        Record* rec = m_record.Get();
        rec->Reset();
        for (size_t i = 0; i < m_fields.size(); ++i) {
            const FieldBase* field = m_fields[i];
            nb::str key(m_fieldNames[i].c_str(), m_fieldNames[i].size());
            if (!row.contains(key)) {
                field->SetNull(rec);
                continue;
            }
            nb::object val = row[key];
            if (val.is_none()) {
                field->SetNull(rec);
                continue;
            }
            set_field_from_python(field, rec, val);
        }
        m_db.AppendRecord(rec->GetRecord());
    }

    void write_columns(const nb::dict& columns) {
        // Determine row count from first column
        if (m_fields.empty()) return;

        nb::str first_key(m_fieldNames[0].c_str(), m_fieldNames[0].size());
        if (!columns.contains(first_key)) return;
        nb::list first_col = nb::cast<nb::list>(columns[first_key]);
        size_t n = nb::len(first_col);

        Record* rec = m_record.Get();
        for (size_t row = 0; row < n; ++row) {
            rec->Reset();
            for (size_t f = 0; f < m_fields.size(); ++f) {
                const FieldBase* field = m_fields[f];
                nb::str key(m_fieldNames[f].c_str(), m_fieldNames[f].size());
                if (!columns.contains(key)) {
                    field->SetNull(rec);
                    continue;
                }
                nb::list col = nb::cast<nb::list>(columns[key]);
                nb::object val = col[row];
                if (val.is_none()) {
                    field->SetNull(rec);
                    continue;
                }
                set_field_from_python(field, rec, val);
            }
            m_db.AppendRecord(rec->GetRecord());
        }
    }

    // Context manager support
    YXDBWriter& enter() { return *this; }

private:
    static void set_field_from_python(const FieldBase* field, Record* rec, nb::handle val) {
        switch (field->m_ft) {
            case E_FT_Bool:
                field->SetFromBool(rec, nb::cast<bool>(val));
                break;
            case E_FT_Byte:
            case E_FT_Int16:
            case E_FT_Int32:
                field->SetFromInt32(rec, nb::cast<int>(val));
                break;
            case E_FT_Int64:
                field->SetFromInt64(rec, nb::cast<int64_t>(val));
                break;
            case E_FT_Float:
            case E_FT_Double:
            case E_FT_FixedDecimal:
                field->SetFromDouble(rec, nb::cast<double>(val));
                break;
            case E_FT_String:
            case E_FT_V_String:
            case E_FT_Date:
            case E_FT_Time:
            case E_FT_DateTime: {
                std::string s = nb::cast<std::string>(val);
                field->SetFromString(rec, s.c_str(), s.size());
                break;
            }
            case E_FT_WString:
            case E_FT_V_WString: {
                std::string s = nb::cast<std::string>(val);
                WString ws = utf8_to_wstring(s);
                const U16unit* p = ws.c_str();
                size_t len = 0;
                while (p[len] != 0) ++len;
                field->SetFromString(rec, p, len);
                break;
            }
            case E_FT_Blob:
            case E_FT_SpatialObj: {
                auto buf = nb::cast<nb::bytes>(val);
                BlobVal bv(static_cast<unsigned>(nb::len(buf)),
                           reinterpret_cast<const void*>(buf.c_str()));
                field->SetFromBlob(rec, bv);
                break;
            }
            default:
                break;
        }
    }
};

NB_MODULE(_openyxdb, m) {
    m.doc() = "Low-level Python bindings for OpenYXDB";

    // Register the C++ Error exception so it translates to Python RuntimeError
    nb::register_exception_translator([](const std::exception_ptr& p, void*) {
        try {
            std::rethrow_exception(p);
        } catch (const SRC::Error& e) {
            std::string msg = wstring_to_utf8(e.GetErrorDescription());
            PyErr_SetString(PyExc_RuntimeError, msg.c_str());
        }
    });

    nb::class_<FieldInfo>(m, "FieldInfo")
        .def(nb::init<>())
        .def_rw("name", &FieldInfo::name)
        .def_rw("type", &FieldInfo::type)
        .def_rw("size", &FieldInfo::size)
        .def_rw("scale", &FieldInfo::scale)
        .def("__repr__", [](const FieldInfo& fi) {
            return "FieldInfo(name='" + fi.name + "', type='" + fi.type
                   + "', size=" + std::to_string(fi.size)
                   + ", scale=" + std::to_string(fi.scale) + ")";
        });

    nb::class_<YXDBReader>(m, "Reader")
        .def(nb::init<const std::string&>(), "path"_a)
        .def("close", &YXDBReader::close)
        .def_prop_ro("num_records", &YXDBReader::num_records)
        .def_prop_ro("schema", &YXDBReader::schema)
        .def("read_records", &YXDBReader::read_records,
             "Read all records as a list of dicts")
        .def("read_columns", &YXDBReader::read_columns,
             "Read all records in columnar format (dict of column name to list)")
        .def("__enter__", &YXDBReader::enter, nb::rv_policy::reference)
        .def("__exit__", [](YXDBReader& self, nb::args) { self.close(); });

    nb::class_<YXDBWriter>(m, "Writer")
        .def(nb::init<const std::string&, const std::vector<FieldInfo>&>(),
             "path"_a, "schema"_a)
        .def("close", &YXDBWriter::close)
        .def("write_record", &YXDBWriter::write_record, "row"_a,
             "Write a single record from a dict")
        .def("write_columns", &YXDBWriter::write_columns, "columns"_a,
             "Write records from columnar data (dict of column name to list)")
        .def("__enter__", &YXDBWriter::enter, nb::rv_policy::reference)
        .def("__exit__", [](YXDBWriter& self, nb::args) { self.close(); });
}
