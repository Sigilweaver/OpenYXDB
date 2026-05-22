---
sidebar_position: 9
---

# Low-level API

The `Reader`, `Writer`, and `FieldInfo` classes give direct access to the C++
core for scenarios where the high-level functions are not sufficient.

## FieldInfo

`FieldInfo` describes a single YXDB field.

| Attribute | Type | Description |
| --- | --- | --- |
| `name` | `str` | Field name as it appears in the file metadata |
| `type` | `str` | YXDB type string (e.g. `"Int32"`, `"V_WString"`) -- see [Field types](./field-types) |
| `size` | `int` | Field size in bytes (or max characters for string types) |
| `scale` | `int` | Decimal places for `FixedDecimal`; 0 for all other types |

```python
from openyxdb import FieldInfo

fi = FieldInfo()
fi.name = "score"
fi.type = "Double"
fi.size = 8
fi.scale = 0
```

## Reader

`Reader` is a context manager that opens a YXDB file for reading.

```python
from openyxdb import Reader

with Reader("data.yxdb") as r:
    print(r.num_records)   # total record count
    print(r.fields)        # list[FieldInfo]
```

### `read_records()`

Returns all records as a list of dicts:

```python
with Reader("data.yxdb") as r:
    for record in r.read_records():
        print(record)
```

### `read_columns()`

Returns all data as a list of `(name, list)` tuples:

```python
with Reader("data.yxdb") as r:
    columns = dict(r.read_columns())
    print(columns["score"][:5])
```

### `read_columns_subset(columns, offset=0, limit=None)`

Decodes only the requested columns, starting at `offset` and reading at most
`limit` records. This is the primitive backing `scan_yxdb` pushdown:

```python
with Reader("data.yxdb") as r:
    cols = r.read_columns_subset(["id", "score"], offset=100, limit=1000)
```

## Writer

`Writer` is a context manager that opens a YXDB file for writing.

```python
from openyxdb import Writer, FieldInfo

fi_id = FieldInfo(); fi_id.name = "id"; fi_id.type = "Int32"; fi_id.size = 4; fi_id.scale = 0
fi_label = FieldInfo(); fi_label.name = "label"; fi_label.type = "V_WString"; fi_label.size = 262144; fi_label.scale = 0

with Writer("output.yxdb", [fi_id, fi_label]) as w:
    w.write_columns({"id": [1, 2, 3], "label": ["a", "b", "c"]})
```

### `write_record(record)`

Write a single record dict:

```python
with Writer("output.yxdb", schema) as w:
    w.write_record({"id": 1, "label": "a"})
    w.write_record({"id": 2, "label": "b"})
```

### `write_columns(columns)`

Write all columns at once from a `dict[str, list]`:

```python
with Writer("output.yxdb", schema) as w:
    w.write_columns({"id": [1, 2, 3], "label": ["a", "b", "c"]})
```

## Error handling

C++ errors are translated to Python `RuntimeError` with the original error
message. File-not-found and schema mismatches raise `RuntimeError` as well.

```python
try:
    with Reader("missing.yxdb") as r:
        pass
except RuntimeError as e:
    print(e)
```
