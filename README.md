# Alteryx OpenYXDB C++ library

The [Alteryx](https://www.alteryx.com/) OpenYXDB provides a set of mechanisms to read/write of [YXDB](https://help.alteryx.com/20214/designer/alteryx-database-file-format) file format.

## Requirements

* reqires compiler with a support of C++17
* reqires CMake 3.16 or later

## Building

The library was tested under MSVC19, GCC9, MingGW.

## Examples

The only documentation for now may be found directly in code under test folder.

## YXDB Limitations

* YXDB files have an optional spatial index. This code does not support that. When reading, if the source file has a spatial index, it will skip over it and read properly, but it will not utilize the index. Writing will not attempt to create one.
* YXDBs support spatial objects, but this code doesn’t help you much with them. Alteryx stores spatial objects internally as blobs in the SHP format. If you know how to deal with that, you can get and set spatial objects.

