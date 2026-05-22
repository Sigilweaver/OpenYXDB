---
sidebar_position: 2
---

# Install

OpenYXDB ships as a Python wheel with an embedded C++ core.

## Python

Install from PyPI:

```sh
pip install openyxdb
```

Pre-built wheels target Python 3.10+ on Linux x64, macOS Intel/ARM, and
Windows x64.

### Optional extras

Install extras to pull in additional integrations:

```sh
# Pandas support
pip install openyxdb[pandas]

# Polars support
pip install openyxdb[polars]

# DuckDB support
pip install openyxdb[duckdb]

# Everything
pip install openyxdb[all]
```

PyArrow is a required runtime dependency -- all read and write functions use it
for type mapping. It is always installed with the base package.

## Building from source

Building from source requires a C++ compiler, CMake 3.20+, and a Python 3.10+
environment.

### With pixi (recommended)

```sh
git clone https://github.com/Sigilweaver/OpenYXDB
cd OpenYXDB
pixi install
pixi run build
```

### With pip

```sh
git clone https://github.com/Sigilweaver/OpenYXDB
cd OpenYXDB
pip install -e .
```

This uses scikit-build-core to drive CMake. Ninja is used as the generator
when available.

## Verifying the install

```python
import openyxdb
print(openyxdb.__version__)
```

Or from the command line:

```sh
python -m openyxdb --version
```

Run the built-in help to see quick-start examples:

```sh
python -m openyxdb
```
