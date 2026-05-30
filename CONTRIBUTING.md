# Contributing

Thanks for your interest in OpenYXDB. Issues and pull requests are
welcome.

## Development setup

OpenYXDB uses [pixi](https://pixi.sh) to manage both the C++
toolchain and the Python environment.

```sh
pixi install
pixi run build       # configures + builds the C++ core
pixi run test-python # pytest suite
pixi run test-cpp    # C++ unit tests
```

## Pull request checklist

- C++ changes build cleanly under the pixi-pinned compiler.
- Python: `pytest` passes (the full corpus suite is gated by the
  `OPENYXDB_E1_CORPUS` env var; small-suite tests must still
  pass).
- New parser/writer behaviour must come with a `.yxdb` fixture
  and a round-trip test.
- User-facing changes update the README and add a `[Unreleased]`
  note to [CHANGELOG.md](CHANGELOG.md).

## DCO

By submitting a contribution you certify that you have the right
to submit the work under the project license (GPL-3.0-only,
inherited from the upstream Alteryx OpenYXDB) and agree to the
[Developer Certificate of Origin](https://developercertificate.org/).

## License

[GPL-3.0-only](LICENSE), preserving the upstream Alteryx OpenYXDB
licence.
