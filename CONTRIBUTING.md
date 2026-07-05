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

## Contributing code (pull requests)

PRs are welcome for changes of any size, including large or breaking ones -
there's no requirement to open an issue first. That said, for larger changes
you may want to open an issue before writing code, especially if you're
unsure whether it fits the project's direction: a large PR that conflicts
with the roadmap can still be rejected even if the code itself is solid, and
an issue is a cheap way to check alignment before investing the time.

For any PR:

- C++ changes build cleanly under the pixi-pinned compiler.
- Python: `pytest` passes (the full corpus suite is gated by the
  `OPENYXDB_E1_CORPUS` env var; small-suite tests must still
  pass).
- New parser/writer behaviour must come with a `.yxdb` fixture
  and a round-trip test.
- User-facing changes update the README and add a `[Unreleased]`
  note to [CHANGELOG.md](CHANGELOG.md).
- Prefer [Conventional Commits](https://www.conventionalcommits.org/)
  (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`).
- Source is ASCII-only.

## Vendor software and clean-room policy

The classic YXDB layout in this project comes from
[Alteryx's own open-source implementation](https://github.com/alteryx/OpenYXDB),
so there is no clean-room concern there - it was never reverse-engineered.

The E2 (AMP engine) columnar layout is different: it was reverse-engineered
independently, and the same clean-room rule applies to it as to any other
proprietary format in this ecosystem. Do not run, depend on, or validate E2
parser/writer changes against Alteryx Designer or any tool that reads the
format through Alteryx's own libraries - not in CI, not in tests, not in
local development. Correctness on the E2 layout is argued only from
independent analysis, roundtrip and self-consistency invariants, and the
public corpus.

**Pull requests touching the E2 layout that were written or verified with
the help of Alteryx Designer or other proprietary Alteryx tooling will not
be accepted**, regardless of code quality, since accepting them would
compromise the clean-room provenance of that part of the project. If you've
found a bug this way, or you'd simply rather not write the fix yourself,
please open an issue instead. Describe the symptom on the input that
triggers it - what's wrong, and on what file - without pasting output from
Alteryx tooling or values you learned by running it. We'll investigate and
fix it from independent analysis. Detailed issue reports are genuinely
useful and will be acted on.

## Security

Please report security vulnerabilities privately via GitHub Security
Advisories - see [SECURITY.md](SECURITY.md). Do not open public issues
for vulnerabilities.

## DCO

By submitting a contribution you certify that you have the right
to submit the work under the project license (GPL-3.0-only,
inherited from upstream Alteryx OpenYXDB) and agree to the
[Developer Certificate of Origin](https://developercertificate.org/).

## License

[GPL-3.0-only](LICENSE), preserving the upstream Alteryx OpenYXDB
licence.
