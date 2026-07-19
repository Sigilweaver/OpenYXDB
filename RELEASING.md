# Releasing OpenYXDB

Standard procedure for cutting an OpenYXDB release and publishing to PyPI.

## Steps

1. **Bump the version.** Update `version` in `pyproject.toml`.
2. **Update the changelog.** Move the `[Unreleased]` notes in
   [CHANGELOG.md](CHANGELOG.md) under a new `## [X.Y.Z] - YYYY-MM-DD`
   heading.
3. **Commit.** `release: vX.Y.Z` (see `git log` for the established style).
   Push the commit to `main` (directly or via PR) and let CI run on it.
4. **Confirm CI is green before tagging.** `publish.yml` triggers directly
   on `push: tags: ["v*"]` and has no way to depend on `ci.yml` passing -
   GitHub Actions can't `needs:` a job defined in another workflow file.
   So this has to be checked by hand, before the tag exists, not after:

   ```sh
   scripts/check-release-ready.sh main
   ```

   This resolves the ref to a commit and checks that the most recent
   `ci.yml` run for that commit completed successfully. It refuses (exit 1)
   if CI hasn't run yet or didn't pass. Run it against the exact commit
   you're about to tag.

5. **Tag.** Once the check passes, create an annotated tag on that commit
   and push it:

   ```sh
   git tag -a vX.Y.Z -m "vX.Y.Z"
   git push origin vX.Y.Z
   ```

   The tag push triggers `publish.yml`, which builds the sdist and wheels
   (Linux, macOS, Windows) and publishes to PyPI via trusted publishing.
6. **Verify.** Watch the run (`gh run watch` or the Actions tab) and check
   the new version shows up on [PyPI](https://pypi.org/project/openyxdb/).

## Notes

- Not every version bump has to be tagged/published immediately; a
  version can be bumped and committed to `main` without a corresponding
  tag if the release itself isn't ready to ship yet. Only a pushed `vX.Y.Z`
  tag triggers a publish.
- This repo has no `audit.yml`, so `check-release-ready.sh` only checks
  `ci.yml`.
