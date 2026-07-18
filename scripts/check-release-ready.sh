#!/usr/bin/env bash
# Refuse to say a commit is release-ready unless the most recent ci.yml run
# for that commit completed successfully.
#
# publish.yml triggers directly on `push: tags: ["v*"]` and GitHub Actions
# has no way for one workflow file to `needs:` a job defined in a separate
# workflow file, so this check has to run before the tag is created (see
# RELEASING.md and https://github.com/Sigilweaver/OpenYXDB/issues/1).
#
# This repo has no audit.yml, so only ci.yml is checked.
#
# Usage: scripts/check-release-ready.sh [ref]
#   ref defaults to HEAD.

set -euo pipefail

ref="${1:-HEAD}"
# `^{commit}` peels annotated tags to the commit they point at; git rev-parse
# on a bare tag name otherwise returns the tag object's own SHA, which never
# has a CI run against it.
sha="$(git rev-parse "${ref}^{commit}")"

echo "Checking release readiness for $ref ($sha)..."

check_workflow() {
  local workflow="$1"
  local runs
  runs="$(gh run list -w "$workflow" -c "$sha" --json status,conclusion,url -L 1)"

  if [[ "$(echo "$runs" | jq 'length')" -eq 0 ]]; then
    echo "FAIL: no run of $workflow found for commit $sha" >&2
    return 1
  fi

  local status conclusion url
  status="$(echo "$runs" | jq -r '.[0].status')"
  conclusion="$(echo "$runs" | jq -r '.[0].conclusion')"
  url="$(echo "$runs" | jq -r '.[0].url')"

  if [[ "$status" != "completed" ]]; then
    echo "FAIL: latest $workflow run for $sha has not completed (status=$status) - $url" >&2
    return 1
  fi

  if [[ "$conclusion" != "success" ]]; then
    echo "FAIL: latest $workflow run for $sha did not succeed (conclusion=$conclusion) - $url" >&2
    return 1
  fi

  echo "OK: $workflow succeeded for $sha - $url"
  return 0
}

if check_workflow "ci.yml"; then
  echo "Release ready: ci.yml is green for $sha"
  exit 0
fi

exit 1
