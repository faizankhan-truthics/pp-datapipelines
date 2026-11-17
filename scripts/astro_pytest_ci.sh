#!/usr/bin/env bash
# Run `astro dev parse` and `astro dev pytest` against .env.ci (dummy values), exactly like CI
# does, then put the real .env back. Used by `tox -e pytest`.
set -euo pipefail
cd "$(dirname "$0")/.."

# A symlinked .env would be overwritten through the link and not restored; refuse instead.
if [ -L .env ]; then
  echo ".env is a symlink; replace it with a regular file before running this script." >&2
  exit 1
fi

backup=""
if [ -f .env ]; then
  backup="$(mktemp)"
  cp .env "$backup"
fi
restore() {
  if [ -n "$backup" ]; then mv "$backup" .env; else rm -f .env; fi
}
trap restore EXIT

cp .env.ci .env
astro dev parse
astro dev pytest
