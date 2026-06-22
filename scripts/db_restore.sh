#!/usr/bin/env bash
# Restore the SQLite database + track pars from the private data repo.
#
# The database is NOT stored in this public repo. It lives as a release asset
# in a separate PRIVATE repo so it is never publicly downloadable. This script
# pulls the latest copy at the start of each daily run.
#
# Requires: gh CLI authenticated via GH_TOKEN (the DB_REPO_TOKEN secret),
# scoped to the private data repo with Contents: read.
set -euo pipefail

REPO="${DB_REPO:-muzzy292/harness-racing-data}"
TAG="${DB_TAG:-db-latest}"
ASSET="harness-data.tar.gz"

mkdir -p data
echo "Restoring database from ${REPO} (${TAG}/${ASSET})..."

if ! gh release download "$TAG" --repo "$REPO" --pattern "$ASSET" --output "$ASSET" --clobber; then
  echo "ERROR: could not download ${ASSET} from ${REPO} (${TAG})." >&2
  echo "Has the database been seeded into the private repo yet?" >&2
  echo "See scripts/SEEDING.md for the one-time setup." >&2
  exit 1
fi

# Tarball contains data/harness.db and data/track_pars.json (repo-relative paths).
tar -xzf "$ASSET"
rm -f "$ASSET"

if [ ! -f data/harness.db ]; then
  echo "ERROR: restore completed but data/harness.db is missing." >&2
  exit 1
fi
echo "Database restored:"
ls -la data/harness.db data/track_pars.json 2>/dev/null || true
