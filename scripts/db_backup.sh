#!/usr/bin/env bash
# Persist the updated SQLite database + track pars back to the private data repo.
#
# Writes two things to the PRIVATE data repo:
#   1. db-latest   — rolling working copy, overwritten each run (used by restore).
#   2. db-backups  — dated daily snapshots for point-in-time recovery, pruned to
#                    the most recent RETAIN_BACKUPS so storage stays bounded.
#
# Requires: gh CLI authenticated via GH_TOKEN (the DB_REPO_TOKEN secret),
# scoped to the private data repo with Contents: read and write.
set -euo pipefail

REPO="${DB_REPO:-muzzy292/harness-racing-data}"
TAG="${DB_TAG:-db-latest}"
BACKUP_TAG="${DB_BACKUP_TAG:-db-backups}"
ASSET="harness-data.tar.gz"
RETAIN_BACKUPS="${RETAIN_BACKUPS:-14}"

if [ ! -f data/harness.db ]; then
  echo "No data/harness.db to back up — skipping." >&2
  exit 0
fi

echo "Packing database..."
FILES="data/harness.db"
[ -f data/track_pars.json ] && FILES="$FILES data/track_pars.json"
tar -czf "$ASSET" $FILES

# 1) Rolling working copy (restore reads this).
gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1 \
  || gh release create "$TAG" --repo "$REPO" --title "Latest database" \
       --notes "Rolling working copy — overwritten on every daily run."
gh release upload "$TAG" "$ASSET" --repo "$REPO" --clobber
echo "Uploaded working copy to ${TAG}."

# 2) Dated snapshot for recovery.
DATED="harness-data-$(date -u +%Y%m%d).tar.gz"
cp "$ASSET" "$DATED"
gh release view "$BACKUP_TAG" --repo "$REPO" >/dev/null 2>&1 \
  || gh release create "$BACKUP_TAG" --repo "$REPO" --title "Database backups" \
       --notes "Dated daily snapshots (rolling retention)."
gh release upload "$BACKUP_TAG" "$DATED" --repo "$REPO" --clobber
echo "Uploaded dated backup ${DATED}."

# 3) Prune old dated snapshots, keeping the most recent RETAIN_BACKUPS.
mapfile -t OLD < <(
  gh release view "$BACKUP_TAG" --repo "$REPO" --json assets \
    --jq '.assets | map(.name) | sort | reverse | .['"$RETAIN_BACKUPS"':] | .[]' 2>/dev/null || true
)
for name in "${OLD[@]:-}"; do
  [ -z "$name" ] && continue
  echo "Pruning old backup ${name}..."
  gh release delete-asset "$BACKUP_TAG" "$name" --repo "$REPO" --yes || true
done

rm -f "$ASSET" "$DATED"
echo "Backup complete."
