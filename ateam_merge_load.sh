#!/bin/bash
# Build the merged ABC + A-team test database (SCRUM-6720), from Shuai's ~shuai/merge_load.sh.
#
# Usage (on the dev box, as a member of the docker group):
#   ./ateam_merge_load.sh <ateam_dump> <abc_dump> [database]
#
#   ateam_dump  pg_dump -Fc of the A-team curation database
#   abc_dump    pg_dump -Fc of ABC with the A-team conventions (SCRUM-6719), e.g. a fresh stage
#               dump; a pre-SCRUM-6719 dump (any prod dump before that deploy) does not match the
#               ateam_merge revisions
#   database    default literature_ateam_merge_test (DROPPED and recreated)
#
# Steps: recreate the database; restore the A-team dump; rename A-team's person and reference
# (they collide with ABC's) to ateam_person and ateam_reference, with reference_pkey to
# ateam_reference_pkey (A-team's person key is person_pkey1 and does not collide); restore the
# ABC dump; check the result. The A-team foreign keys follow the renames by themselves.
#
# The restores run inside the PostgreSQL 18.4 container with its own pg_restore (the host's is
# 17), from a copy of each dump in the container's /tmp that is removed afterwards. Then, from an
# ateam_merge checkout:  alembic -x db_url=postgresql://postgres@localhost:5432/<database> upgrade head
#
# First run 2026-10-21 (A-team dump 2026-10-13_21-00-27, stage dump 2026-10-21 19:00 UTC): 41 min,
# no restore errors; 290 tables, alembic 80595e1e0d4d, 131 A-team FKs to ateam_person, 12 to
# ateam_reference.
set -euo pipefail

CONTAINER=${CONTAINER:-agr-literature-main-postgres-1}
JOBS=${JOBS:-6}

[ $# -ge 2 ] || { sed -n '4,12p' "$0"; exit 1; }
ATEAM_DUMP=$1
ABC_DUMP=$2
DB=${3:-literature_ateam_merge_test}
for f in "$ATEAM_DUMP" "$ABC_DUMP"; do
    [ -r "$f" ] || { echo "ERROR: cannot read $f"; exit 1; }
done

psql_c() { docker exec -i "$CONTAINER" psql -U postgres -v ON_ERROR_STOP=1 "$@"; }
restore() {
    local dump=$1 name
    name=/tmp/$(basename "$dump")
    echo "== $(date -u) copy $dump into $CONTAINER"
    docker cp "$dump" "$CONTAINER:$name"
    echo "== $(date -u) pg_restore $name"
    # --no-owner/--no-privileges: owners and grants of the source databases do not exist here.
    # pg_restore exits non-zero on any error; keep going so the copy is removed, then report.
    local rc=0
    docker exec "$CONTAINER" pg_restore -U postgres -d "$DB" -j "$JOBS" --no-owner --no-privileges "$name" || rc=$?
    docker exec "$CONTAINER" rm -f "$name"
    echo "== $(date -u) pg_restore $(basename "$dump") exit=$rc"
    return $rc
}

echo "== $(date -u) recreate $DB in $CONTAINER"
psql_c -d postgres -c "DROP DATABASE IF EXISTS \"$DB\" WITH (FORCE)"
psql_c -d postgres -c "CREATE DATABASE \"$DB\""

restore "$ATEAM_DUMP"

echo "== $(date -u) rename A-team person/reference"
psql_c -d "$DB" <<'SQL'
ALTER TABLE person RENAME TO ateam_person;
ALTER TABLE reference RENAME TO ateam_reference;
ALTER TABLE ateam_reference RENAME CONSTRAINT reference_pkey TO ateam_reference_pkey;
SQL

restore "$ABC_DUMP"

echo "== $(date -u) checks"
psql_c -d "$DB" -At <<'SQL'
SELECT 'tables: ' || count(*) FROM pg_tables WHERE schemaname = 'public';
SELECT 'alembic_version: ' || string_agg(version_num, ',') FROM alembic_version;
SELECT 'A-team FKs to ateam_person: ' || count(*) FROM pg_constraint WHERE contype = 'f' AND confrelid = 'ateam_person'::regclass;
SELECT 'A-team FKs to ateam_reference: ' || count(*) FROM pg_constraint WHERE contype = 'f' AND confrelid = 'ateam_reference'::regclass;
SELECT 'ABC FKs to reference: ' || count(*) FROM pg_constraint WHERE contype = 'f' AND confrelid = 'reference'::regclass;
SELECT 'ontologyterm rows: ' || count(*) FROM ontologyterm;
SELECT 'ABC reference rows: ' || count(*) FROM reference;
SELECT 'invalid indexes: ' || count(*) FROM pg_index WHERE NOT indisvalid;
SQL
echo "== $(date -u) done: $DB"
