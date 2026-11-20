#!/bin/bash
# SCRUM-6850: checks setup.sh runs so a rebuild cannot silently lose part of the ksql pipeline.
#
# On 2026-11-10 a stage rebuild ran ~22 h and then refused promotion: manual_indexing_tag was
# empty, Debezium writes no records (and so creates no topic) for an empty table, ksqlDB never
# created the source table, and the 15 statements that depend on it failed -- including the
# private index's topic_entity_tags. The Makefile now pre-creates every source topic; these
# helpers make the rest of the rebuild account for empty tables and stop at a broken pipeline.
#
# Output goes to stdout only: the GELF driver files every stderr line as ERROR.

# Tables in table.include.list ("public.a,public.b,...") that have at least one row. Sets
# NONEMPTY_SOURCE_TABLES to the count and returns 0; returns 1 (count unset) if any table cannot
# be checked. Uses the PSQL_* environment and PGPASSWORD.
count_nonempty_source_tables() {
    local include_list=$1
    local table has tables
    local count=0
    NONEMPTY_SOURCE_TABLES=""
    IFS=',' read -ra tables <<< "${include_list}"
    for table in "${tables[@]}"; do
        table="${table//[[:space:]]/}"
        [ -n "${table}" ] || continue
        has=$(psql -h "${PSQL_HOST}" -U "${PSQL_USERNAME}" -p "${PSQL_PORT}" -d "${PSQL_DATABASE}" \
                   -Atc "SELECT EXISTS (SELECT 1 FROM ${table})" 2>/dev/null)
        case "${has}" in
            t) count=$((count + 1)) ;;
            f) echo "  ${table} is empty: Debezium creates no topic for it, so Gate 1 does not wait for one." ;;
            *) echo "  could not check whether ${table} is empty."; return 1 ;;
        esac
    done
    NONEMPTY_SOURCE_TABLES=${count}
    return 0
}

# Names of the tables and streams the statement files in $1 create, upper-cased as ksqlDB
# stores unquoted names, one per line.
ksql_created_objects() {
    local stmtdir=$1
    cat "${stmtdir}"/*.sql 2>/dev/null \
        | grep -oiE 'CREATE[[:space:]]+(OR[[:space:]]+REPLACE[[:space:]]+)?(SOURCE[[:space:]]+)?(TABLE|STREAM)[[:space:]]+[A-Za-z0-9_]+' \
        | awk '{print toupper($NF)}' | sort -u
}

# Check that every table and stream the statements in $1 create exists in ksqlDB ($2 = the
# /ksql endpoint URL). A statement can be acknowledged without its object being created (the
# manual_indexing_tag source table on 2026-11-10), so the response alone is not enough.
verify_ksql_objects() {
    local stmtdir=$1
    local url=$2
    local existing tables streams name
    local missing=0 expected=0
    tables=$(curl -sS -X POST "${url}" -H "Accept: application/vnd.ksql.v1+json" \
                  -H "Content-Type: application/json" -d '{"ksql": "SHOW TABLES;"}')
    streams=$(curl -sS -X POST "${url}" -H "Accept: application/vnd.ksql.v1+json" \
                   -H "Content-Type: application/json" -d '{"ksql": "SHOW STREAMS;"}')
    # The only "name" fields in a SHOW TABLES / SHOW STREAMS response are the object names.
    existing=$(printf '%s\n%s\n' "${tables}" "${streams}" | grep -oE '"name" *: *"[^"]*"' \
                   | sed -E 's/.*"([^"]*)"$/\1/' | tr '[:lower:]' '[:upper:]')
    if [ -z "${existing}" ]; then
        echo "ksql verification: ksqlDB listed no tables or streams (or could not be reached)."
        return 1
    fi
    for name in $(ksql_created_objects "${stmtdir}"); do
        expected=$((expected + 1))
        if ! grep -qxF "${name}" <<< "${existing}"; then
            echo "  ksql object MISSING after setup: ${name}"
            missing=$((missing + 1))
        fi
    done
    echo "ksql verification: ${expected} tables/streams expected, ${missing} missing."
    [ "${missing}" = "0" ]
}
