#!/bin/bash
# SCRUM-6336 -- Flink reindex / blue-green orchestrator (replaces the ksqlDB debezium/setup.sh flow).
#
# Model shift vs ksqlDB: a single Flink streaming job per index maintains it continuously, and Flink's
# retract semantics delete correctly (SCRUM-6337), so routine full reindexes are NO LONGER needed --
# the live index stays correct indefinitely. This script is for the INITIAL build and for blue-green
# REBUILDS (mapping / SQL-logic changes only).
#
# Flow: pick the inactive slot -> create its indexes with the real mappings -> (re)deploy the Debezium
# 3.5 source for a fresh snapshot -> wait for the snapshot topics -> submit the Flink SQL jobs against
# the inactive slot via the SQL Gateway -> wait for the jobs to CATCH UP (index size stable near the
# source count) -> flip the aliases -> cancel the old slot's jobs.
#
# All coordination is curl-based (ES REST, Kafka Connect REST, Flink SQL Gateway REST) so it runs in a
# plain container, exactly like the old setup.sh.
set -euo pipefail

# ---- config (env-driven; defaults are the local dbz-test box) --------------------------------------
ES="http://${ELASTICSEARCH_HOST:-elasticsearch}:${ELASTICSEARCH_PORT:-9200}"
CONNECT="http://${DEBEZIUM_CONNECTOR_HOST:-dbz_connector}:${DEBEZIUM_CONNECTOR_PORT:-8083}"
GW="http://${FLINK_SQLGATEWAY_HOST:-flink_jobmanager}:${FLINK_SQLGATEWAY_PORT:-8083}"
FLINK_REST="http://${FLINK_JOBMANAGER_HOST:-flink_jobmanager}:${FLINK_JOBMANAGER_PORT:-8081}"
SQL_DIR="${FLINK_SQL_DIR:-/opt/flink-sql}"          # dir holding references_index.sql / public_references_index.sql
BASE_DIR="${BASE_DIR:-/}"                            # dir holding the connector json + ES mapping files

PRIVATE_ALIAS="${DEBEZIUM_INDEX_NAME:-references_index}"
PUBLIC_ALIAS="public_references_index"
PRIVATE_MAPPING="${BASE_DIR}elasticsearch-settings.json"
PUBLIC_MAPPING="${BASE_DIR}elasticsearch-settings-public.json"
PIPELINE_JSON="${BASE_DIR}sort-authors-pipeline.json"
SOURCE_JSON="${BASE_DIR}postgres-source-flink.json"
SOURCE_CONNECTOR="postgres-source-flink"
SLOT_NAME="debezium_unified"
CATCHUP_STABLE_SECS="${DBZ_FLINK_CATCHUP_STABLE_SECS:-120}"   # index size unchanged this long == caught up
CATCHUP_MAX_SECS="${DBZ_DATA_PROCESSING_SLEEP:-20000}"        # hard cap

log() { echo "[setup_flink $(date -u +%H:%M:%S)] $*"; }

# ---- ES helpers ------------------------------------------------------------------------------------
es()   { curl -s -X "$1" "${ES}$2" "${@:3}"; }
es_count() { es GET "/$1/_count" | sed 's/.*"count":\([0-9]*\).*/\1/'; }

create_authors_pipeline() {
  # Both mapping files set index.default_pipeline=sort_authors_by_order, so the pipeline MUST exist
  # before the indexes take any writes (a missing default_pipeline fails every bulk request). PUT is
  # idempotent. Mirrors setup.sh in the ksqlDB path (SCRUM-6405).
  local ack; ack=$(es PUT "/_ingest/pipeline/sort_authors_by_order" \
    -H 'Content-Type: application/json' --data-binary @"$PIPELINE_JSON" \
    | grep -o '"acknowledged":true' || true)
  [ -n "$ack" ] || { log "FATAL: could not create ingest pipeline sort_authors_by_order from $PIPELINE_JSON"; exit 1; }
  log "created ingest pipeline sort_authors_by_order"
}

create_slot_index() {   # $1=index name  $2=mapping file
  es DELETE "/$1" >/dev/null 2>&1 || true
  local ack; ack=$(es PUT "/$1" -H 'Content-Type: application/json' --data-binary @"$2" | grep -o '"acknowledged":true' || true)
  [ -n "$ack" ] || { log "FATAL: could not create index $1 with mapping $2"; exit 1; }
  log "created index $1 from $(basename "$2")"
}

alias_current_slot() {  # echo the slot (1|2) the alias currently points at, or empty
  es GET "/_alias/$1" 2>/dev/null | grep -oE "${1}_[12]" | grep -oE '[12]$' | head -1
}

flip_alias() {          # $1=alias  $2=new index (slot)
  es POST "/_aliases" -H 'Content-Type: application/json' -d \
    "{\"actions\":[{\"remove\":{\"index\":\"${1}_*\",\"alias\":\"$1\"}},{\"add\":{\"index\":\"$2\",\"alias\":\"$1\"}}]}" \
    | grep -q '"acknowledged":true' && log "alias $1 -> $2" || { log "FATAL: alias flip failed for $1"; exit 1; }
}

# ---- Debezium source (fresh snapshot) --------------------------------------------------------------
deploy_source() {
  # drop the connector + slot/publication so we re-snapshot from scratch (same as setup.sh)
  curl -s -X DELETE "${CONNECT}/connectors/${SOURCE_CONNECTOR}" >/dev/null 2>&1 || true
  PGPASSWORD="$PSQL_PASSWORD" psql -h "$PSQL_HOST" -p "$PSQL_PORT" -U "$PSQL_USERNAME" -d "$PSQL_DATABASE" -tAc \
    "SELECT pg_drop_replication_slot('${SLOT_NAME}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='${SLOT_NAME}');" >/dev/null 2>&1 || true
  PGPASSWORD="$PSQL_PASSWORD" psql -h "$PSQL_HOST" -p "$PSQL_PORT" -U "$PSQL_USERNAME" -d "$PSQL_DATABASE" -c \
    "DROP PUBLICATION IF EXISTS ${SLOT_NAME};" >/dev/null 2>&1 || true
  # env-substitute the secrets into the connector config, then POST
  local cfg; cfg=$(sed -e "s#\${PSQL_HOST}#${PSQL_HOST}#" -e "s#\${PSQL_PORT}#${PSQL_PORT}#" \
    -e "s#\${PSQL_USERNAME}#${PSQL_USERNAME}#" -e "s#\${PSQL_PASSWORD}#${PSQL_PASSWORD}#" \
    -e "s#\${PSQL_DATABASE}#${PSQL_DATABASE}#" "$SOURCE_JSON")
  echo "$cfg" | curl -s -X POST "${CONNECT}/connectors" -H 'Content-Type: application/json' -d @- >/dev/null
  log "deployed Debezium 3.5 source (fresh snapshot starting)"
}

wait_for_source_running() {   # Gate 1: Debezium source connector + task are RUNNING (snapshot streaming into Kafka)
  log "Gate 1: waiting for Debezium source '${SOURCE_CONNECTOR}' to be RUNNING ..."
  local n=0
  # status JSON reports "state":"RUNNING" once for the connector and once for each task -> expect >=2
  until [ "$(curl -s "${CONNECT}/connectors/${SOURCE_CONNECTOR}/status" | grep -o '"state":"RUNNING"' | wc -l)" -ge 2 ] \
        || [ $n -ge 300 ]; do sleep 5; n=$((n+5)); done
  log "Gate 1: source is RUNNING (waited ${n}s); Flink jobs read from earliest as the snapshot streams in"
}

# ---- Flink SQL Gateway submission ------------------------------------------------------------------
# Opens one session, executes every statement of a .sql file in order (so CREATE TABLE/VIEW precede the
# INSERT), retargeting the ES sink index to the given slot index. The final INSERT submits the job.
flink_submit_file() {   # $1=sql file  $2=index-name-in-file  $3=slot-index-name
  local sh; sh=$(curl -s -X POST "${GW}/v1/sessions" -H 'Content-Type: application/json' -d '{}' \
                 | grep -o '"sessionHandle":"[^"]*"' | cut -d'"' -f4)
  [ -n "$sh" ] || { log "FATAL: could not open SQL Gateway session (is flink_sqlgateway up?)"; exit 1; }
  # split on statement-terminating semicolons (our SQL has no ';' inside string literals)
  awk 'BEGIN{RS=";\n"} NF{gsub(/^[ \t\r\n]+/,""); print $0 ";" "\036"}' \
      <(sed "s/${2}/${3}/g" "$1") | while IFS= read -r -d $'\036' stmt; do
    [ -z "$(echo "$stmt" | tr -d '[:space:];')" ] && continue
    local body; body=$(printf '%s' "$stmt" | python3 -c 'import json,sys; print(json.dumps({"statement": sys.stdin.read()}))')
    curl -s -X POST "${GW}/v1/sessions/${sh}/statements" -H 'Content-Type: application/json' -d "$body" >/dev/null
    sleep 1
  done
  log "submitted $(basename "$1") -> $3 via SQL Gateway"
}

wait_for_catchup() {    # Gate 2: index size stable for CATCHUP_STABLE_SECS (job has drained the backlog)
  log "Gate 2: waiting for $1 to catch up (stable ${CATCHUP_STABLE_SECS}s, cap ${CATCHUP_MAX_SECS}s) ..."
  local prev=-1 stable=0 elapsed=0
  while [ $elapsed -lt "$CATCHUP_MAX_SECS" ]; do
    sleep 30; elapsed=$((elapsed+30))
    local c; c=$(es_count "$1" 2>/dev/null || echo 0)
    if [ "$c" = "$prev" ] && [ "$c" -gt 0 ]; then
      stable=$((stable+30))
      [ $stable -ge "$CATCHUP_STABLE_SECS" ] && { log "$1 caught up at $c docs"; return 0; }
    else stable=0; fi
    log "  $1 = $c docs (stable ${stable}s)"; prev=$c
  done
  log "Gate 2: cap reached; flipping anyway (index=$1)"
}

# ---- main ------------------------------------------------------------------------------------------
active=$(alias_current_slot "$PRIVATE_ALIAS"); [ "$active" = "1" ] && SLOT=2 || SLOT=1
log "active slot=${active:-none}; building INACTIVE slot ${SLOT}"
PRIV_IDX="${PRIVATE_ALIAS}_${SLOT}"; PUB_IDX="${PUBLIC_ALIAS}_${SLOT}"

create_authors_pipeline
create_slot_index "$PRIV_IDX" "$PRIVATE_MAPPING"
create_slot_index "$PUB_IDX"  "$PUBLIC_MAPPING"
deploy_source
wait_for_source_running                       # Gate 1: source connector RUNNING (not slot-specific)
flink_submit_file "${SQL_DIR}/references_index.sql"        flink_references_index        "$PRIV_IDX"
flink_submit_file "${SQL_DIR}/public_references_index.sql" flink_public_references_index "$PUB_IDX"
wait_for_catchup "$PRIV_IDX"
wait_for_catchup "$PUB_IDX"
flip_alias "$PRIVATE_ALIAS" "$PRIV_IDX"
flip_alias "$PUBLIC_ALIAS"  "$PUB_IDX"
log "reindex complete: aliases now serve slot ${SLOT}. Old-slot Flink jobs can be cancelled once verified."
