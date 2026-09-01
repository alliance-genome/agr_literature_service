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
  # "no alias yet" is the normal bootstrap state (fresh Elasticsearch), not an error: the greps
  # exit 1 on no match, and under `set -euo pipefail` that would kill the script at the first
  # main-body line with no output at all. Swallow it and let the caller treat empty as "no slot".
  es GET "/_alias/$1" 2>/dev/null | grep -oE "${1}_[12]" | grep -oE '[12]$' | head -1 || true
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

# ---- Gate 1.5: wait for the Debezium topics ---------------------------------------------------------
# Debezium creates a table's topic only when the snapshot first emits a row for it, so the topics
# appear progressively over the life of the snapshot. A Flink Kafka source that starts before its
# topic exists dies immediately with UnknownTopicOrPartitionException and the whole job FAILs. The
# connector's own /topics endpoint is the authoritative list of what it has produced so far, so this
# stays curl+jq only. Required topics are derived from the SQL, never hardcoded.
wait_for_topics() {     # $@ = sql files to scan for 'topic'='...'
  local want total elapsed=0 have missing t
  want=$(grep -ho "'topic'='[^']*'" "$@" | cut -d"'" -f4 | sort -u)
  total=$(printf '%s\n' "$want" | grep -c .)
  log "Gate 1.5: waiting for $total Debezium topics (snapshot creates them lazily) ..."
  while [ "$elapsed" -lt "$CATCHUP_MAX_SECS" ]; do
    missing=""
    have=$(curl -s "${CONNECT}/connectors/${SOURCE_CONNECTOR}/topics" \
           | jq -r ".[\"${SOURCE_CONNECTOR}\"].topics[]?" 2>/dev/null | sort -u)
    for t in $want; do
      printf '%s\n' "$have" | grep -qx "$t" || missing="$missing $t"
    done
    if [ -z "$missing" ]; then
      log "Gate 1.5: all $total topics present (waited ${elapsed}s)"
      return 0
    fi
    log "  missing $(printf '%s' "$missing" | wc -w)/$total:$(printf '%s' "$missing" | cut -c1-100)"
    sleep 15; elapsed=$((elapsed+15))
  done
  log "FATAL: Gate 1.5 timed out after ${elapsed}s; still missing:$missing"
  log "  a table with zero rows never produces a topic -- check those tables in the source DB"
  exit 1
}

# Expected document count for an index, straight from the source DB. Gate 2 needs a real target:
# "the count stopped changing" is indistinguishable from "the job stalled" without one.
db_count() {            # $1 = SQL returning a single integer
  # Bounded: on a saturated box this count can crawl, and Gate 2 must not hang on it. An empty
  # result disables the stall guard, so the caller warns loudly rather than proceeding silently.
  # statement_timeout is applied via PGOPTIONS, NOT a second -c: psql echoes the "SET" command tag
  # on stdout, and `tr -d '[:space:]'` then glues it onto the number ("SET1295410"). That value
  # fails every `[ "$target" -gt 0 ]` test, which silently sets min=0 and DISABLES Gate 2's stall
  # guard -- the run still completes and can flip the alias onto a partial index.
  PGPASSWORD="$PSQL_PASSWORD" \
  PGOPTIONS="-c statement_timeout=${DBZ_FLINK_TARGET_QUERY_TIMEOUT:-180s}" \
  psql -h "$PSQL_HOST" -p "$PSQL_PORT" -U "$PSQL_USERNAME" \
    -d "$PSQL_DATABASE" -v ON_ERROR_STOP=1 \
    -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

# Current state of a Flink job by pipeline.name (empty if no job from THIS run is listed yet).
# /jobs/overview retains FAILED/FINISHED jobs from earlier runs under the same pipeline.name, so
# restrict to jobs started at/after this run's marker and take the newest. Without that, a stale
# FAILED entry from a previous attempt aborts the new run before its job even registers.
flink_job_state() {     # $1=job name  $2=only consider jobs started at/after this epoch-ms
  curl -s "${FLINK_REST}/jobs/overview" \
    | jq -r --arg n "$1" --argjson since "${2:-0}" \
        '[.jobs[]? | select(.name==$n) | select(.["start-time"] >= $since)]
          | sort_by(.["start-time"]) | last | .state // empty' 2>/dev/null | head -1
}

# ---- Flink SQL Gateway submission ------------------------------------------------------------------
# Opens one session, executes every statement of a .sql file in order (so CREATE TABLE/VIEW precede the
# INSERT), retargeting the ES sink index to the given slot index. The final INSERT submits the job.
flink_submit_file() {   # $1=sql file  $2=index-name-in-file  $3=slot-index-name
  # `|| true` so a gateway that is down (grep finds no handle -> exit 1 -> pipefail) reaches the
  # explicit FATAL below instead of dying silently under `set -e`.
  local sh; sh=$(curl -s -X POST "${GW}/v1/sessions" -H 'Content-Type: application/json' -d '{}' \
                 | grep -o '"sessionHandle":"[^"]*"' | cut -d'"' -f4 || true)
  [ -n "$sh" ] || { log "FATAL: could not open SQL Gateway session (is flink_sqlgateway up?)"; exit 1; }
  # split on statement-terminating semicolons (our SQL has no ';' inside string literals).
  # Reads from a process substitution rather than a pipe so a FATAL below aborts the whole
  # script instead of just a subshell.
  local n=0
  while IFS= read -r -d $'\036' stmt; do
    [ -z "$(echo "$stmt" | tr -d '[:space:];')" ] && continue
    # jq -Rs JSON-encodes the statement. The runner image ships jq, not python3, and this
    # script is meant to be curl/jq-only so it runs in a plain container.
    local body resp op
    body=$(printf '%s' "$stmt" | jq -Rs '{statement: .}')
    resp=$(curl -s -X POST "${GW}/v1/sessions/${sh}/statements" \
                -H 'Content-Type: application/json' -d "$body")
    op=$(printf '%s' "$resp" | jq -r '.operationHandle // empty')
    if [ -z "$op" ]; then
      log "FATAL: SQL Gateway rejected statement #$((n+1)) of $(basename "$1")"
      log "  stmt: $(printf '%s' "$stmt" | tr '\n' ' ' | cut -c1-200)"
      log "  resp: $(printf '%s' "$resp" | cut -c1-400)"
      exit 1
    fi
    n=$((n+1))
    sleep 1
  done < <(awk 'BEGIN{RS=";\n"} NF{gsub(/^[ \t\r\n]+/,""); print $0 ";" "\036"}' \
                <(sed "s/${2}/${3}/g" "$1"))
  log "submitted $(basename "$1") ($n statements) -> $3 via SQL Gateway"
}

wait_for_catchup() {    # Gate 2: index size stable for CATCHUP_STABLE_SECS (job has drained the backlog)
                        # $1=index  $2=flink job name  $3=run marker epoch-ms  $4=expected doc count
  local target="${4:-0}" min=0
  [ "$target" -gt 0 ] 2>/dev/null && min=$(( target * ${DBZ_FLINK_MIN_CATCHUP_PCT:-99} / 100 ))
  log "Gate 2: waiting for $1 to catch up (target ${target:-?}, min ${min}, stable ${CATCHUP_STABLE_SECS}s, cap ${CATCHUP_MAX_SECS}s) ..."
  local prev=-1 stable=0 elapsed=0
  while [ $elapsed -lt "$CATCHUP_MAX_SECS" ]; do
    sleep 30; elapsed=$((elapsed+30))
    # A dead job leaves the count frozen, which otherwise looks identical to "still working" and
    # burns the whole cap before flipping. Fail fast instead. An empty state means the job is not
    # listed yet (transient right after submit), so only act on a definite terminal state.
    local js; js=$(flink_job_state "$2" "${3:-0}")
    case "$js" in
      FAILED|CANCELED|SUSPENDED|FAILING)
        log "FATAL: Flink job '$2' is $js after ${elapsed}s -- aborting without flipping $1"
        log "  inspect: curl ${FLINK_REST}/jobs/overview  and  /jobs/<jid>/exceptions"
        exit 1 ;;
    esac
    local c; c=$(es_count "$1" 2>/dev/null || echo 0)
    if [ "$c" = "$prev" ] && [ "$c" -gt 0 ]; then
      stable=$((stable+30))
      if [ $stable -ge "$CATCHUP_STABLE_SECS" ]; then
        # A stalled job also holds a perfectly stable count. Only a count that actually reached the
        # source total means "caught up"; anything else is a stall and must not reach the alias.
        if [ "$min" -gt 0 ] && [ "$c" -lt "$min" ]; then
          log "FATAL: $1 held at $c docs for ${stable}s but target is $target (min $min, $((c*100/target))%)."
          log "  That is a STALL, not catch-up -- refusing to flip the alias onto a partial index."
          exit 1
        fi
        log "$1 caught up at $c docs (target $target)"; return 0
      fi
    else stable=0; fi
    log "  $1 = $c docs$([ "$target" -gt 0 ] 2>/dev/null && echo " ($((c*100/target))% of $target)") (stable ${stable}s, job=$js)"; prev=$c
  done
  # Cap reached. Flipping an alias onto an empty index takes the search down, so never do it.
  local c; c=$(es_count "$1" 2>/dev/null || echo 0)
  if [ "${c:-0}" -le 0 ]; then
    log "FATAL: Gate 2 cap reached and $1 is empty ($c docs) -- refusing to flip the alias"
    exit 1
  fi
  if [ "$min" -gt 0 ] && [ "$c" -lt "$min" ]; then
    log "FATAL: Gate 2 cap reached with $1 at $c/$target docs ($((c*100/target))%) -- refusing to flip a partial index"
    exit 1
  fi
  log "Gate 2: cap reached; flipping anyway (index=$1, $c docs of $target)"
}

# Re-running this script rebuilds a slot index from scratch (DELETE + PUT). Any job left running
# from a previous attempt is still writing into that same index, so it must be cancelled first --
# otherwise two generations race and the "caught up" count is meaningless.
cancel_stale_jobs() {   # $@ = pipeline names
  local n j st
  for n in "$@"; do
    for j in $(curl -s "${FLINK_REST}/jobs/overview" \
               | jq -r --arg n "$n" '.jobs[]? | select(.name==$n)
                   | select(.state=="RUNNING" or .state=="RESTARTING" or .state=="CREATED")
                   | .jid' 2>/dev/null); do
      curl -s -X PATCH "${FLINK_REST}/jobs/${j}?mode=cancel" >/dev/null 2>&1 || true
      log "cancelled stale job $n ($j)"
      sleep 2
    done
  done
}

# ---- main ------------------------------------------------------------------------------------------
active=$(alias_current_slot "$PRIVATE_ALIAS"); [ "$active" = "1" ] && SLOT=2 || SLOT=1
log "active slot=${active:-none}; building INACTIVE slot ${SLOT}"
PRIV_IDX="${PRIVATE_ALIAS}_${SLOT}"; PUB_IDX="${PUBLIC_ALIAS}_${SLOT}"

cancel_stale_jobs references_index public_references_index
create_authors_pipeline
create_slot_index "$PRIV_IDX" "$PRIVATE_MAPPING"
create_slot_index "$PUB_IDX"  "$PUBLIC_MAPPING"
# DBZ_FLINK_SKIP_SOURCE=1 reuses the CDC topics already in Kafka instead of re-snapshotting.
# Valid whenever the topics still hold the full history (they are read from earliest); use it when
# only the SQL or Flink config changed.
if [ "${DBZ_FLINK_SKIP_SOURCE:-0}" = "1" ]; then
  log "skipping Debezium source redeploy (DBZ_FLINK_SKIP_SOURCE=1); reusing existing topics"
else
  deploy_source
fi
wait_for_source_running                       # Gate 1: source connector RUNNING (not slot-specific)
wait_for_topics "${SQL_DIR}/references_index.sql" "${SQL_DIR}/public_references_index.sql"
# marker so Gate 2 ignores jobs left over from earlier attempts
RUN_SINCE_MS=$(( $(date -u +%s) * 1000 ))
# Expected doc counts, mirroring each INSERT's INNER joins (LEFT joins cannot drop a reference):
#   private: FROM reference JOIN citation
#   public : the same, plus an INNER join on the in-corpus aggregate (mca.corpus = TRUE)
PRIV_TARGET=$(db_count "SELECT count(*) FROM reference r JOIN citation c ON r.citation_id = c.citation_id")
PUB_TARGET=$(db_count "SELECT count(DISTINCT r.reference_id) FROM reference r
                        JOIN citation c ON r.citation_id = c.citation_id
                        JOIN mod_corpus_association m ON m.reference_id = r.reference_id AND m.corpus = TRUE")
log "targets from source DB: private=${PRIV_TARGET:-unavailable} public=${PUB_TARGET:-unavailable}"
for _t in "$PRIV_TARGET" "$PUB_TARGET"; do
  # Must be all-digits, not merely non-empty: any stray token (a psql command tag, a notice) makes
  # every `-gt` test fail, which disables the stall guard without saying so.
  case "$_t" in
    "" | *[!0-9]*) log "WARNING: target count '${_t}' is not a plain number -- Gate 2's stall guard is DISABLED for that index" ;;
  esac
done
flink_submit_file "${SQL_DIR}/references_index.sql"        flink_references_index        "$PRIV_IDX"
flink_submit_file "${SQL_DIR}/public_references_index.sql" flink_public_references_index "$PUB_IDX"
wait_for_catchup "$PRIV_IDX" references_index        "$RUN_SINCE_MS" "$PRIV_TARGET"
wait_for_catchup "$PUB_IDX"  public_references_index "$RUN_SINCE_MS" "$PUB_TARGET"
flip_alias "$PRIVATE_ALIAS" "$PRIV_IDX"
flip_alias "$PUBLIC_ALIAS"  "$PUB_IDX"
log "reindex complete: aliases now serve slot ${SLOT}. Old-slot Flink jobs can be cancelled once verified."
