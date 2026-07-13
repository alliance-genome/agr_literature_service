#!/bin/bash

# Source the status manager functions
source /status_manager.sh

# Configure sleep timings based on environment
if [[ "${ENV_STATE}" == "test" ]]; then
    # Test environment - much shorter sleeps for smaller datasets.
    # Fixed on purpose: test ignores the DBZ_KSQL_SETUP_SLEEP / DBZ_DATA_PROCESSING_SLEEP env vars,
    # so test runs stay fast regardless of what is configured for prod.
    CONNECTOR_SETUP_SLEEP=0
    KSQL_SETUP_SLEEP=5
    KSQL_POST_SLEEP=10
    DATA_PROCESSING_SLEEP=20
    echo "Running in TEST mode with reduced sleep timings"
else
    # Production environment - longer sleeps for full datasets.
    # The two big waits are overridable via the DBZ_-prefixed env vars (wired through
    # docker-compose), falling back to the safe production defaults when unset.
    CONNECTOR_SETUP_SLEEP=10
    KSQL_SETUP_SLEEP="${DBZ_KSQL_SETUP_SLEEP:-1200}"
    KSQL_POST_SLEEP=10
    DATA_PROCESSING_SLEEP="${DBZ_DATA_PROCESSING_SLEEP:-20000}"
    echo "Running in PRODUCTION mode (KSQL_SETUP_SLEEP=${KSQL_SETUP_SLEEP}s, DATA_PROCESSING_SLEEP=${DATA_PROCESSING_SLEEP}s)"
fi

# Track timing for metrics
SETUP_START=$(date +%s)

# SCRUM-6240: alias-based blue/green swap. The app queries an ALIAS (= DEBEZIUM_INDEX_NAME);
# the physical data lives in two fixed slots <name>_1 / <name>_2. Each rebuild targets the
# INACTIVE slot and then atomically flips the alias to it -- search always hits a fully-built
# index, and the previous slot stays as an instant-rollback backup. No reindex; bounded at 2
# copies. Same flow for test and prod; only the wait timings differ.
export INDEX_ALIAS="${DEBEZIUM_INDEX_NAME}"
export PUBLIC_INDEX_ALIAS="public_${DEBEZIUM_INDEX_NAME}"

if ! SLOT=$(resolve_inactive_suffix "${ELASTICSEARCH_HOST}" "${ELASTICSEARCH_PORT}" "${INDEX_ALIAS}"); then
    echo "FATAL: could not resolve the active slot (Elasticsearch unreachable or unexpected response)."
    echo "Aborting BEFORE touching any index, so a transient ES error can never clobber the live slot."
    set_reindex_status "error" "{\"message\": \"alias lookup failed; aborted before any index change\"}"
    exit 1
fi
export INDEX_NAME_CURRENT="${INDEX_ALIAS}_${SLOT}"
export PUBLIC_INDEX_NAME_CURRENT="${PUBLIC_INDEX_ALIAS}_${SLOT}"
echo "Building into slot _${SLOT}: ${INDEX_NAME_CURRENT} / ${PUBLIC_INDEX_NAME_CURRENT}"
echo "(alias '${INDEX_ALIAS}' keeps serving the other slot until the cutover)"

# Set initial reindexing status
set_reindex_status "setup" "{\"env_state\": \"${ENV_STATE}\", \"slot\": \"${SLOT}\"}"

# Delete + recreate ONLY the inactive slot (the live slot is never touched here)
curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${INDEX_NAME_CURRENT}
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${INDEX_NAME_CURRENT} -d @/elasticsearch-settings.json

curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_CURRENT}
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_CURRENT} -d @/elasticsearch-settings-public.json

export PGPASSWORD=${PSQL_PASSWORD}
# Drop replication slots (both old and new)
psql -h ${PSQL_HOST} -U ${PSQL_USERNAME} -p ${PSQL_PORT} -d ${PSQL_DATABASE} -c "DO \$\$DECLARE slot text; BEGIN FOREACH slot IN ARRAY ARRAY['debezium_unified','debezium_extract_fields','debezium_joined_tables','debezium_mod','debezium_referencetype','debezium_reference','debezium_citation','debezium_mod_referencetype','debezium_topic_entity_tag_source','debezium_copyright_license','debezium_mod_corpus_association','debezium_resource', debezium_curation_status'] LOOP IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = slot) THEN PERFORM pg_drop_replication_slot(slot); END IF; END LOOP; END\$\$;"

# Drop publications so the connector recreates them from the CURRENT table.include.list.
# The connector uses publication.autocreate.mode=filtered, which only creates a publication
# when it does not already exist -- it does NOT add newly-included tables to a pre-existing
# publication. Without dropping them here, tables added to table.include.list after the
# publication was first created (e.g. indexing_priority, manual_indexing_tag) never get
# published, so their row changes never stream into the index.
psql -h ${PSQL_HOST} -U ${PSQL_USERNAME} -p ${PSQL_PORT} -d ${PSQL_DATABASE} -c "DO \$\$DECLARE pub text; BEGIN FOREACH pub IN ARRAY ARRAY['debezium_unified','debezium_extract_fields','debezium_joined_tables','debezium_mod','debezium_referencetype','debezium_reference','debezium_citation','debezium_mod_referencetype','debezium_topic_entity_tag_source','debezium_copyright_license','debezium_mod_corpus_association','debezium_resource', 'debezium_curation_status'] LOOP EXECUTE format('DROP PUBLICATION IF EXISTS %I', pub); END LOOP; END\$\$;"

# Create single unified connector
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-unified.json)
sleep ${CONNECTOR_SETUP_SLEEP}

# Gate 1 (SCRUM-6231): instead of a blind ${KSQL_SETUP_SLEEP}s wait, poll until the Debezium snapshot
# has created the source topics the ksql CTAS sources reference. Test mode keeps the fixed short sleep
# so the integration test's timing is unchanged; in production KSQL_SETUP_SLEEP is the max-wait cap.
# expected_count = number of tables in the connector's table.include.list (wait for all per-table
# topics, robust to a slow snapshot on a big table); 0 if undeterminable (helper falls back to
# topic-set stability).
if [[ "${ENV_STATE}" == "test" ]]; then
    sleep ${KSQL_SETUP_SLEEP}
else
    EXPECTED_SOURCE_TOPICS=$(jq -r '.config["table.include.list"] // "" | select(length > 0)' /postgres-source-unified.json 2>/dev/null | awk -F',' '{print NF}')
    [[ "${EXPECTED_SOURCE_TOPICS}" =~ ^[0-9]+$ ]] || EXPECTED_SOURCE_TOPICS=0
    wait_for_source_topics_ready "${DEBEZIUM_CONNECTOR_HOST}" "${DEBEZIUM_CONNECTOR_PORT}" \
        "postgres-source-unified" "${EXPECTED_SOURCE_TOPICS}" "${KSQL_SETUP_SLEEP}" || true
fi

# --- SCRUM-6231: submit ksql statements ONE PER REQUEST, not as one big batch ---
# Posting all of ksql_queries.ksql in a single /ksql request hit a metastore-visibility
# race: a CTAS referencing a table created earlier in the SAME batch could fail with
# "<TABLE> does not exist", and ksqlDB then aborts every statement after it -- including
# the final reference_joined / public_reference_joined tables the ES sinks consume, so the
# index ends up empty. Submitting one statement per request makes each /ksql call block
# until its command is executed, so the next statement always sees the prior one. The
# leading `SET 'auto.offset.reset'='earliest'` is carried on every request via
# streamsProperties (ksql.streams.auto.offset.reset).
submit_ksql_statements() {
    local file="$1"
    local url="http://${DEBEZIUM_KSQLDB_HOST}:${DEBEZIUM_KSQLDB_PORT}/ksql"
    local raw="/tmp/ksql_raw.sql"
    local stmtdir="/tmp/ksql_stmts"
    local n=0 failed=0 f label body resp attempt first ok

    # 1) Unwrap the JSON ({"ksql":"<SQL>"}) to raw SQL: keep the lines between the opening
    #    quote line (a line that is just ") and the closing quote line (just ",), and
    #    unescape the only escape the file uses (\" -> ").
    awk '/^"$/ && !s {s=1; next} /^",$/ && s {exit} s' "$file" | sed 's/\\"/"/g' > "$raw"
    if [ ! -s "$raw" ]; then
        echo "ERROR: could not extract SQL from ${file}; aborting ksql setup." >&2
        return 1
    fi

    # 2) Split on ';' into one file per statement (statements span multiple lines).
    rm -rf "$stmtdir"; mkdir -p "$stmtdir"
    awk -v d="$stmtdir" 'BEGIN{RS=";"; i=0}
        { s=$0; gsub(/^[ \t\n]+|[ \t\n]+$/,"",s);
          if (length(s)>0) { i++; fn=sprintf("%s/%04d.sql", d, i); printf "%s;", s > fn; close(fn) } }' "$raw"

    # 3) Submit each statement on its own request, retrying briefly on a transient
    #    dependency error (defense-in-depth; sequential submission should prevent it).
    for f in "$stmtdir"/*.sql; do
        first=$(grep -vE '^[[:space:]]*$' "$f" | head -1)
        case "$(printf '%s' "$first" | tr 'a-z' 'A-Z')" in
            SET\ *) continue ;;   # the SET is applied via streamsProperties below
        esac
        n=$((n+1))
        label=$(head -c 60 "$f" | tr '\n' ' ')
        body=$(jq -Rs '{ksql: ., streamsProperties: {"ksql.streams.auto.offset.reset":"earliest"}}' "$f")
        ok=0
        for attempt in 1 2 3 4 5; do
            resp=$(curl -s -X POST "$url" -H "Accept: application/vnd.ksql.v1+json" \
                        -H "Content-Type: application/json" -d "$body")
            if echo "$resp" | grep -q '"@type":"statement_error"'; then
                if echo "$resp" | grep -qiE 'already exists'; then ok=1; break; fi
                echo "  [stmt ${n} attempt ${attempt}] retry '${label}...' -> $(echo "$resp" | grep -oE '"message":"[^"]*"' | head -1)"
                sleep 3
            else
                ok=1; break
            fi
        done
        if [ "$ok" = "1" ]; then
            echo "  [stmt ${n}] OK: ${label}..."
        else
            echo "  [stmt ${n}] FAILED: ${label}..."
            failed=$((failed+1))
        fi
    done
    echo "ksql submission complete: ${n} statements submitted, ${failed} failed."
    [ "$failed" = "0" ]
}

submit_ksql_statements /ksql_queries.ksql || echo "WARNING: one or more ksql statements failed; downstream topics/index may be incomplete."
sleep ${KSQL_POST_SLEEP}

# SCRUM-6240: one sink per index (no temp/permanent split -- there is no reindex). The sink
# writes into the slot being built and keeps writing there after the alias flip; the cutover is
# alias-only, decoupled from Kafka Connect. Delete any prior connector of the same name first
# (it points at the now-old slot); DELETE is a no-op if it does not exist.
export SINK_NAME="elastic-sink"
export PUBLIC_SINK_NAME="elastic-sink-public"
export SINK_INDEX_NAME="${INDEX_NAME_CURRENT}"
export PUBLIC_SINK_INDEX_NAME="${PUBLIC_INDEX_NAME_CURRENT}"

curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/${SINK_NAME}
curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/${PUBLIC_SINK_NAME}

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$SINK_INDEX_NAME$SINK_NAME' < /elasticsearch-sink.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$PUBLIC_SINK_INDEX_NAME$PUBLIC_SINK_NAME' < /elasticsearch-sink-public.json)

# Track setup completion
SETUP_END=$(date +%s)
SETUP_DURATION=$((SETUP_END - SETUP_START))

# Update status to data processing phase
set_reindex_status "data_processing" "{\"phase\": \"waiting_for_kafka_data\"}"

# Wait for data processing with intelligent polling for test mode
if [[ "${ENV_STATE}" == "test" ]]; then
    sleep ${DATA_PROCESSING_SLEEP}
    echo "Polling for data in test mode..."
    max_attempts=12  # 12 attempts * 5 seconds = 1 minute max wait
    attempt=0
    while [[ $attempt -lt $max_attempts ]]; do
        sleep 5
        new_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${INDEX_NAME_CURRENT}/_count 2>/dev/null | jq -r '.count // 0' 2>/dev/null || echo "0")
        public_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_CURRENT}/_count 2>/dev/null | jq -r '.count // 0' 2>/dev/null || echo "0")
        
        echo "Attempt $((attempt + 1)): Private index: ${new_index_doc_count} docs, Public index: ${public_index_doc_count} docs"
        
        if [[ $new_index_doc_count -gt 0 ]] && [[ $public_index_doc_count -gt 0 ]]; then
            echo "Data found in both indexes! Proceeding..."
            break
        fi
        
        attempt=$((attempt + 1))
    done
else
    # Gate 2 (SCRUM-6231): instead of a blind ${DATA_PROCESSING_SLEEP}s wait, poll until the pipeline
    # DRAINS -- both temp indexes' docs.count + store.size_in_bytes + index_total strictly unchanged
    # for 10 min (activity-based, so it survives the sink's by-id upserts that keep doc COUNT flat
    # while joined objects are still re-indexed) AND both ES sink connectors RUNNING with no failed
    # tasks. DATA_PROCESSING_SLEEP is the hard max-wait cap (fallback if live CDC keeps it moving).
    echo "Production mode: waiting for the data pipeline to drain (max ${DATA_PROCESSING_SLEEP}s)..."
    wait_for_pipeline_drained "${ELASTICSEARCH_HOST}" "${ELASTICSEARCH_PORT}" \
        "${INDEX_NAME_CURRENT}" "${PUBLIC_INDEX_NAME_CURRENT}" \
        "${DEBEZIUM_CONNECTOR_HOST}" "${DEBEZIUM_CONNECTOR_PORT}" \
        "${SINK_NAME}" "${PUBLIC_SINK_NAME}" "${DATA_PROCESSING_SLEEP}" || true

    # Check both indexes have data and promote them
    new_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${INDEX_NAME_CURRENT}/_count | jq '.count')
    public_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_CURRENT}/_count | jq '.count')
fi

echo "Final counts - Private index: ${new_index_doc_count} docs, Public index: ${public_index_doc_count} docs"

# Track data processing completion
DATA_PROCESSING_END=$(date +%s)
DATA_PROCESSING_DURATION=$((DATA_PROCESSING_END - SETUP_END))

# SCRUM-6240: promote by optimizing the freshly-built slot and atomically flipping the alias to
# it. The old slot stays as the instant-rollback backup (overwritten on the next rebuild). Same
# path for test and prod. Guard: never flip to an empty slot, so a failed/empty build can never
# replace a healthy live index.
if [[ $new_index_doc_count -gt 0 ]] && [[ $public_index_doc_count -gt 0 ]]; then
    set_reindex_status "reindexing" "{\"phase\": \"optimize_and_flip\", \"slot\": \"${SLOT}\", \"private_index_docs\": $new_index_doc_count, \"public_index_docs\": $public_index_doc_count}"

    # Optimize + warm OFF the serving path (the alias still points at the old slot here). These are
    # best-effort: a failed optimize leaves the index unoptimized but still correct, so it does not
    # block the flip.
    optimize_and_warm_index "${ELASTICSEARCH_HOST}" "${ELASTICSEARCH_PORT}" "${INDEX_NAME_CURRENT}"
    optimize_and_warm_index "${ELASTICSEARCH_HOST}" "${ELASTICSEARCH_PORT}" "${PUBLIC_INDEX_NAME_CURRENT}"

    # Atomic cutover: point both aliases at the new slot (handles first-run bootstrap). The flip is
    # THE critical step, so only report success if BOTH aliases were acknowledged by ES.
    if flip_alias "${ELASTICSEARCH_HOST}" "${ELASTICSEARCH_PORT}" "${INDEX_ALIAS}" "${INDEX_NAME_CURRENT}" \
       && flip_alias "${ELASTICSEARCH_HOST}" "${ELASTICSEARCH_PORT}" "${PUBLIC_INDEX_ALIAS}" "${PUBLIC_INDEX_NAME_CURRENT}"; then
        PROMOTE_END=$(date +%s)
        PROMOTE_DURATION=$((PROMOTE_END - DATA_PROCESSING_END))
        echo "Alias '${INDEX_ALIAS}' now serves slot _${SLOT} (${new_index_doc_count} docs); previous slot retained as backup."
        set_reindex_status "completed" "{\"message\": \"Alias flipped to slot _${SLOT}\", \"total_documents\": $new_index_doc_count}"
        save_completion_metrics "$(jq -r '.started_at' /var/lib/debezium_status/reindex_status.json)" \
            "$SETUP_DURATION" "$DATA_PROCESSING_DURATION" "$PROMOTE_DURATION" "$new_index_doc_count"
    else
        echo "ERROR: alias flip was NOT acknowledged; the live index was NOT switched to slot _${SLOT}. The previous slot keeps serving."
        set_reindex_status "error" "{\"message\": \"alias flip failed; live index unchanged\", \"slot\": \"${SLOT}\"}"
        exit 1
    fi
else
    # A 0-doc build means the pipeline produced nothing. Do NOT flip (this guard protects the live
    # slot). NOTE: the sink was already repointed at this (empty) build slot above, so the previous
    # slot keeps SERVING but no longer receives CDC -- it is frozen, not merely "unchanged" -- until
    # a rebuild succeeds. Investigate the pipeline.
    echo "WARNING: build slot _${SLOT} empty (private=${new_index_doc_count}, public=${public_index_doc_count}); NOT flipping alias."
    echo "The previous slot keeps serving but is now FROZEN (its sink moved to the empty slot) until a rebuild succeeds."
    set_reindex_status "error" "{\"message\": \"empty build slot; alias not flipped; served slot frozen\", \"private_index_docs\": $new_index_doc_count, \"public_index_docs\": $public_index_doc_count}"
    exit 1
fi

echo "Debezium setup completed successfully!"