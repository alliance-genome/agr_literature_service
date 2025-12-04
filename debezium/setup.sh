#!/bin/bash

# Source the status manager functions
source /status_manager.sh

# Configure sleep timings based on environment
if [[ "${ENV_STATE}" == "test" ]]; then
    # Test environment - much shorter sleeps for smaller datasets
    CONNECTOR_SETUP_SLEEP=0
    KSQL_SETUP_SLEEP=5
    KSQL_POST_SLEEP=10
    DATA_PROCESSING_SLEEP=20
    echo "Running in TEST mode with reduced sleep timings"
else
    # Production environment - longer sleeps for full datasets
    CONNECTOR_SETUP_SLEEP=10
    KSQL_SETUP_SLEEP=1200
    KSQL_POST_SLEEP=10
    DATA_PROCESSING_SLEEP=20000
    echo "Running in PRODUCTION mode with full sleep timings"
fi

# Track timing for metrics
SETUP_START=$(date +%s)

export INDEX_NAME_FINAL="${DEBEZIUM_INDEX_NAME}"
export PUBLIC_INDEX_NAME_FINAL="public_${INDEX_NAME_FINAL}"

if [[ "${ENV_STATE}" == "test" ]]; then
    # Test mode - use final index names directly
    export INDEX_NAME_CURRENT="${INDEX_NAME_FINAL}"
    export PUBLIC_INDEX_NAME_CURRENT="${PUBLIC_INDEX_NAME_FINAL}"
else
    # Production mode - use temporary indexes
    export INDEX_NAME_CURRENT="${INDEX_NAME_FINAL}_temp"
    export PUBLIC_INDEX_NAME_CURRENT="${PUBLIC_INDEX_NAME_FINAL}_temp"
fi

# Set initial reindexing status
set_reindex_status "setup" "{\"env_state\": \"${ENV_STATE}\"}"

# Delete and create both private and public indexes
curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${INDEX_NAME_CURRENT}
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${INDEX_NAME_CURRENT} -d @/elasticsearch-settings.json

curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_CURRENT}
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_CURRENT} -d @/elasticsearch-settings-public.json

export PGPASSWORD=${PSQL_PASSWORD}
# Drop replication slots (both old and new)
psql -h ${PSQL_HOST} -U ${PSQL_USERNAME} -p ${PSQL_PORT} -d ${PSQL_DATABASE} -c "DO \$\$DECLARE slot text; BEGIN FOREACH slot IN ARRAY ARRAY['debezium_unified','debezium_extract_fields','debezium_joined_tables','debezium_mod','debezium_referencetype','debezium_reference','debezium_citation','debezium_mod_referencetype','debezium_topic_entity_tag_source','debezium_copyright_license','debezium_mod_corpus_association','debezium_resource'] LOOP IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = slot) THEN PERFORM pg_drop_replication_slot(slot); END IF; END LOOP; END\$\$;"

# Create single unified connector
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-unified.json)
sleep ${CONNECTOR_SETUP_SLEEP}
sleep ${KSQL_SETUP_SLEEP}

curl -X "POST" http://${DEBEZIUM_KSQLDB_HOST}:${DEBEZIUM_KSQLDB_PORT}/ksql -H "Accept: application/vnd.ksql.v1+json" -H "Content-Type: application/json" -d @/ksql_queries.ksql
sleep ${KSQL_POST_SLEEP}

# Create sink connectors - use different names for test vs production
if [[ "${ENV_STATE}" == "test" ]]; then
    # Test mode - use final connector names directly
    export SINK_NAME="elastic-sink"
    export PUBLIC_SINK_NAME="elastic-sink-public"
else
    # Production mode - use temporary connector names
    export SINK_NAME="elastic-sink-temp"
    export PUBLIC_SINK_NAME="elastic-sink-public-temp"
fi

export SINK_INDEX_NAME="${INDEX_NAME_CURRENT}"
export PUBLIC_SINK_INDEX_NAME="${PUBLIC_INDEX_NAME_CURRENT}"
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
    echo "Production mode: waiting ${DATA_PROCESSING_SLEEP} seconds for data processing..."
    sleep ${DATA_PROCESSING_SLEEP}
    
    # Check both indexes have data and promote them
    new_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${INDEX_NAME_CURRENT}/_count | jq '.count')
    public_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_CURRENT}/_count | jq '.count')
fi

echo "Final counts - Private index: ${new_index_doc_count} docs, Public index: ${public_index_doc_count} docs"

# Track data processing completion
DATA_PROCESSING_END=$(date +%s)
DATA_PROCESSING_DURATION=$((DATA_PROCESSING_END - SETUP_END))

if [[ "${ENV_STATE}" == "test" ]]; then
    # Test mode - indexes are already final, no promotion needed
    echo "Test mode: Setup complete with final indexes"

    # Mark as completed
    set_reindex_status "completed" "{\"message\": \"Test mode - no reindex needed\"}"

    # Save metrics
    save_completion_metrics "$(jq -r '.started_at' /var/lib/debezium_status/reindex_status.json)" \
        "$SETUP_DURATION" "$DATA_PROCESSING_DURATION" 0 "$new_index_doc_count"
else
    # Production mode - promote temporary indexes to final if they have data
    if [[ $new_index_doc_count -gt 0 ]] && [[ $public_index_doc_count -gt 0 ]]
    then
      # Delete old connectors
      curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink
      curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink-public

      # Update status to reindexing phase
      REINDEX_START=$(date +%s)
      set_reindex_status "reindexing" "{\"phase\": \"promoting_indexes\", \"private_index_docs\": $new_index_doc_count, \"public_index_docs\": $public_index_doc_count}"

      # Prepare private index
      echo "Preparing private index..."
      curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${INDEX_NAME_FINAL}
      curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${INDEX_NAME_FINAL} -d @/elasticsearch-settings.json

      # Prepare public index
      echo "Preparing public index..."
      curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_FINAL}
      curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_FINAL} -d @/elasticsearch-settings-public.json

      # Start both reindex operations in parallel
      echo "Starting parallel reindex operations..."
      PRIVATE_TASK_ID=$(start_reindex "${ELASTICSEARCH_HOST}" "${ELASTICSEARCH_PORT}" "${INDEX_NAME_CURRENT}" "${INDEX_NAME_FINAL}")
      PUBLIC_TASK_ID=$(start_reindex "${ELASTICSEARCH_HOST}" "${ELASTICSEARCH_PORT}" "${PUBLIC_INDEX_NAME_CURRENT}" "${PUBLIC_INDEX_NAME_FINAL}")

      # Verify both tasks started successfully
      if [[ "$PRIVATE_TASK_ID" == "null" ]] || [[ "$PUBLIC_TASK_ID" == "null" ]]; then
        echo "Error: Failed to start one or more reindex tasks"
        echo "Private task ID: $PRIVATE_TASK_ID"
        echo "Public task ID: $PUBLIC_TASK_ID"
        exit 1
      fi

      # Poll both tasks in parallel
      poll_multiple_reindex_tasks "${ELASTICSEARCH_HOST}" "${ELASTICSEARCH_PORT}" \
        "$PRIVATE_TASK_ID" "${INDEX_NAME_FINAL}" \
        "$PUBLIC_TASK_ID" "${PUBLIC_INDEX_NAME_FINAL}"

      # Track reindex completion
      REINDEX_END=$(date +%s)
      REINDEX_DURATION=$((REINDEX_END - REINDEX_START))

      # Create permanent connectors
      export SINK_NAME="elastic-sink"
      export PUBLIC_SINK_NAME="elastic-sink-public"
      export SINK_INDEX_NAME="${INDEX_NAME_FINAL}"
      export PUBLIC_SINK_INDEX_NAME="${PUBLIC_INDEX_NAME_FINAL}"
      curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$SINK_INDEX_NAME$SINK_NAME' < /elasticsearch-sink.json)
      curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$PUBLIC_SINK_INDEX_NAME$PUBLIC_SINK_NAME' < /elasticsearch-sink-public.json)

      # Mark as completed
      set_reindex_status "completed" "{\"message\": \"Reindex completed successfully\", \"total_documents\": $new_index_doc_count}"

      # Save metrics
      save_completion_metrics "$(jq -r '.started_at' /var/lib/debezium_status/reindex_status.json)" \
          "$SETUP_DURATION" "$DATA_PROCESSING_DURATION" "$REINDEX_DURATION" "$new_index_doc_count"
    fi

    # Clean up temporary connectors (only in production mode)
    curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink-temp
    curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink-public-temp
fi

echo "Debezium setup completed successfully!"