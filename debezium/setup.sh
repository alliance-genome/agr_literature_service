#!/bin/bash

# Configure sleep timings based on environment
if [[ "${ENV_STATE}" == "test" ]]; then
    # Test environment - much shorter sleeps for smaller datasets
    CONNECTOR_SETUP_SLEEP=5
    KSQL_SETUP_SLEEP=30
    KSQL_POST_SLEEP=60
    DATA_PROCESSING_SLEEP=60
    echo "Running in TEST mode with reduced sleep timings"
else
    # Production environment - longer sleeps for full datasets
    CONNECTOR_SETUP_SLEEP=10
    KSQL_SETUP_SLEEP=300
    KSQL_POST_SLEEP=10
    DATA_PROCESSING_SLEEP=20000
    echo "Running in PRODUCTION mode with full sleep timings"
fi

DEBEZIUM_INDEX_NAME_ORIG="${DEBEZIUM_INDEX_NAME}"
PUBLIC_INDEX_NAME_ORIG="public_references_index"

if [[ "${ENV_STATE}" == "test" ]]; then
    # Test mode - use final index names directly
    export PUBLIC_INDEX_NAME="${PUBLIC_INDEX_NAME_ORIG}"
else
    # Production mode - use temporary indexes
    DEBEZIUM_INDEX_NAME="${DEBEZIUM_INDEX_NAME}_temp"
    export PUBLIC_INDEX_NAME="${PUBLIC_INDEX_NAME_ORIG}_temp"
fi

# Delete and create both private and public indexes
curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME}
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME} -d @/elasticsearch-settings.json

curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME}
curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME} -d @/elasticsearch-settings-public.json

export PGPASSWORD=${PSQL_PASSWORD}
psql -h ${PSQL_HOST} -U ${PSQL_USERNAME} -p ${PSQL_PORT} -d ${PSQL_DATABASE} -c "select pg_drop_replication_slot('debezium_mod'); select pg_drop_replication_slot('debezium_referencetype'); select pg_drop_replication_slot('debezium_reference'); select pg_drop_replication_slot('debezium_joined_tables'); select pg_drop_replication_slot('debezium_citation'); select pg_drop_replication_slot('debezium_mod_referencetype'); select pg_drop_replication_slot('debezium_topic_entity_tag_source'); select pg_drop_replication_slot('debezium_public_tables');"

# Create existing connectors
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-reference.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-citation.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-mod.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-mod-referencetype.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-referencetype.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-topic-entity-tag-source.json)

# Create new connector for public index tables
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-public-tables.json)

sleep ${CONNECTOR_SETUP_SLEEP}
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$PSQL_HOST$PSQL_USERNAME$PSQL_PORT$PSQL_DATABASE$PSQL_PASSWORD' < /postgres-source-joined_tables.json)
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

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$DEBEZIUM_INDEX_NAME$SINK_NAME' < /elasticsearch-sink.json)
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$PUBLIC_INDEX_NAME$PUBLIC_SINK_NAME' < /elasticsearch-sink-public.json)

# Wait for data processing with intelligent polling for test mode
if [[ "${ENV_STATE}" == "test" ]]; then
    sleep 30
    echo "Polling for data in test mode..."
    max_attempts=12  # 12 attempts * 5 seconds = 1 minute max wait
    attempt=0
    while [[ $attempt -lt $max_attempts ]]; do
        sleep 5
        new_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME}/_count 2>/dev/null | jq -r '.count // 0' 2>/dev/null || echo "0")
        public_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME}/_count 2>/dev/null | jq -r '.count // 0' 2>/dev/null || echo "0")
        
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
    new_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME}/_count | jq '.count')
    public_index_doc_count=$(curl -s http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME}/_count | jq '.count')
fi

echo "Final counts - Private index: ${new_index_doc_count} docs, Public index: ${public_index_doc_count} docs"

if [[ "${ENV_STATE}" == "test" ]]; then
    # Test mode - indexes are already final, no promotion needed
    echo "Test mode: Setup complete with final indexes"
else
    # Production mode - promote temporary indexes to final if they have data
    if [[ $new_index_doc_count -gt 0 ]] && [[ $public_index_doc_count -gt 0 ]]
    then
      # Delete old connectors
      curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink
      curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink-public

      # Promote private index
      DEBEZIUM_INDEX_NAME="${DEBEZIUM_INDEX_NAME_ORIG}"
      curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME_ORIG}
      curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${DEBEZIUM_INDEX_NAME_ORIG} -d @/elasticsearch-settings.json
      curl -i -X POST -H "Accept:application/json" -H "Content-Type: application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/_reindex?pretty -d"{\"source\": {\"index\": \"${DEBEZIUM_INDEX_NAME}\"}, \"dest\": {\"index\": \"${DEBEZIUM_INDEX_NAME_ORIG}\"}}"
      
      # Promote public index
      PUBLIC_INDEX_NAME="${PUBLIC_INDEX_NAME_ORIG}"
      curl -i -X DELETE http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_ORIG}
      curl -i -X PUT -H "Accept:application/json" -H  "Content-Type:application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${PUBLIC_INDEX_NAME_ORIG} -d @/elasticsearch-settings-public.json
      curl -i -X POST -H "Accept:application/json" -H "Content-Type: application/json" http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/_reindex?pretty -d"{\"source\": {\"index\": \"${PUBLIC_INDEX_NAME}\"}, \"dest\": {\"index\": \"${PUBLIC_INDEX_NAME_ORIG}\"}}"

      # Create permanent connectors
      export SINK_NAME="elastic-sink"
      export PUBLIC_SINK_NAME="elastic-sink-public"
      curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$DEBEZIUM_INDEX_NAME$SINK_NAME' < /elasticsearch-sink.json)
      curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/ -d @<(envsubst '$ELASTICSEARCH_HOST$ELASTICSEARCH_PORT$PUBLIC_INDEX_NAME$PUBLIC_SINK_NAME' < /elasticsearch-sink-public.json)
    fi

    # Clean up temporary connectors (only in production mode)
    curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink-temp
    curl -i -X DELETE http://${DEBEZIUM_CONNECTOR_HOST}:${DEBEZIUM_CONNECTOR_PORT}/connectors/elastic-sink-public-temp
fi