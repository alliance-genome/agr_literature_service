#!/bin/bash

# Status file paths
STATUS_DIR="/var/lib/debezium_status"
STATUS_FILE="${STATUS_DIR}/reindex_status.json"
METRICS_FILE="${STATUS_DIR}/reindex_metrics.json"

# Ensure status directory exists
mkdir -p "${STATUS_DIR}"

# Function to get current timestamp in ISO 8601 format
get_timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

# Function to calculate seconds since a timestamp
seconds_since() {
    local start_time=$1
    local start_epoch=$(date -d "$start_time" +%s 2>/dev/null || echo "0")
    local current_epoch=$(date +%s)
    echo $((current_epoch - start_epoch))
}

# Function to set reindexing status
set_reindex_status() {
    local phase=$1
    local extra_data=${2:-"{}"}

    local timestamp=$(get_timestamp)

    # If this is the start, initialize the status
    if [[ "$phase" == "setup" ]]; then
        cat > "${STATUS_FILE}" <<EOF
{
  "is_reindexing": true,
  "phase": "$phase",
  "started_at": "$timestamp",
  "current_phase_started_at": "$timestamp",
  "estimated_completion_at": null,
  "progress_percentage": 0,
  "phase_details": $extra_data
}
EOF
    else
        # Update existing status
        if [[ -f "${STATUS_FILE}" ]]; then
            local started_at=$(jq -r '.started_at' "${STATUS_FILE}")
            local progress=0
            local eta=null

            # Calculate progress based on phase
            case "$phase" in
                "data_processing")
                    progress=20
                    ;;
                "reindexing")
                    progress=60
                    ;;
                "completed")
                    progress=100
                    ;;
            esac

            # Calculate ETA if we have historical data
            if [[ -f "${METRICS_FILE}" ]] && [[ "$phase" != "completed" ]]; then
                # Get average duration and convert to integer (floor)
                local avg_duration=$(jq -r '.averages.total_duration_seconds // 0 | floor' "${METRICS_FILE}")
                if [[ "$avg_duration" -gt 0 ]]; then
                    local elapsed=$(seconds_since "$started_at")
                    local remaining=$((avg_duration - elapsed))
                    if [[ $remaining -gt 0 ]]; then
                        eta=$(date -u -d "@$(($(date +%s) + remaining))" +"%Y-%m-%dT%H:%M:%SZ")
                        # Recalculate progress based on time elapsed
                        progress=$((elapsed * 100 / avg_duration))
                        if [[ $progress -gt 95 ]] && [[ "$phase" != "completed" ]]; then
                            progress=95
                        fi
                    fi
                fi
            fi

            cat > "${STATUS_FILE}" <<EOF
{
  "is_reindexing": $([ "$phase" != "completed" ] && echo "true" || echo "false"),
  "phase": "$phase",
  "started_at": "$started_at",
  "current_phase_started_at": "$timestamp",
  "estimated_completion_at": $([[ "$eta" != "null" ]] && echo "\"$eta\"" || echo "null"),
  "progress_percentage": $progress,
  "phase_details": $extra_data
}
EOF
        fi
    fi

    chmod 644 "${STATUS_FILE}"
}

# Function to poll Elasticsearch reindex task
poll_reindex_task() {
    local task_id=$1
    local es_host=$2
    local es_port=$3
    local index_name=$4

    echo "Polling reindex task: $task_id for index: $index_name"

    local max_attempts=7200  # 10 hours max (5 second intervals)
    local attempt=0

    while [[ $attempt -lt $max_attempts ]]; do
        sleep 5

        # Check task status
        local task_response=$(curl -s "http://${es_host}:${es_port}/_tasks/${task_id}")
        local completed=$(echo "$task_response" | jq -r '.completed // false')

        if [[ "$completed" == "true" ]]; then
            echo "Reindex task completed for $index_name"
            return 0
        fi

        # Get progress information if available
        local created=$(echo "$task_response" | jq -r '.task.status.created // 0')
        local total=$(echo "$task_response" | jq -r '.task.status.total // 0')

        if [[ $total -gt 0 ]]; then
            local task_progress=$((created * 100 / total))
            echo "Reindex progress for $index_name: $created/$total documents ($task_progress%)"

            # Update status with reindex progress (60-90% of overall progress)
            local overall_progress=$((60 + task_progress * 30 / 100))
            local phase_details=$(cat <<EOF
{
  "reindex_task_id": "$task_id",
  "documents_processed": $created,
  "total_documents": $total,
  "index_name": "$index_name"
}
EOF
)
            set_reindex_status "reindexing" "$phase_details"
        fi

        attempt=$((attempt + 1))
    done

    echo "Warning: Reindex task polling timed out for $index_name"
    return 1
}

# Function to execute reindex and return task ID (non-blocking)
start_reindex() {
    local es_host=$1
    local es_port=$2
    local source_index=$3
    local dest_index=$4

    # Log to stderr to avoid polluting return value
    echo "Starting reindex from $source_index to $dest_index" >&2

    # Execute reindex and capture the task ID
    local reindex_response=$(curl -s -X POST \
        -H "Accept: application/json" \
        -H "Content-Type: application/json" \
        "http://${es_host}:${es_port}/_reindex?wait_for_completion=false" \
        -d "{\"source\": {\"index\": \"${source_index}\"}, \"dest\": {\"index\": \"${dest_index}\"}}")

    local task_id=$(echo "$reindex_response" | jq -r '.task')

    if [[ -z "$task_id" ]] || [[ "$task_id" == "null" ]]; then
        echo "Error: Failed to get task ID for reindex. Response: $reindex_response" >&2
        echo "null"
        return 1
    fi

    echo "Reindex task created with ID: $task_id for $dest_index" >&2

    # Wait briefly and verify task is queryable
    sleep 1
    local task_check=$(curl -s "http://${es_host}:${es_port}/_tasks/${task_id}")
    local task_exists=$(echo "$task_check" | jq -r 'has("task") or has("completed")')

    if [[ "$task_exists" != "true" ]]; then
        echo "Warning: Task $task_id was created but not immediately queryable. Waiting..." >&2
        sleep 2
        # Try one more time
        task_check=$(curl -s "http://${es_host}:${es_port}/_tasks/${task_id}")
        task_exists=$(echo "$task_check" | jq -r 'has("task") or has("completed")')
        if [[ "$task_exists" != "true" ]]; then
            echo "Error: Task $task_id is not queryable after waiting" >&2
            echo "null"
            return 1
        fi
    fi

    # Return only the task ID to stdout
    echo "$task_id"
    return 0
}

# Function to poll multiple reindex tasks in parallel
poll_multiple_reindex_tasks() {
    local es_host=$1
    local es_port=$2
    local private_task_id=$3
    local private_index=$4
    local public_task_id=$5
    local public_index=$6

    echo "Polling both reindex tasks in parallel..."
    echo "  Private index task: $private_task_id"
    echo "  Public index task: $public_task_id"

    local max_attempts=7200  # 10 hours max (5 second intervals)
    local attempt=0
    local private_completed=false
    local public_completed=false

    # Initialize progress tracking variables
    local private_created=0
    local private_total=0
    local public_created=0
    local public_total=0

    while [[ $attempt -lt $max_attempts ]]; do
        sleep 5

        # Check private index task if not completed
        if [[ "$private_completed" == "false" ]]; then
            local private_response=$(curl -s "http://${es_host}:${es_port}/_tasks/${private_task_id}")
            local private_done=$(echo "$private_response" | jq -r '.completed // false')

            if [[ "$private_done" == "true" ]]; then
                echo "Private index reindex completed"
                private_completed=true
            else
                private_created=$(echo "$private_response" | jq -r '.task.status.created // 0')
                private_total=$(echo "$private_response" | jq -r '.task.status.total // 0')
                if [[ $private_total -gt 0 ]]; then
                    local private_pct=$((private_created * 100 / private_total))
                    echo "Private index progress: $private_created/$private_total ($private_pct%)"
                fi
            fi
        fi

        # Check public index task if not completed
        if [[ "$public_completed" == "false" ]]; then
            local public_response=$(curl -s "http://${es_host}:${es_port}/_tasks/${public_task_id}")
            local public_done=$(echo "$public_response" | jq -r '.completed // false')

            if [[ "$public_done" == "true" ]]; then
                echo "Public index reindex completed"
                public_completed=true
            else
                public_created=$(echo "$public_response" | jq -r '.task.status.created // 0')
                public_total=$(echo "$public_response" | jq -r '.task.status.total // 0')
                if [[ $public_total -gt 0 ]]; then
                    local public_pct=$((public_created * 100 / public_total))
                    echo "Public index progress: $public_created/$public_total ($public_pct%)"
                fi
            fi
        fi

        # Update overall progress based on both tasks
        if [[ "$private_completed" == "false" ]] || [[ "$public_completed" == "false" ]]; then
            # Calculate combined progress (average of both)
            local combined_progress=0
            if [[ $private_total -gt 0 ]] && [[ $public_total -gt 0 ]]; then
                local private_task_pct=$((private_created * 100 / private_total))
                local public_task_pct=$((public_created * 100 / public_total))
                combined_progress=$(( (private_task_pct + public_task_pct) / 2 ))

                # Map to 60-95% of overall progress
                local overall_progress=$((60 + combined_progress * 35 / 100))

                local phase_details=$(cat <<EOF
{
  "private_task_id": "$private_task_id",
  "public_task_id": "$public_task_id",
  "private_docs_processed": $private_created,
  "private_total_docs": $private_total,
  "public_docs_processed": $public_created,
  "public_total_docs": $public_total
}
EOF
)
                set_reindex_status "reindexing" "$phase_details"
            fi
        fi

        # Both completed, exit
        if [[ "$private_completed" == "true" ]] && [[ "$public_completed" == "true" ]]; then
            echo "Both reindex tasks completed successfully"
            return 0
        fi

        attempt=$((attempt + 1))
    done

    echo "Warning: Reindex task polling timed out"
    return 1
}

# Function to save metrics after completion
save_completion_metrics() {
    local started_at=$1
    local setup_duration=$2
    local data_processing_duration=$3
    local reindex_duration=$4
    local doc_count=$5

    local completed_at=$(get_timestamp)
    local total_duration=$((setup_duration + data_processing_duration + reindex_duration))

    # Initialize metrics file if it doesn't exist
    if [[ ! -f "${METRICS_FILE}" ]]; then
        cat > "${METRICS_FILE}" <<EOF
{
  "completed_runs": [],
  "averages": {
    "total_duration_seconds": 0,
    "reindex_duration_seconds": 0
  }
}
EOF
    fi

    # Add new run to completed_runs
    local new_run=$(cat <<EOF
{
  "completed_at": "$completed_at",
  "total_duration_seconds": $total_duration,
  "phase_durations": {
    "setup": $setup_duration,
    "data_processing": $data_processing_duration,
    "reindexing": $reindex_duration
  },
  "document_count": $doc_count
}
EOF
)

    # Update metrics file with new run and recalculate averages
    local temp_file="${METRICS_FILE}.tmp"
    jq --argjson new_run "$new_run" '
        .completed_runs += [$new_run] |
        .completed_runs |= (if length > 10 then .[1:] else . end) |
        .averages.total_duration_seconds = ([.completed_runs[].total_duration_seconds] | add / length) |
        .averages.reindex_duration_seconds = ([.completed_runs[].phase_durations.reindexing] | add / length)
    ' "${METRICS_FILE}" > "$temp_file" && mv "$temp_file" "${METRICS_FILE}"

    chmod 644 "${METRICS_FILE}"
}

# Gate 1 (SCRUM-6231): wait for the Debezium snapshot to create the source topics, instead of a
# blind ${KSQL_SETUP_SLEEP}s wait, so ksql CTAS submission starts as soon as the topics its sources
# reference exist. ksql submits with auto.offset.reset=earliest, so it consumes the full snapshot as
# it streams in -- it only needs the topics to EXIST, not the snapshot to be finalized. Signal
# (HTTP-only -- the dbz_setup container has just curl/jq): the source connector's produced-topic set
# via Kafka Connect topic tracking (GET /connectors/<c>/topics); a topic appears once the connector
# has produced its first record for that table.
#   - expected_count > 0: proceed once produced topics >= expected_count and the connector is RUNNING.
#   - expected_count == 0 (undeterminable): proceed once the topic set is non-empty and unchanged for
#     DBZ_SNAPSHOT_STABLE_POLLS consecutive polls.
# Capped at max_wait; on cap it returns 1 and the caller proceeds anyway (matching the old fixed
# sleep -- submit_ksql_statements still retries on transient dependency errors).
wait_for_source_topics_ready() {
    local connect_host=$1
    local connect_port=$2
    local connector=$3
    local expected_count=$4
    local max_wait=$5

    local interval="${DBZ_SNAPSHOT_POLL_INTERVAL:-10}"
    local stable_needed="${DBZ_SNAPSHOT_STABLE_POLLS:-9}"   # ~90s of no new topics (fallback mode)
    local deadline=$(( $(date +%s) + max_wait ))
    local prev=-1 stable=0 count state resp

    [[ "$expected_count" =~ ^[0-9]+$ ]] || expected_count=0
    echo "Gate 1: waiting for Debezium snapshot topics (connector=$connector, expected>=${expected_count}, cap=${max_wait}s, interval=${interval}s)..."
    while [[ $(date +%s) -lt $deadline ]]; do
        resp=$(curl -s --max-time 10 "http://${connect_host}:${connect_port}/connectors/${connector}/topics")
        count=$(echo "$resp" | jq -r --arg c "$connector" '.[$c].topics | length' 2>/dev/null)
        [[ "$count" =~ ^[0-9]+$ ]] || count=0
        resp=$(curl -s --max-time 10 "http://${connect_host}:${connect_port}/connectors/${connector}/status")
        state=$(echo "$resp" | jq -r '.connector.state // "UNKNOWN"' 2>/dev/null)

        if [[ "$count" -gt 0 && "$count" -eq "$prev" ]]; then
            stable=$((stable + 1))
        else
            stable=0
        fi
        echo "  Gate 1: ${count} source topics (state=${state}, stable=${stable}/${stable_needed})"

        if [[ "$state" == "RUNNING" && "$count" -gt 0 ]]; then
            if [[ "$expected_count" -gt 0 && "$count" -ge "$expected_count" ]]; then
                echo "Gate 1: ${count} topics present (>= ${expected_count}); proceeding to ksql setup."
                return 0
            fi
            if [[ "$expected_count" -eq 0 && "$stable" -ge "$stable_needed" ]]; then
                echo "Gate 1: source topics settled at ${count}; proceeding to ksql setup."
                return 0
            fi
        fi
        prev=$count
        sleep "$interval"
    done
    echo "Gate 1: WARNING cap of ${max_wait}s reached (topics=${prev}); proceeding anyway."
    return 1
}

# Gate 2 (SCRUM-6231): wait for the reindex pipeline to DRAIN, instead of a blind ~${DATA_PROCESSING_SLEEP}s
# wait, before promoting temp -> final. Doc COUNT alone is not a valid signal (the ES sink upserts by
# doc id, so re-emitted joined objects overwrite docs and leave the count flat while indexing
# continues). Track THREE metrics per temp index from one _stats call: docs.count, store.size_in_bytes,
# and index_total (cumulative indexing ops, incl. overwrites). "Drained" = ALL of them, on BOTH
# indexes, STRICTLY UNCHANGED for DBZ_DRAIN_STABLE_SECONDS (default 600s = 10 min), with both indexes
# non-empty AND both ES sink connectors RUNNING with no FAILED tasks (so "stable" can't mean a dead
# sink or a not-yet-started load). Strict no-change is intentional: if live CDC keeps it moving, the
# max_wait cap (DBZ_DATA_PROCESSING_SLEEP) is the hard fallback. HTTP-only.
wait_for_pipeline_drained() {
    local es_host=$1 es_port=$2 private_index=$3 public_index=$4
    local connect_host=$5 connect_port=$6 sink=$7 public_sink=$8 max_wait=$9

    local interval="${DBZ_DRAIN_POLL_INTERVAL:-30}"
    local stable_seconds="${DBZ_DRAIN_STABLE_SECONDS:-600}"   # require 10 min of no change by default
    # (10 min, not 5: a dev run showed a ~4.5-min lull between the final reindex batches that nearly
    #  tripped a 5-min window prematurely; 10 min clears the observed lull. Capped by max_wait.)
    local deadline=$(( $(date +%s) + max_wait ))
    local last_change=$(date +%s) prev=""
    local stats_q='"\(._all.primaries.docs.count):\(._all.primaries.store.size_in_bytes):\(._all.primaries.indexing.index_total)"'
    local health_q='((.connector.state // "") == "RUNNING") and ((.tasks // []) | all(.state == "RUNNING"))'

    echo "Gate 2: waiting for both temp indexes to be STABLE (docs.count + store.size_in_bytes + index_total"
    echo "Gate 2: strictly unchanged) for ${stable_seconds}s, sinks healthy. poll=${interval}s cap=${max_wait}s."
    while [[ $(date +%s) -lt $deadline ]]; do
        sleep "$interval"

        local ps=$(curl -s --max-time 15 "http://${es_host}:${es_port}/${private_index}/_stats")
        local us=$(curl -s --max-time 15 "http://${es_host}:${es_port}/${public_index}/_stats")
        local psig=$(echo "$ps" | jq -r "$stats_q" 2>/dev/null)
        local usig=$(echo "$us" | jq -r "$stats_q" 2>/dev/null)
        local pdocs=$(echo "$ps" | jq -r '._all.primaries.docs.count // 0' 2>/dev/null); pdocs=${pdocs:-0}
        local udocs=$(echo "$us" | jq -r '._all.primaries.docs.count // 0' 2>/dev/null); udocs=${udocs:-0}

        local ss=$(curl -s --max-time 10 "http://${connect_host}:${connect_port}/connectors/${sink}/status")
        local us2=$(curl -s --max-time 10 "http://${connect_host}:${connect_port}/connectors/${public_sink}/status")
        local sink_ok=$(echo "$ss" | jq -r "$health_q" 2>/dev/null)
        local psink_ok=$(echo "$us2" | jq -r "$health_q" 2>/dev/null)

        local sig="P=${psig} U=${usig}"
        local now=$(date +%s)
        # reset the 5-min timer if anything changed, or either index is still empty (load not done starting)
        if [[ "$sig" != "$prev" ]] || [[ "$pdocs" -le 0 ]] || [[ "$udocs" -le 0 ]]; then
            last_change=$now
            prev="$sig"
        fi
        local stable_for=$(( now - last_change ))
        echo "  Gate 2: ${sig} sinks=[priv:${sink_ok:-?},pub:${psink_ok:-?}] stable_for=${stable_for}s/${stable_seconds}s"

        if [[ "$sink_ok" == "true" && "$psink_ok" == "true" \
              && "$pdocs" -gt 0 && "$udocs" -gt 0 \
              && "$stable_for" -ge "$stable_seconds" ]]; then
            echo "Gate 2: both indexes stable for ${stable_for}s and sinks healthy; proceeding to promotion."
            return 0
        fi
    done
    echo "Gate 2: WARNING cap of ${max_wait}s reached; proceeding to promotion check anyway."
    return 1
}

# ---------------------------------------------------------------------------
# SCRUM-6240: alias-based blue/green index swap helpers.
# The app queries an ALIAS; physical data lives in two fixed slots <name>_1 / <name>_2.
# Each rebuild targets the inactive slot, optimizes it, then atomically flips the alias.
# ---------------------------------------------------------------------------

# Echo the INACTIVE slot suffix ("1" or "2") for an alias, so a rebuild always targets the slot
# the alias is NOT currently serving. Defaults to "1" when the alias is absent (bootstrap) or
# points at neither slot.
resolve_inactive_suffix() {
    local es_host=$1 es_port=$2 alias=$3
    local current
    current=$(curl -s --max-time 10 "http://${es_host}:${es_port}/_alias/${alias}" 2>/dev/null \
              | jq -r 'if type=="object" and (has("error")|not) then (keys[0] // "") else "" end' 2>/dev/null)
    if [[ "$current" == "${alias}_1" ]]; then
        echo "2"
    else
        echo "1"
    fi
}

# Force-merge an index to a single segment (optimize + expunge upsert tombstones) and warm it
# (page-cache + global ordinals) BEFORE it serves traffic. Runs off the serving path (the alias
# still points at the old slot at this point).
optimize_and_warm_index() {
    local es_host=$1 es_port=$2 index=$3
    echo "Optimizing ${index} (force_merge max_num_segments=1)..."
    curl -s -X POST "http://${es_host}:${es_port}/${index}/_forcemerge?max_num_segments=1" \
        -H "Content-Type: application/json" >/dev/null
    echo "Warming ${index} (match_all to page in caches)..."
    curl -s -X POST "http://${es_host}:${es_port}/${index}/_search" \
        -H "Content-Type: application/json" -d '{"size":0,"query":{"match_all":{}}}' >/dev/null
}

# Atomically point <alias> at <new_index>, removing it from any other index it currently
# references (single _aliases call = zero-downtime cutover). Bootstrap: if a CONCRETE index
# literally named <alias> exists (pre-migration), delete it first so the alias name is free
# (Elasticsearch forbids an alias and an index sharing a name).
flip_alias() {
    local es_host=$1 es_port=$2 alias=$3 new_index=$4
    local base="http://${es_host}:${es_port}"
    local current_list concrete idx actions=""
    current_list=$(curl -s "${base}/_alias/${alias}" 2>/dev/null \
                   | jq -r 'if type=="object" and (has("error")|not) then keys[] else empty end' 2>/dev/null)
    concrete=$(curl -s "${base}/_cat/indices/${alias}?h=index" 2>/dev/null | tr -d '[:space:]')
    if [[ "$concrete" == "$alias" ]]; then
        echo "Bootstrap: deleting legacy concrete index '${alias}' to free the alias name"
        curl -s -X DELETE "${base}/${alias}" >/dev/null
        current_list=""
    fi
    for idx in $current_list; do
        [[ "$idx" == "$new_index" ]] && continue
        actions="${actions}{\"remove\":{\"index\":\"${idx}\",\"alias\":\"${alias}\"}},"
    done
    actions="${actions}{\"add\":{\"index\":\"${new_index}\",\"alias\":\"${alias}\"}}"
    echo "Flipping alias '${alias}' -> '${new_index}'"
    curl -s -X POST "${base}/_aliases" -H "Content-Type: application/json" \
        -d "{\"actions\":[${actions}]}"
    echo
}

# Export functions for use in other scripts
export -f get_timestamp
export -f seconds_since
export -f set_reindex_status
export -f poll_reindex_task
export -f start_reindex
export -f poll_multiple_reindex_tasks
export -f save_completion_metrics
export -f wait_for_source_topics_ready
export -f wait_for_pipeline_drained
export -f resolve_inactive_suffix
export -f optimize_and_warm_index
export -f flip_alias