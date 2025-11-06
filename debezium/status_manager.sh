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
                local avg_duration=$(jq -r '.averages.total_duration_seconds // 0' "${METRICS_FILE}")
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

    echo "Starting reindex from $source_index to $dest_index"

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

    echo "Reindex task created with ID: $task_id for $dest_index"
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
                local private_created=$(echo "$private_response" | jq -r '.task.status.created // 0')
                local private_total=$(echo "$private_response" | jq -r '.task.status.total // 0')
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
                local public_created=$(echo "$public_response" | jq -r '.task.status.created // 0')
                local public_total=$(echo "$public_response" | jq -r '.task.status.total // 0')
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

# Export functions for use in other scripts
export -f get_timestamp
export -f seconds_since
export -f set_reindex_status
export -f poll_reindex_task
export -f start_reindex
export -f poll_multiple_reindex_tasks
export -f save_completion_metrics