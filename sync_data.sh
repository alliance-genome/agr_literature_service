#!/bin/bash
# Script to sync S3 bucket and/or database between environments
# S3: Syncs all content from agr-literature/prod/ to agr-literature/develop/
# DB: Creates a full dump from source database and restores to destination
#
# Usage:
#   ./sync_data.sh                          # Interactive mode
#   ./sync_data.sh --db-only                # Database sync only (skip menu)
#   ./sync_data.sh --s3-only                # S3 sync only (skip menu)
#   ./sync_data.sh --source name --dest name  # Use saved connections
#   ./sync_data.sh --list                   # List saved connections
#   ./sync_data.sh --delete name            # Delete a saved connection

set -e

# S3 configuration
BUCKET="agr-literature"
SOURCE_PREFIX="prod/"
DEST_PREFIX="develop/"

# Config file location
CONFIG_FILE="${HOME}/.agr_sync_config.json"

# Temporary target for the database dump. Parallel dumps require directory format
# (pg_dump -j only works with -Fd), so the target is a directory when JOBS > 1 and a
# single custom-format file otherwise. Both live under /tmp.
DUMP_STAMP="$(date +%Y%m%d_%H%M%S)"
DUMP_FILE="/tmp/agr_literature_dump_${DUMP_STAMP}.dump"
DUMP_DIR="/tmp/agr_literature_dump_${DUMP_STAMP}.dir"

# Parallel jobs for pg_dump/pg_restore, used by default (a plain `make sync-data`
# is parallel). Scales with the host but capped at 8: each job opens its own
# connection, and the source is often a shared or production server. Lower it with
# `--jobs 2`, or `--jobs 1` for the original serial single-file behaviour.
DEFAULT_JOBS=$( { nproc 2>/dev/null || echo 2; } | awk '{ j=$1+0; if (j>8) j=8; if (j<1) j=1; print j }' )
JOBS="$DEFAULT_JOBS"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# CLI argument variables
DB_ONLY=false
S3_ONLY=false
SOURCE_CONNECTION=""
DEST_CONNECTION=""
LIST_CONNECTIONS=false
DELETE_CONNECTION=""

print_header() {
    echo ""
    echo "=============================================="
    echo "$1"
    echo "=============================================="
    echo ""
}

print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

print_success() {
    echo -e "${GREEN}$1${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

print_info() {
    echo -e "${BLUE}$1${NC}"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --db-only)
                DB_ONLY=true
                shift
                ;;
            --s3-only)
                S3_ONLY=true
                shift
                ;;
            --source)
                SOURCE_CONNECTION="$2"
                shift 2
                ;;
            --dest|--destination)
                DEST_CONNECTION="$2"
                shift 2
                ;;
            --list)
                LIST_CONNECTIONS=true
                shift
                ;;
            --delete)
                DELETE_CONNECTION="$2"
                shift 2
                ;;
            --jobs|-j)
                if [[ ! "$2" =~ ^[0-9]+$ ]] || [[ "$2" -lt 1 ]]; then
                    print_error "--jobs requires a positive integer (got '${2:-}')"
                    exit 1
                fi
                JOBS="$2"
                shift 2
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

show_help() {
    echo "Usage: sync_data.sh [OPTIONS]"
    echo ""
    echo "Sync database and/or S3 bucket between environments."
    echo ""
    echo "Options:"
    echo "  --db-only              Skip menu, sync database only"
    echo "  --s3-only              Skip menu, sync S3 only"
    echo "  --source <name>        Use saved connection as source database"
    echo "  --dest <name>          Use saved connection as destination database"
    echo "  --list                 List all saved connections"
    echo "  --delete <name>        Delete a saved connection"
    echo "  --jobs, -j <n>         Parallel dump/restore jobs (default: ${DEFAULT_JOBS})"
    echo "                         n > 1 uses directory-format dumps (required by"
    echo "                         pg_dump -j); n = 1 uses a single custom-format file."
    echo "                         Each job opens its own connection to the server."
    echo "  --help, -h             Show this help message"
    echo ""
    echo "Examples:"
    echo "  sync_data.sh                                    # Interactive mode"
    echo "  sync_data.sh --db-only                          # Database sync only"
    echo "  sync_data.sh --db-only --source prod --dest dev # Use saved connections"
    echo "  sync_data.sh --db-only --jobs 8                 # Faster sync, 8 jobs"
    echo "  sync_data.sh --db-only --jobs 1                 # Serial (old behaviour)"
    echo "  sync_data.sh --list                             # List saved connections"
    echo ""
}

# Initialize config file if it doesn't exist
init_config() {
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo '{"connections": {}}' > "$CONFIG_FILE"
        chmod 600 "$CONFIG_FILE"
    fi
}

# Load a saved connection by name
# Returns: Sets DB_HOST, DB_PORT, DB_NAME, DB_USER variables
get_saved_connection() {
    local name="$1"
    init_config

    if ! command -v jq &> /dev/null; then
        print_error "jq is required for config file management. Please install it."
        return 1
    fi

    local conn=$(jq -r ".connections[\"$name\"] // empty" "$CONFIG_FILE")
    if [[ -z "$conn" ]]; then
        print_error "Connection '$name' not found in config file."
        return 1
    fi

    DB_HOST=$(echo "$conn" | jq -r '.host')
    DB_PORT=$(echo "$conn" | jq -r '.port')
    DB_NAME=$(echo "$conn" | jq -r '.name')
    DB_USER=$(echo "$conn" | jq -r '.user')

    return 0
}

# Save a connection to the config file
save_connection() {
    local name="$1"
    local host="$2"
    local port="$3"
    local dbname="$4"
    local user="$5"

    init_config

    if ! command -v jq &> /dev/null; then
        print_error "jq is required for config file management. Please install it."
        return 1
    fi

    local tmp_file=$(mktemp)
    jq ".connections[\"$name\"] = {\"host\": \"$host\", \"port\": \"$port\", \"name\": \"$dbname\", \"user\": \"$user\"}" \
        "$CONFIG_FILE" > "$tmp_file" && mv "$tmp_file" "$CONFIG_FILE"
    chmod 600 "$CONFIG_FILE"

    print_success "Connection '$name' saved to config file."
}

# Delete a saved connection
delete_connection() {
    local name="$1"
    init_config

    if ! command -v jq &> /dev/null; then
        print_error "jq is required for config file management. Please install it."
        return 1
    fi

    local conn=$(jq -r ".connections[\"$name\"] // empty" "$CONFIG_FILE")
    if [[ -z "$conn" ]]; then
        print_error "Connection '$name' not found in config file."
        return 1
    fi

    local tmp_file=$(mktemp)
    jq "del(.connections[\"$name\"])" "$CONFIG_FILE" > "$tmp_file" && mv "$tmp_file" "$CONFIG_FILE"
    chmod 600 "$CONFIG_FILE"

    print_success "Connection '$name' deleted from config file."
}

# List all saved connections
list_connections() {
    init_config

    if ! command -v jq &> /dev/null; then
        print_error "jq is required for config file management. Please install it."
        return 1
    fi

    local connections=$(jq -r '.connections | keys[]' "$CONFIG_FILE" 2>/dev/null)

    if [[ -z "$connections" ]]; then
        echo "No saved connections found."
        echo ""
        echo "Connections are saved to: $CONFIG_FILE"
        echo "You can save connections when entering database credentials interactively."
        return 0
    fi

    print_header "Saved Database Connections"
    echo "Config file: $CONFIG_FILE"
    echo ""

    while IFS= read -r name; do
        local conn=$(jq -r ".connections[\"$name\"]" "$CONFIG_FILE")
        local host=$(echo "$conn" | jq -r '.host')
        local port=$(echo "$conn" | jq -r '.port')
        local dbname=$(echo "$conn" | jq -r '.name')
        local user=$(echo "$conn" | jq -r '.user')
        printf "  %-20s %s@%s:%s/%s\n" "[$name]" "$user" "$host" "$port" "$dbname"
    done <<< "$connections"

    echo ""
    echo "Note: Passwords are not saved. You will be prompted for password when using saved connections."
}

# Get list of saved connection names for menu display
get_connection_names() {
    init_config
    if command -v jq &> /dev/null; then
        jq -r '.connections | keys[]' "$CONFIG_FILE" 2>/dev/null || true
    fi
}

# Function to prompt for database credentials
# Can use saved connection or prompt for new credentials
get_db_credentials() {
    local db_type="$1"  # "source" or "destination"
    local saved_name="$2"  # optional: name of saved connection to use

    # If a saved connection name was provided, use it
    if [[ -n "$saved_name" ]]; then
        print_info "Using saved connection: $saved_name"
        if get_saved_connection "$saved_name"; then
            if [[ "$db_type" == "source" ]]; then
                SOURCE_DB_HOST="$DB_HOST"
                SOURCE_DB_PORT="$DB_PORT"
                SOURCE_DB_NAME="$DB_NAME"
                SOURCE_DB_USER="$DB_USER"
                read -s -p "Password for ${SOURCE_DB_USER}@${SOURCE_DB_HOST}: " SOURCE_DB_PASSWORD
                echo ""
            else
                DEST_DB_HOST="$DB_HOST"
                DEST_DB_PORT="$DB_PORT"
                DEST_DB_NAME="$DB_NAME"
                DEST_DB_USER="$DB_USER"
                read -s -p "Password for ${DEST_DB_USER}@${DEST_DB_HOST}: " DEST_DB_PASSWORD
                echo ""
            fi
            return 0
        else
            return 1
        fi
    fi

    print_header "Enter ${db_type^^} Database Credentials"

    # Show saved connections if any exist
    local saved_connections=$(get_connection_names)
    if [[ -n "$saved_connections" ]]; then
        echo "Saved connections:"
        local i=1
        local conn_array=()
        while IFS= read -r name; do
            local conn=$(jq -r ".connections[\"$name\"]" "$CONFIG_FILE")
            local host=$(echo "$conn" | jq -r '.host')
            local port=$(echo "$conn" | jq -r '.port')
            local dbname=$(echo "$conn" | jq -r '.name')
            local user=$(echo "$conn" | jq -r '.user')
            printf "  %d) %-15s %s@%s:%s/%s\n" "$i" "[$name]" "$user" "$host" "$port" "$dbname"
            conn_array+=("$name")
            ((i++))
        done <<< "$saved_connections"
        echo "  $i) Enter new credentials"
        echo ""

        read -p "Select option [1-$i]: " selection

        if [[ "$selection" =~ ^[0-9]+$ ]] && [[ "$selection" -ge 1 ]] && [[ "$selection" -lt "$i" ]]; then
            local selected_name="${conn_array[$((selection-1))]}"
            get_db_credentials "$db_type" "$selected_name"
            return $?
        fi
        echo ""
    fi

    # Prompt for new credentials
    read -p "${db_type^} DB Host [localhost]: " db_host
    db_host=${db_host:-localhost}

    read -p "${db_type^} DB Port [5432]: " db_port
    db_port=${db_port:-5432}

    read -p "${db_type^} DB Name [literature]: " db_name
    db_name=${db_name:-literature}

    read -p "${db_type^} DB Username [postgres]: " db_user
    db_user=${db_user:-postgres}

    read -s -p "${db_type^} DB Password: " db_password
    echo ""

    # Export variables with prefix
    if [[ "$db_type" == "source" ]]; then
        SOURCE_DB_HOST="$db_host"
        SOURCE_DB_PORT="$db_port"
        SOURCE_DB_NAME="$db_name"
        SOURCE_DB_USER="$db_user"
        SOURCE_DB_PASSWORD="$db_password"
    else
        DEST_DB_HOST="$db_host"
        DEST_DB_PORT="$db_port"
        DEST_DB_NAME="$db_name"
        DEST_DB_USER="$db_user"
        DEST_DB_PASSWORD="$db_password"
    fi

    # Offer to save the connection (only if jq is available)
    if command -v jq &> /dev/null; then
        echo ""
        read -p "Save this connection? Enter a name (or press Enter to skip): " save_name
        if [[ -n "$save_name" ]]; then
            save_connection "$save_name" "$db_host" "$db_port" "$db_name" "$db_user"
        fi
    fi
}

# Function to test database connection
test_db_connection() {
    local host="$1"
    local port="$2"
    local dbname="$3"
    local user="$4"
    local password="$5"
    local label="$6"

    echo "Testing connection to ${label} database..."
    if PGPASSWORD="$password" psql -h "$host" -p "$port" -U "$user" -d "$dbname" -c "SELECT 1;" > /dev/null 2>&1; then
        print_success "Connection to ${label} database successful!"
        return 0
    else
        print_error "Failed to connect to ${label} database!"
        return 1
    fi
}

# Function to perform database sync
sync_database() {
    print_header "Database Sync: Source -> Destination"

    # Get source database credentials (using saved connection if provided)
    get_db_credentials "source" "$SOURCE_CONNECTION"

    # Get destination database credentials (using saved connection if provided)
    get_db_credentials "destination" "$DEST_CONNECTION"

    # Parallel dumps need directory format; a single job keeps the original
    # single-file custom format.
    if [[ "$JOBS" -gt 1 ]]; then
        DUMP_TARGET="$DUMP_DIR"
        DUMP_FORMAT_ARGS=( -Fd -j "$JOBS" )
        RESTORE_JOB_ARGS=( -j "$JOBS" )
        DUMP_DESC="directory format, ${JOBS} parallel jobs"
    else
        DUMP_TARGET="$DUMP_FILE"
        DUMP_FORMAT_ARGS=( -Fc )
        RESTORE_JOB_ARGS=()
        DUMP_DESC="custom format, single job"
    fi

    echo ""
    echo "=== Database Sync Configuration ==="
    echo "Source:      ${SOURCE_DB_USER}@${SOURCE_DB_HOST}:${SOURCE_DB_PORT}/${SOURCE_DB_NAME}"
    echo "Destination: ${DEST_DB_USER}@${DEST_DB_HOST}:${DEST_DB_PORT}/${DEST_DB_NAME}"
    echo "Dump target: ${DUMP_TARGET}"
    echo "Parallelism: ${DUMP_DESC}"
    echo ""

    # Check that source and destination are not the same database
    if [[ "$SOURCE_DB_HOST" == "$DEST_DB_HOST" ]] && \
       [[ "$SOURCE_DB_PORT" == "$DEST_DB_PORT" ]] && \
       [[ "$SOURCE_DB_NAME" == "$DEST_DB_NAME" ]]; then
        print_error "Source and destination are the same database!"
        print_error "Cannot sync a database to itself. Please use different source and destination."
        return 1
    fi

    # Test connections
    if ! test_db_connection "$SOURCE_DB_HOST" "$SOURCE_DB_PORT" "$SOURCE_DB_NAME" "$SOURCE_DB_USER" "$SOURCE_DB_PASSWORD" "source"; then
        print_error "Cannot proceed without source database connection."
        return 1
    fi

    if ! test_db_connection "$DEST_DB_HOST" "$DEST_DB_PORT" "$DEST_DB_NAME" "$DEST_DB_USER" "$DEST_DB_PASSWORD" "destination"; then
        print_error "Cannot proceed without destination database connection."
        return 1
    fi

    print_warning "This will REPLACE ALL DATA in the destination database!"
    print_warning "Database: ${DEST_DB_NAME} on ${DEST_DB_HOST}:${DEST_DB_PORT}"
    echo ""
    read -p "Are you sure you want to proceed? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        echo "Database sync aborted."
        return 1
    fi

    echo ""
    echo "Step 1/3: Creating dump from source database..."
    echo "This may take a while depending on database size..."
    PGPASSWORD="$SOURCE_DB_PASSWORD" pg_dump \
        -h "$SOURCE_DB_HOST" \
        -p "$SOURCE_DB_PORT" \
        -U "$SOURCE_DB_USER" \
        -d "$SOURCE_DB_NAME" \
        "${DUMP_FORMAT_ARGS[@]}" \
        --no-owner \
        --no-acl \
        --verbose \
        -f "$DUMP_TARGET"

    # -e, not -f: a directory-format dump is a directory, not a regular file
    if [[ ! -e "$DUMP_TARGET" ]]; then
        print_error "Failed to create database dump!"
        return 1
    fi

    DUMP_SIZE=$(du -sh "$DUMP_TARGET" | cut -f1)
    print_success "Dump created successfully! Size: ${DUMP_SIZE}"

    echo ""
    echo "Step 2/3: Dropping and recreating destination database schema..."

    # Drop publications (they exist at database level, not schema level)
    echo "Dropping existing publications..."
    PGPASSWORD="$DEST_DB_PASSWORD" psql \
        -h "$DEST_DB_HOST" \
        -p "$DEST_DB_PORT" \
        -U "$DEST_DB_USER" \
        -d "$DEST_DB_NAME" \
        -c "DO \$\$ DECLARE pub RECORD; BEGIN FOR pub IN SELECT pubname FROM pg_publication LOOP EXECUTE 'DROP PUBLICATION IF EXISTS ' || quote_ident(pub.pubname); END LOOP; END \$\$;"

    # Drop all tables and recreate schema
    PGPASSWORD="$DEST_DB_PASSWORD" psql \
        -h "$DEST_DB_HOST" \
        -p "$DEST_DB_PORT" \
        -U "$DEST_DB_USER" \
        -d "$DEST_DB_NAME" \
        -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

    print_success "Destination schema cleared!"

    echo ""
    echo "Step 3/3: Restoring dump to destination database..."
    echo "This may take a while..."

    RESTORE_EXIT_CODE=0
    PGPASSWORD="$DEST_DB_PASSWORD" pg_restore \
        -h "$DEST_DB_HOST" \
        -p "$DEST_DB_PORT" \
        -U "$DEST_DB_USER" \
        -d "$DEST_DB_NAME" \
        "${RESTORE_JOB_ARGS[@]}" \
        --no-owner \
        --no-acl \
        --verbose \
        "$DUMP_TARGET" || RESTORE_EXIT_CODE=$?

    if [[ $RESTORE_EXIT_CODE -ne 0 ]]; then
        print_warning "pg_restore exited with code ${RESTORE_EXIT_CODE} (may include warnings)"
    fi

    # Verify restore by checking table counts
    echo ""
    echo "Verifying restore - checking table row counts..."
    PGPASSWORD="$DEST_DB_PASSWORD" psql \
        -h "$DEST_DB_HOST" \
        -p "$DEST_DB_PORT" \
        -U "$DEST_DB_USER" \
        -d "$DEST_DB_NAME" \
        -c "SELECT schemaname, relname as table_name, n_live_tup as row_count FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 20;"

    print_success "Database restore completed!"

    # Cleanup dump (a file for -Fc, a directory for -Fd)
    echo ""
    read -p "Delete temporary dump? (yes/no) [yes]: " cleanup
    cleanup=${cleanup:-yes}
    if [[ "$cleanup" == "yes" ]]; then
        rm -rf "$DUMP_TARGET"
        echo "Dump deleted."
    else
        echo "Dump kept at: ${DUMP_TARGET}"
    fi

    print_success "=== Database sync complete ==="
}

# Function to perform S3 sync
sync_s3() {
    print_header "S3 Bucket Sync: prod -> develop"

    echo "Bucket: s3://${BUCKET}"
    echo "Source: ${SOURCE_PREFIX}"
    echo "Destination: ${DEST_PREFIX}"
    echo ""

    echo "Select S3 sync option:"
    echo "  1) Dry run (preview changes without executing)"
    echo "  2) Execute (perform the actual sync)"
    echo "  3) Skip S3 sync"
    read -p "Enter choice [1/2/3]: " s3_choice

    case "$s3_choice" in
        1)
            echo ""
            echo "=== DRY RUN MODE ==="
            echo "Previewing sync from s3://${BUCKET}/${SOURCE_PREFIX} to s3://${BUCKET}/${DEST_PREFIX}..."
            echo ""
            aws s3 sync "s3://${BUCKET}/${SOURCE_PREFIX}" "s3://${BUCKET}/${DEST_PREFIX}" --delete --dryrun
            echo ""
            print_success "=== Dry run complete (no changes made) ==="
            ;;
        2)
            print_warning "This will SYNC and DELETE files not in source."
            read -p "Are you sure? (yes/no): " confirm
            if [[ "$confirm" != "yes" ]]; then
                echo "S3 sync aborted."
                return 1
            fi
            echo ""
            echo "Syncing s3://${BUCKET}/${SOURCE_PREFIX} to s3://${BUCKET}/${DEST_PREFIX}..."
            echo "Files in destination that don't exist in source will be deleted."
            echo ""
            aws s3 sync "s3://${BUCKET}/${SOURCE_PREFIX}" "s3://${BUCKET}/${DEST_PREFIX}" --delete
            echo ""
            print_success "=== S3 sync complete ==="
            ;;
        3|*)
            echo "S3 sync skipped."
            ;;
    esac
}

# Main execution
main() {
    # Parse command line arguments
    parse_args "$@"

    # Handle --list command
    if [[ "$LIST_CONNECTIONS" == true ]]; then
        list_connections
        exit 0
    fi

    # Handle --delete command
    if [[ -n "$DELETE_CONNECTION" ]]; then
        delete_connection "$DELETE_CONNECTION"
        exit 0
    fi

    # Validate mutually exclusive options
    if [[ "$DB_ONLY" == true ]] && [[ "$S3_ONLY" == true ]]; then
        print_error "--db-only and --s3-only cannot be used together"
        exit 1
    fi

    # Check if saved connections are specified but sync type is not
    if [[ -n "$SOURCE_CONNECTION" || -n "$DEST_CONNECTION" ]]; then
        if [[ "$DB_ONLY" != true ]] && [[ "$S3_ONLY" != true ]]; then
            print_warning "Using saved connections implies database sync. Adding --db-only."
            DB_ONLY=true
        fi
    fi

    print_header "AGR Literature Service - Data Sync Tool"

    # Determine what to sync
    if [[ "$DB_ONLY" == true ]]; then
        sync_database
    elif [[ "$S3_ONLY" == true ]]; then
        sync_s3
    else
        # Interactive menu
        echo "What would you like to sync?"
        echo "  1) Database only"
        echo "  2) S3 bucket only"
        echo "  3) All (Database + S3)"
        echo "  4) Abort"
        echo ""
        read -p "Enter choice [1/2/3/4]: " main_choice

        case "$main_choice" in
            1)
                sync_database
                ;;
            2)
                sync_s3
                ;;
            3)
                echo ""
                echo "Starting full sync (Database + S3)..."
                sync_database
                if [[ $? -eq 0 ]]; then
                    sync_s3
                else
                    print_warning "Database sync failed or was aborted. Skipping S3 sync."
                fi
                ;;
            4|*)
                echo "Aborted."
                exit 0
                ;;
        esac
    fi

    echo ""
    print_success "=== Sync operation finished ==="
}

# Run main function with all arguments
main "$@"
