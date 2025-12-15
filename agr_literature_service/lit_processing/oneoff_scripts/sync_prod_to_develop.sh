#!/bin/bash
# Script to sync S3 bucket and/or database from prod to develop
# S3: Syncs all content from agr-literature/prod/ to agr-literature/develop/
# DB: Creates a full dump from source database and restores to destination

set -e

# S3 configuration
BUCKET="agr-literature"
SOURCE_PREFIX="prod/"
DEST_PREFIX="develop/"

# Temporary file for database dump
DUMP_FILE="/tmp/agr_literature_dump_$(date +%Y%m%d_%H%M%S).dump"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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

# Function to prompt for database credentials
get_db_credentials() {
    local db_type="$1"  # "source" or "destination"

    print_header "Enter ${db_type^^} Database Credentials"

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

    # Get source database credentials
    get_db_credentials "source"

    # Get destination database credentials
    get_db_credentials "destination"

    echo ""
    echo "=== Database Sync Configuration ==="
    echo "Source:      ${SOURCE_DB_USER}@${SOURCE_DB_HOST}:${SOURCE_DB_PORT}/${SOURCE_DB_NAME}"
    echo "Destination: ${DEST_DB_USER}@${DEST_DB_HOST}:${DEST_DB_PORT}/${DEST_DB_NAME}"
    echo "Dump file:   ${DUMP_FILE}"
    echo ""

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
        -Fc \
        --no-owner \
        --no-acl \
        -f "$DUMP_FILE"

    if [[ ! -f "$DUMP_FILE" ]]; then
        print_error "Failed to create database dump!"
        return 1
    fi

    DUMP_SIZE=$(du -h "$DUMP_FILE" | cut -f1)
    print_success "Dump created successfully! Size: ${DUMP_SIZE}"

    echo ""
    echo "Step 2/3: Dropping and recreating destination database schema..."

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

    PGPASSWORD="$DEST_DB_PASSWORD" pg_restore \
        -h "$DEST_DB_HOST" \
        -p "$DEST_DB_PORT" \
        -U "$DEST_DB_USER" \
        -d "$DEST_DB_NAME" \
        --no-owner \
        --no-acl \
        --single-transaction \
        "$DUMP_FILE" || true  # pg_restore may return non-zero even on success with warnings

    print_success "Database restore completed!"

    # Cleanup dump file
    echo ""
    read -p "Delete temporary dump file? (yes/no) [yes]: " cleanup
    cleanup=${cleanup:-yes}
    if [[ "$cleanup" == "yes" ]]; then
        rm -f "$DUMP_FILE"
        echo "Dump file deleted."
    else
        echo "Dump file kept at: ${DUMP_FILE}"
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

# Main menu
print_header "AGR Literature Service - Prod to Develop Sync"

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

echo ""
print_success "=== Sync operation finished ==="