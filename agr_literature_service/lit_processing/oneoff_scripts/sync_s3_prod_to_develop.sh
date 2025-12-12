#!/bin/bash
# Script to sync S3 bucket from prod to develop
# Syncs all content from agr-literature/prod/ to agr-literature/develop/
# and deletes any files in develop/ that don't exist in prod/

set -e

BUCKET="agr-literature"
SOURCE_PREFIX="prod/"
DEST_PREFIX="develop/"

echo "=== S3 Bucket Sync: prod -> develop ==="
echo "Bucket: s3://${BUCKET}"
echo "Source: ${SOURCE_PREFIX}"
echo "Destination: ${DEST_PREFIX}"
echo ""

# Dry run or real execution prompt
echo "Select an option:"
echo "  1) Dry run (preview changes without executing)"
echo "  2) Execute (perform the actual sync)"
echo "  3) Abort"
read -p "Enter choice [1/2/3]: " choice

case "$choice" in
    1)
        echo ""
        echo "=== DRY RUN MODE ==="
        echo "Previewing sync from s3://${BUCKET}/${SOURCE_PREFIX} to s3://${BUCKET}/${DEST_PREFIX}..."
        echo ""
        aws s3 sync "s3://${BUCKET}/${SOURCE_PREFIX}" "s3://${BUCKET}/${DEST_PREFIX}" --delete --dryrun
        echo ""
        echo "=== Dry run complete (no changes made) ==="
        ;;
    2)
        read -p "This will SYNC and DELETE files not in source. Are you sure? (yes/no): " confirm
        if [[ "$confirm" != "yes" ]]; then
            echo "Aborted."
            exit 1
        fi
        echo ""
        echo "Syncing s3://${BUCKET}/${SOURCE_PREFIX} to s3://${BUCKET}/${DEST_PREFIX}..."
        echo "Files in destination that don't exist in source will be deleted."
        echo ""
        aws s3 sync "s3://${BUCKET}/${SOURCE_PREFIX}" "s3://${BUCKET}/${DEST_PREFIX}" --delete
        echo ""
        echo "=== Sync complete ==="
        ;;
    3|*)
        echo "Aborted."
        exit 1
        ;;
esac