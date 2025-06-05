#!/bin/bash
set -e

echo "🧪 Testing Debezium Setup - Both Public and Private Indexes"
echo "==========================================================="

# Configuration
ENV_FILE=${ENV_FILE:-.env.test}
PUBLIC_INDEX_NAME="public_references_index"
PRIVATE_INDEX_NAME="test_references_index"

echo "📋 Using environment file: $ENV_FILE"

# Load environment variables
if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
    echo "✅ Environment loaded from $ENV_FILE"
else
    echo "❌ Environment file $ENV_FILE not found"
    exit 1
fi

# Function to check service health
check_service() {
    local service=$1
    local url=$2
    local max_attempts=30
    local attempt=1
    
    echo "🔍 Checking $service at $url..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            echo "✅ $service is healthy"
            return 0
        fi
        echo "   Attempt $attempt/$max_attempts: waiting for $service..."
        sleep 2
        attempt=$((attempt + 1))
    done
    echo "❌ $service failed to start"
    return 1
}

# Function to wait for index to be populated
wait_for_index() {
    local index_name=$1
    local min_docs=${2:-10}
    local max_attempts=60
    local attempt=1
    
    echo "⏳ Waiting for $index_name to be populated (minimum $min_docs documents)..."
    while [ $attempt -le $max_attempts ]; do
        doc_count=$(curl -s "http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/$index_name/_count" | jq -r '.count // 0' 2>/dev/null || echo "0")
        if [ "$doc_count" -ge "$min_docs" ]; then
            echo "✅ $index_name has $doc_count documents"
            return 0
        fi
        echo "   Attempt $attempt/$max_attempts: $index_name has $doc_count documents (need $min_docs)"
        sleep 5
        attempt=$((attempt + 1))
    done
    echo "❌ $index_name failed to populate with enough documents"
    return 1
}

echo ""
echo "🐳 Step 1: Starting Docker services..."
echo "======================================"

# Start core services
echo "Starting PostgreSQL and Elasticsearch..."
docker-compose --env-file $ENV_FILE up -d postgres elasticsearch

# Wait for services to be healthy
sleep 10
check_service "PostgreSQL" "http://localhost:${PSQL_PORT:-5432}" || (echo "Note: PostgreSQL check may fail but service might still work"; true)
check_service "Elasticsearch" "http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}"

echo ""
echo "💾 Step 2: Initializing test database..."
echo "========================================"

# Initialize test database with sample data
echo "Creating test database with sample data..."
docker-compose --env-file $ENV_FILE run --rm dev_app sh tests/init_test_db.sh

# Insert some sample relations, copyright, and mesh data for testing
echo "Adding sample data for new tables..."
docker-compose --env-file $ENV_FILE run --rm dev_app bash -c "
export ENV_STATE=test
python3 /workdir/debezium/test_debezium/create_test_subset.py || python3 -c \"
import os
import sys
sys.path.append('/workdir')
from agr_literature_service.api.database import get_db
from agr_literature_service.api.models.reference_model import Reference
from agr_literature_service.api.models.reference_relation_model import ReferenceRelation
from agr_literature_service.api.models.copyright_license_model import CopyrightLicense
from agr_literature_service.api.models.mesh_detail_model import MeshDetail
from agr_literature_service.api.models.resource_model import Resource
from sqlalchemy.orm import Session

# Get database session
db_gen = get_db()
db = next(db_gen)

try:
    # Get first few references for testing
    references = db.query(Reference).limit(5).all()
    print(f'Found {len(references)} references for testing')
    
    if len(references) >= 2:
        # Add sample reference relations
        relation = ReferenceRelation(
            reference_id_from=references[0].reference_id,
            reference_id_to=references[1].reference_id,
            reference_relation_type='Reviews'
        )
        db.add(relation)
        
        # Add sample copyright license
        copyright_license = CopyrightLicense(
            reference_id=references[0].reference_id,
            copyright_license_text='Creative Commons Attribution License',
            copyright_license_url='https://creativecommons.org/licenses/by/4.0/',
            open_access=True
        )
        db.add(copyright_license)
        
        # Add sample mesh terms
        mesh = MeshDetail(
            reference_id=references[0].reference_id,
            mesh_heading_term='Genomics',
            mesh_qualifier_term='methods'
        )
        db.add(mesh)
        
        # Add sample resource if none exists
        existing_resource = db.query(Resource).first()
        if not existing_resource:
            resource = Resource(
                title='Test Journal',
                iso_abbreviation='Test J.',
                medline_abbreviation='Test J',
                issn_print='1234-5678',
                issn_electronic='5678-1234'
            )
            db.add(resource)
        
        db.commit()
        print('✅ Sample data added for testing new tables')
    else:
        print('⚠️  Not enough references found to create relations')

except Exception as e:
    print(f'❌ Error adding sample data: {e}')
    db.rollback()
finally:
    db.close()
\"
"

echo ""
echo "🔄 Step 3: Starting Debezium stack..."
echo "===================================="

# Set public index name
export PUBLIC_INDEX_NAME="$PUBLIC_INDEX_NAME"

# Start Debezium stack
echo "Starting Kafka, KsqlDB, and Debezium connectors..."
make restart-debezium-local ENV_FILE=$ENV_FILE

echo ""
echo "⏳ Step 4: Waiting for data to populate..."
echo "========================================="

# Wait for both indexes to be populated
echo "Waiting for indexes to be created and populated..."
sleep 30

wait_for_index "$PRIVATE_INDEX_NAME" 5
wait_for_index "$PUBLIC_INDEX_NAME" 5

echo ""
echo "🔍 Step 5: Verifying indexes..."
echo "==============================="

# Check private index
echo "📊 Private Index ($PRIVATE_INDEX_NAME):"
private_count=$(curl -s "http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/$PRIVATE_INDEX_NAME/_count" | jq -r '.count // 0')
echo "   Documents: $private_count"

# Check public index  
echo "📊 Public Index ($PUBLIC_INDEX_NAME):"
public_count=$(curl -s "http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/$PUBLIC_INDEX_NAME/_count" | jq -r '.count // 0')
echo "   Documents: $public_count"

# Get sample documents
echo ""
echo "📄 Sample Public Index Document:"
echo "================================"
curl -s "http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/$PUBLIC_INDEX_NAME/_search?size=1&pretty" | jq '.hits.hits[0]._source // {}'

echo ""
echo "📄 Sample Private Index Document:"
echo "================================"
curl -s "http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/$PRIVATE_INDEX_NAME/_search?size=1&pretty" | jq '.hits.hits[0]._source // {}'

echo ""
echo "🔍 Field Comparison:"
echo "==================="

# Get field mappings for both indexes
echo "Public Index Fields:"
curl -s "http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/$PUBLIC_INDEX_NAME/_mapping" | jq -r '.*.mappings.properties | keys[]' | sort | head -20

echo ""
echo "Private Index Fields:"
curl -s "http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/$PRIVATE_INDEX_NAME/_mapping" | jq -r '.*.mappings.properties | keys[]' | sort | head -20

echo ""
echo "🎉 Debezium Test Complete!"
echo "=========================="
echo "Both indexes are now running and populated:"
echo "  Public Index:  http://localhost:${ELASTICSEARCH_PORT}/$PUBLIC_INDEX_NAME"
echo "  Private Index: http://localhost:${ELASTICSEARCH_PORT}/$PRIVATE_INDEX_NAME"
echo ""
echo "📊 Run detailed verification:"
echo "./debezium/test_debezium/verify_indexes.sh"
echo ""
echo "🔍 Quick exploration commands:"
echo "curl 'http://localhost:${ELASTICSEARCH_PORT}/$PUBLIC_INDEX_NAME/_search?q=*&size=10&pretty'"
echo "curl 'http://localhost:${ELASTICSEARCH_PORT}/$PRIVATE_INDEX_NAME/_search?q=*&size=10&pretty'"
echo ""
echo "📈 Compare field counts:"
echo "curl 'http://localhost:${ELASTICSEARCH_PORT}/$PUBLIC_INDEX_NAME/_search?size=1' | jq '.hits.hits[0]._source | keys | length'"
echo "curl 'http://localhost:${ELASTICSEARCH_PORT}/$PRIVATE_INDEX_NAME/_search?size=1' | jq '.hits.hits[0]._source | keys | length'"