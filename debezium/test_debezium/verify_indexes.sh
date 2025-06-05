#!/bin/bash

# Load environment
ENV_FILE=${ENV_FILE:-.env.test}
if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
fi

PUBLIC_INDEX=${PUBLIC_INDEX_NAME:-public_references_index}
PRIVATE_INDEX=${DEBEZIUM_INDEX_NAME:-test_references_index}
ES_URL="http://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}"

echo "🔍 Index Verification Tool"
echo "=========================="
echo "Public Index: $PUBLIC_INDEX"
echo "Private Index: $PRIVATE_INDEX"
echo "Elasticsearch: $ES_URL"
echo ""

# Function to run query and format output
run_query() {
    local index=$1
    local query=$2
    local description=$3
    
    echo "📊 $description"
    echo "Index: $index"
    echo "Query: $query"
    echo "Response:"
    curl -s "$ES_URL/$index/_search?$query&pretty" | jq '.hits.hits[0]._source // {}'
    echo ""
    echo "---"
    echo ""
}

# Function to compare field presence
compare_fields() {
    echo "🔄 Field Comparison Between Indexes"
    echo "==================================="
    
    echo "Public Index Fields:"
    curl -s "$ES_URL/$PUBLIC_INDEX/_search?size=1" | jq -r '.hits.hits[0]._source | keys[]' 2>/dev/null | sort || echo "No data found"
    
    echo ""
    echo "Private Index Fields:"  
    curl -s "$ES_URL/$PRIVATE_INDEX/_search?size=1" | jq -r '.hits.hits[0]._source | keys[]' 2>/dev/null | sort || echo "No data found"
    
    echo ""
    echo "Public Index ONLY fields (should be limited set):"
    public_fields=$(curl -s "$ES_URL/$PUBLIC_INDEX/_search?size=1" | jq -r '.hits.hits[0]._source | keys[]' 2>/dev/null | sort)
    private_fields=$(curl -s "$ES_URL/$PRIVATE_INDEX/_search?size=1" | jq -r '.hits.hits[0]._source | keys[]' 2>/dev/null | sort)
    
    echo "$public_fields" | while read field; do
        if echo "$private_fields" | grep -q "^$field$"; then
            echo "✅ $field (in both)"
        else
            echo "🆕 $field (public only)"
        fi
    done
    
    echo ""
    echo "Private Index ONLY fields (should include MOD data, workflow tags, etc.):"
    echo "$private_fields" | while read field; do
        if ! echo "$public_fields" | grep -q "^$field$"; then
            echo "🔒 $field (private only)"
        fi
    done
    echo ""
}

# Function to check document counts
check_counts() {
    echo "📈 Document Counts"
    echo "=================="
    
    public_count=$(curl -s "$ES_URL/$PUBLIC_INDEX/_count" | jq -r '.count // 0')
    private_count=$(curl -s "$ES_URL/$PRIVATE_INDEX/_count" | jq -r '.count // 0')
    
    echo "Public Index: $public_count documents"
    echo "Private Index: $private_count documents"
    
    if [ "$public_count" -eq "$private_count" ]; then
        echo "✅ Document counts match"
    else
        echo "⚠️  Document counts differ (expected for different filtering)"
    fi
    echo ""
}

# Function to check specific new fields
check_new_fields() {
    echo "🆕 New Fields Verification"
    echo "========================="
    
    # Check for relations
    echo "Relations field:"
    curl -s "$ES_URL/$PUBLIC_INDEX/_search?q=relations:*&size=1" | jq '.hits.hits[0]._source.relations // "Not found"'
    
    echo ""
    echo "Copyright License field:"
    curl -s "$ES_URL/$PUBLIC_INDEX/_search?q=copyright_license:*&size=1" | jq '.hits.hits[0]._source.copyright_license // "Not found"'
    
    echo ""
    echo "Mesh Terms field:"
    curl -s "$ES_URL/$PUBLIC_INDEX/_search?q=mesh_terms:*&size=1" | jq '.hits.hits[0]._source.mesh_terms // "Not found"'
    
    echo ""
    echo "Resource Title field:"
    curl -s "$ES_URL/$PUBLIC_INDEX/_search?q=resource_title:*&size=1" | jq '.hits.hits[0]._source.resource_title // "Not found"'
    
    echo ""
}

# Function to run sample searches
run_sample_searches() {
    echo "🔍 Sample Search Queries"
    echo "======================="
    
    run_query "$PUBLIC_INDEX" "q=genomics&size=1" "Search for 'genomics' in public index"
    run_query "$PRIVATE_INDEX" "q=genomics&size=1" "Search for 'genomics' in private index"
    
    run_query "$PUBLIC_INDEX" "q=curie:AGRKB*&size=1" "Search by CURIE in public index"
    run_query "$PRIVATE_INDEX" "q=curie:AGRKB*&size=1" "Search by CURIE in private index"
}

# Function to validate required fields
validate_required_fields() {
    echo "✅ Required Fields Validation"
    echo "============================"
    
    required_fields=("curie" "title" "cross_references" "authors" "citation" "abstract" "category" "pubmed_types" "resource_title" "volume" "issue_name" "page_range" "publisher" "language" "date_published" "pubmed_publication_status" "date_arrived_in_pubmed" "date_last_modified_in_pubmed" "date_created" "keywords" "relations" "copyright_license" "mesh_terms")
    
    sample_doc=$(curl -s "$ES_URL/$PUBLIC_INDEX/_search?size=1" | jq '.hits.hits[0]._source // {}')
    
    for field in "${required_fields[@]}"; do
        if echo "$sample_doc" | jq -e "has(\"$field\")" > /dev/null 2>&1; then
            echo "✅ $field"
        else
            echo "❌ $field (missing)"
        fi
    done
    echo ""
}

# Main execution
echo "Starting verification..."
echo ""

check_counts
compare_fields  
check_new_fields
validate_required_fields
run_sample_searches

echo "🎉 Verification Complete!"
echo "========================"
echo ""
echo "💡 Additional queries you can try:"
echo ""
echo "# Search all documents in public index:"
echo "curl '$ES_URL/$PUBLIC_INDEX/_search?size=10&pretty'"
echo ""
echo "# Search for documents with relations:"
echo "curl '$ES_URL/$PUBLIC_INDEX/_search?q=relations:*&pretty'"
echo ""
echo "# Search for documents with mesh terms:"
echo "curl '$ES_URL/$PUBLIC_INDEX/_search?q=mesh_terms:*&pretty'"
echo ""
echo "# Get mapping (field structure) of public index:"
echo "curl '$ES_URL/$PUBLIC_INDEX/_mapping?pretty'"
echo ""
echo "# Compare with private index mapping:"
echo "curl '$ES_URL/$PRIVATE_INDEX/_mapping?pretty'"