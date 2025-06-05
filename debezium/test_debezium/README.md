# Debezium Testing Setup

This directory contains scripts to test the complete Debezium setup with both public and private reference indexes on your local machine.

## 🎯 What Gets Tested

The test suite validates the entire data pipeline:

### **Public Index** (`public_references_index`)
- **Limited fields only** as specified in requirements
- Fields: `curie`, `title`, `cross_references`, `relations`, `authors`, `copyright_license`, `citation`, `abstract`, `category`, `pubmed_types`, `resource_title`, `volume`, `issue_name`, `page_range`, `publisher`, `language`, `date_published`, `pubmed_publication_status`, `date_arrived_in_pubmed`, `date_last_modified_in_pubmed`, `date_created`, `keywords`, `mesh_terms`

### **Private Index** (`test_references_index`) 
- **Full dataset** including sensitive/internal fields
- Additional fields: `mods_in_corpus`, `mods_needs_review`, `workflow_tags`, `topic_entity_tags`, `mod_reference_types`, etc.

### **New Data Sources Tested**
- ✅ `reference_relation` → `relations` field
- ✅ `copyright_license` → `copyright_license` field  
- ✅ `mesh_detail` → `mesh_terms` field
- ✅ `resource` → `resource_title` field
- ✅ Cross-references including obsolete entries

## 📁 Files

| File | Purpose |
|------|---------|
| `test_debezium.sh` | **Main test script** - Complete end-to-end testing |
| `create_test_subset.py` | Creates minimal test dataset with all required tables |
| `verify_indexes.sh` | Detailed verification and comparison of both indexes |
| `README.md` | This documentation |

## 🚀 Quick Start

```bash
# One-command test (recommended)
./debezium/test_debezium/test_debezium.sh

# Or run from the test directory
cd debezium/test_debezium
./test_debezium.sh
```

## 🔧 Manual Testing Steps

```bash
# 1. Start core services
docker-compose --env-file .env.test up -d postgres elasticsearch

# 2. Initialize database with test data
docker-compose --env-file .env.test run --rm dev_app sh tests/init_test_db.sh

# 3. Add sample data for new tables
docker-compose --env-file .env.test run --rm dev_app python3 debezium/test_debezium/create_test_subset.py

# 4. Start complete Debezium stack
make restart-debezium-local ENV_FILE=.env.test

# 5. Verify results
./debezium/test_debezium/verify_indexes.sh
```

## 📊 Verification

The test creates **both indexes simultaneously** and verifies:

- ✅ Document counts match between indexes
- ✅ Public index contains only specified fields
- ✅ Private index contains additional sensitive fields  
- ✅ New fields are properly populated (`relations`, `copyright_license`, `mesh_terms`)
- ✅ Resource titles are correctly joined
- ✅ Cross-references include obsolete entries
- ✅ Search functionality works in both indexes

## 🔍 Sample Verification Commands

```bash
# Check both indexes exist
curl http://localhost:9200/_cat/indices?v

# Document counts
curl http://localhost:9200/public_references_index/_count
curl http://localhost:9200/test_references_index/_count

# Field comparison (public should have fewer fields)
curl 'http://localhost:9200/public_references_index/_search?size=1' | jq '.hits.hits[0]._source | keys | length'
curl 'http://localhost:9200/test_references_index/_search?size=1' | jq '.hits.hits[0]._source | keys | length'

# Sample documents
curl 'http://localhost:9200/public_references_index/_search?size=1&pretty'
curl 'http://localhost:9200/test_references_index/_search?size=1&pretty'

# Search for new fields
curl 'http://localhost:9200/public_references_index/_search?q=relations:*&pretty'
curl 'http://localhost:9200/public_references_index/_search?q=mesh_terms:*&pretty'
```

## 🎯 Expected Results

### **Success Criteria**
- Both indexes populated with same document count
- Public index has ~23 fields (limited set)
- Private index has ~35+ fields (includes MOD/workflow data)
- New fields present and populated: `relations`, `copyright_license`, `mesh_terms`, `resource_title`
- Cross-references include both regular and obsolete entries
- Search queries return expected results in both indexes

### **Test Data Created**
- 3 test references with complete metadata
- Authors and cross-references (including obsolete)
- Reference relations (Reviews, Cites)
- Copyright licenses (Creative Commons variants)
- MeSH terms (Genomics, Bioinformatics, etc.)
- Resource information for journal titles

## ⚡ Resource Requirements

- **RAM:** ~2GB for Docker containers
- **Disk:** ~1GB for data and indexes
- **Time:** ~5-10 minutes for complete setup
- **Dependencies:** Docker, Docker Compose, jq

## 🔧 Troubleshooting

```bash
# Check service status
docker-compose --env-file .env.test ps

# View logs
docker-compose --env-file .env.test logs elasticsearch
docker-compose --env-file .env.test logs dbz_kafka
docker-compose --env-file .env.test logs dbz_ksql_server

# Reset everything
docker-compose --env-file .env.test down -v

# Check Kafka topics
docker-compose --env-file .env.test exec dbz_kafka kafka-topics --list --bootstrap-server localhost:9092

# Check KsqlDB streams
docker-compose --env-file .env.test exec dbz_ksql_server ksql http://localhost:8088
```

## 📋 Environment Variables

The test uses `.env.test` by default, but you can override:

```bash
ENV_FILE=.env.local ./test_debezium/test_debezium.sh
```

Key variables:
- `ELASTICSEARCH_HOST` / `ELASTICSEARCH_PORT`
- `PSQL_HOST` / `PSQL_PORT` / `PSQL_DATABASE`
- `DEBEZIUM_INDEX_NAME` (private index name)
- `PUBLIC_INDEX_NAME` (defaults to `public_references_index`)

## 🎉 Next Steps

After successful testing:

1. **Create Pull Request** with the Debezium changes
2. **Deploy to staging** environment
3. **Run production migration** with full dataset
4. **Update API endpoints** to use public index for external access