# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

The AGR Literature Service is a FastAPI-based backend for the Alliance of Genome Resources (AGR) Literature platform. It manages scientific publications and bibliographic data for genomics research, integrating with PubMed and Model Organism Databases (MODs).

## Key Development Commands

### Environment Setup
```bash
# Start development environment with PostgreSQL
make run-dev-bash    # Launch development container with bash
make run-dev-zsh     # Launch development container with zsh (with vim/zsh configs)

# Build all containers
make build
```

### Code Quality
```bash
# Run linting and type checking (Docker-based)
make run-flake8      # Code linting via Docker
make run-mypy        # Type checking via Docker

# Run locally (faster for development)
make run-local-flake8   # Local flake8 linting
make run-local-mypy     # Local mypy type checking

# Pre-commit quality check (ALWAYS run before committing)
make run-local-flake8 && make run-local-mypy   # Combined check
```

### Testing
```bash
# Full test suite (Docker-based, includes database setup)
make run-test-bash      # Complete test run with coverage (76% minimum)

# Functional tests only
make run-functest       # End-to-end functional tests

# Direct pytest (from within dev container)
./run_tests.sh         # Runs pytest -m "not webtest"
```

### Database Management
```bash
# Create database migration
make alembic-create-migration ALEMBIC_COMMENT="description"

# Apply latest migration
make alembic-apply-latest-migration

# Export production data locally
make dump_prod_locally
```

### Service Management
```bash
# Restart API service
make restart-api

# Restart background processing scripts
make restart-automated-scripts

# Restart both API and automated scripts
make restart-api-and-automated-scripts
```

### Data Processing
```bash
# Bulk file upload for MODs
make bulk_upload_reference_files local_folder=/path mod_abbreviation=SGD

# Debezium (Change Data Capture) setup
make restart-debezium-local    # Local development
make restart-debezium-aws      # AWS environment
make stop-debezium            # Stop Debezium services
```

## Architecture

### Core Components

**API Layer** (`agr_literature_service/api/`):
- **`routers/`** - FastAPI endpoints grouped by entity type
- **`models/`** - SQLAlchemy ORM models for database tables
- **`schemas/`** - Pydantic models for request/response validation
- **`crud/`** - Database operations organized by entity
- **`database/`** - Database configuration and connection management

**Data Processing** (`agr_literature_service/lit_processing/`):
- **`data_ingest/`** - Import pipelines for external data sources
- **`pubmed_ingest/`** - PubMed-specific processing (XML parsing, metadata extraction)
- **`dqm_ingest/`** - Data Quality Management pipeline for MOD data
- **`data_export/`** - Database export utilities
- **`data_check/`** - Quality assurance and validation scripts

### Key Domain Entities

**Model Organism Databases (MODs)**: WB (WormBase), MGI (Mouse), SGD (Yeast), RGD (Rat), ZFIN (Zebrafish), FB (FlyBase)

**Core Data Models**:
- **Reference** - Scientific publications with PubMed integration
- **Author/Editor** - Publication authors with ORCID support
- **Resource** - Journals, books, databases
- **Workflow Tags** - Curation workflow management system
- **Topic Entity Tags** - Subject matter classification
- **Cross References** - Links between publications and biological entities

### Technology Stack

- **FastAPI 0.95.x** - Async Python web framework
- **SQLAlchemy 2.0.x** - Modern ORM with type hints
- **PostgreSQL 13.x** - Primary database
- **Elasticsearch 7.x** - Search indexing
- **Alembic** - Database migrations
- **Docker Compose** - Development orchestration

## Git Workflow and Quality Gates

### Pre-Commit Requirements
**MANDATORY**: Always run these commands before committing any changes:

```bash
# Run both linting and type checking
make run-local-flake8 && make run-local-mypy

# Alternative: Run them separately and ensure both pass
make run-local-flake8    # Must pass with zero errors
make run-local-mypy      # Must pass with zero issues
```

**Important**: 
- All flake8 errors must be resolved before committing
- All mypy type issues must be addressed before committing
- These checks are enforced in CI/CD pipelines
- Use conventional commit format: `<type>[scope]: <description>`

### Common Commit Types
- `feat`: New features
- `fix`: Bug fixes  
- `refactor`: Code restructuring
- `test`: Test additions/modifications
- `docs`: Documentation changes
- `chore`: Maintenance tasks

## Development Notes

### Testing Structure
- Tests require PostgreSQL and Elasticsearch containers
- Minimum 76% code coverage enforced
- Separate functional tests for end-to-end validation
- Test data isolated in `tests/` directories

### Data Processing Patterns
- PubMed XML processing for literature ingestion
- MOD-specific data transformation pipelines
- Automated curation workflow transitions
- S3 integration for file storage

### Environment Files
- `.env.test` - Testing configuration
- Multiple environment file support via `ENV_FILE` variable
- Docker-based development with mounted volumes for code changes

### Bioinformatics Context
When working with this codebase, understand that:
- MODs are authoritative sources for organism-specific literature
- PubMed IDs (PMIDs) are primary identifiers for publications
- Curation workflows involve manual review of automated classifications
- Cross-references link papers to genes, proteins, and biological processes