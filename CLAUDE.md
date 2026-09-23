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
- **PostgreSQL 17.x** - Primary database
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
- `ENV_STATE` is **not** set consistently per deployment: it is `prod` on the
  production box but `build` on the dev box (the repo default). Container names
  embed it (`agr.literature.${ENV_STATE}.<service>.${COMPOSE_PROJECT_NAME}`), so
  the environment a container belongs to is really identified by
  `COMPOSE_PROJECT_NAME`. Check `docker ps` rather than assuming.

### Logging and Monitoring

Container stdout/stderr ships to `logs.alliancegenome.org:12201` via the Docker
GELF driver (`docker-compose.yaml`: `api`, `xml_processing`,
`automated_scripts`, and the five `dbz.*` services). That endpoint is the
`agr_logs` stack — Fluent Bit → S3 partitioned by `dt`/`hour` → Glue/Athena,
plus a web viewer behind Cognito.

Two things about it are counter-intuitive and worth knowing before debugging
anything log-related:

- **Severity in the log store reflects the stream, not the Python level.** The
  GELF driver maps stdout to `INFO` and stderr to `ERROR` and never sees the
  application's own level. `logging.conf` sends every record to `sys.stdout`, so
  an application `logger.error(...)` arrives in Athena labelled `INFO`. Anything
  that filters on `level_name` alone will miss it.
- **Cron output does not reach the log stream by default.** A `> file 2>&1`
  redirect replaces the process's fds, so the output only ever goes to the file.

`crontab` therefore runs every job through `docker/run_cron_job.sh`, which keeps
the per-job log file at exactly the same path and additionally writes an
`ABC-JOB-OK` / `ABC-JOB-FAILED` record to PID 1's fds so the GELF driver picks it
up (failures on fd 2, so they arrive as `level_name='ERROR'`). **Do not revert a
crontab line to a bare redirect** — it silently drops that job out of alerting.
`tests/test_run_cron_job.py` guards this.

Log files live under `${LOG_PATH}` (bind-mounted, web-served at `${LOG_URL}`)
and are truncated per run. Separately, some scripts write their own report files
into subdirectories — `QC/`, `dqm_load/`, `pubmed_search/`, `pubmed_update/`,
`data_check/` — and four QC reports also write dated copies
(`<stem>_YYYYMMDD.log`) that the API exposes via `QC_REPORTS` in
`api/crud/check_crud.py`. Those are unrelated to the crontab redirect.

### Bioinformatics Context
When working with this codebase, understand that:
- MODs are authoritative sources for organism-specific literature
- PubMed IDs (PMIDs) are primary identifiers for publications
- Curation workflows involve manual review of automated classifications
- Cross-references link papers to genes, proteins, and biological processes