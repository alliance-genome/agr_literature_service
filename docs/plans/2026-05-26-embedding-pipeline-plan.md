# Plan: ABC-owned embedding pipeline for reference classification & RAG

## Context

Two downstream systems need vector representations of Alliance references:

- **`agr_automated_information_extraction`** — document classifiers. Today it fetches docs in TEI and recomputes its own *averaged word embeddings* (`--weighted_average_word_embedding`, `--embedding_model_path`) on every train/classify run. It already supports a `--sections_to_use` option.
- **`agr_ai_curation`** — RAG biocuration assistant. Maintains its *own* OpenAI embeddings in **Weaviate**, paragraph-level.

There is no shared, persisted, reusable embedding store; embeddings are recomputed redundantly and can't be shared. This pipeline makes the **ABC (`agr_literature_service`) the source of truth** for reference embeddings: generate them once from the already-generated markdown (and abstracts), store them durably, and serve them to both consumers.

**Goal / outcome:** improve the document classifiers first (better-quality dense embeddings, precomputed and reusable), then enable LLM-based classifiers and a shared RAG vector layer later.

Aligns with existing Blue Team epic **SCRUM-6139 "Store embeddings file in S3 with metadata stored in ABC tables"** (created 2026-05-26), children: SCRUM-6137 (parquet format spike), SCRUM-6140 (embed abstracts w/ small model), SCRUM-6141 (ABC embedding-metadata table), SCRUM-6142 (workflow to embed MD files), SCRUM-6143/6144 (embed WB/FB training sets), SCRUM-6145 (embed older files for classification).

## Key decisions (settled in brainstorm)

1. **Home:** the embedding pipeline + canonical store live in the ABC (this repo).
2. **Embedding model:** OpenAI `text-embedding-3-small` (1536-dim) baseline — cheap (~$0.02/1M tokens; full-corpus full-text backfill ≈$160 one-time), strong, vector-compatible with `agr_ai_curation`. **Not** BioWordVec/BioSentVec as primary (2019-era static vectors — same generation the classifier already averages). **SPECTER2** is the recommended future challenger, droppable in with no schema change.
3. **Flexible multi-vector schema:** each reference can hold *several* vector sets, keyed by `(model, model_version, embedding_level)`. Core design principle.
4. **Granularity:** levels `abstract`, `fulltext` (single pooled vector over the whole markdown), `paragraph`, `section`, `sentence`. **A single reference holds multiple levels at once** — explicitly an `abstract` embedding *and* a `fulltext` embedding coexist (different rows / parquet payloads), since the unique key includes `embedding_level`. First pass generates `abstract` + `fulltext` + `paragraph` for target sets.
5. **Section provenance:** each chunk vector records which source section it came from, stored in the parquet, so consumers can **exclude unwanted sections** (references/acknowledgments/etc.) at train/query time and re-pool `fulltext` with a section-exclusion list (feeds the classifier's `--sections_to_use`).
6. **Storage/serving:** vectors as **parquet on S3**; a **catalog table in ABC Postgres** holds metadata only (not the float vectors). Weaviate **deferred** — added later as a downstream sync for online search/RAG, parquet remaining the source of truth.
7. **Consumption contract:** consumers call an **ABC API endpoint** returning **presigned S3 URL(s)** to the relevant parquet batch(es), filtered by `(mod, model, version, level)`. Mirrors the `/ml_model` download flow; efficient for training-scale bulk reads.

## Architecture

### Catalog table — `reference_embedding` (new model + alembic migration) — SCRUM-6141
An index/catalog, **not** the vectors. New file `api/models/reference_embedding_model.py`, following the `Base, AuditedModel` pattern (`api/models/audited_model.py` provides `date_created`/`date_updated` automatically — do **not** hand-roll timestamps); skip versioning, like `ml_model_model.py`. Register in `api/models/__init__.py` so Alembic autogenerate sees it.
- PK `reference_embedding_id`; `reference_id` FK → `reference.reference_id` (`ondelete="CASCADE"`, indexed) + `relationship("ReferenceModel")`.
- `model_name` (`openai:text-embedding-3-small`), `model_version`, `embedding_level` (`abstract|fulltext|paragraph|section|sentence`), `dimension` (Int), `chunk_count` (Int), `s3_path`, `content_hash` (md5 of embedded source text; indexed — idempotency/staleness), `source_file_md5sum` (nullable; null for abstract-derived).
- Unique constraint via `__table_args__`: `(reference_id, model_name, model_version, embedding_level)` — what lets one reference hold `abstract` **and** `fulltext` simultaneously.
- Migration: `make alembic-create-migration ALEMBIC_COMMENT="add reference_embedding table"`; set `down_revision = 'b1f8e7d6c5a4'` (current head); review autogenerate for `ondelete` + named constraint.

### S3/parquet store — SCRUM-6137 (format spike)
Vectors in parquet under the existing `agr-literature` bucket. Reuse `lit_processing/utils/s3_utils.py`: `upload_file_to_s3`, `download_file_from_s3`, `file_exist_from_s3`. Add `get_embedding_s3_folder(model_name, model_version, level)` → `{env}/reference/embeddings/{model}/{version}/{level}` (env prefix from `ENV_STATE`, mirroring `get_ml_model_s3_folder`); persist the full key in `reference_embedding.s3_path`. Layout: **batched by model+level**, so a whole training set loads in one read.

**Parquet columns (incl. section provenance):** `reference_id, chunk_index, embedding, text_hash, section_name` (raw markdown heading), `section_type` (normalized label: intro/methods/results/discussion/references/… where derivable), `char_start`, `char_end`. Per-chunk provenance enables section exclusion at train/query time and configurable `fulltext` re-pooling. High-cardinality provenance lives in parquet, **not** the catalog DB. (SCRUM-6137 validates the exact format.)

### Pluggable embedder
`Embedder` interface (`name`, `version`, `dimension`, `embed(texts)->vectors`). `OpenAIEmbedder` (text-embedding-3-small) now — batches, rate-limit/retry. Future `Specter2Embedder` / `SentenceTransformerEmbedder` (local), config-driven.

### Generation pipeline — `lit_processing/embedding_ingest/` (new module) — SCRUM-6142
`fetch source text → chunk per level (capturing section provenance) → batch-embed → write parquet to S3 → upsert catalog rows`. New files: `embedder.py`, `chunkers.py`, `parquet_store.py` (pyarrow + s3_utils wrappers; computes `content_hash`/`chunk_count`), `pipeline.py` (orchestration, idempotent via `content_hash`/unique-key check).
- **Abstract source:** `ReferenceModel.abstract`.
- **Fulltext source:** reuse the proven path in `lit_processing/data_ingest/full_text/extract_emails.py` — query `referencefile` for `file_class='converted_merged_main'` + `file_extension='md'`, then `referencefile_crud.download_file(db, referencefile_id=…, mod_access=ModAccess.ALL_ACCESS, use_in_api=False)` returns **decompressed bytes** (no manual gunzip); decode utf-8 → `read_markdown` / `load_document_with_supplements` from `agr_abc_document_parsers.md_reader`. (Optional TEI fallback via `convert_xml_to_markdown`.)
- Chunkers walk `Document.sections`/`.paragraphs`, so `section_name`/`section_type`/offsets come for free; `fulltext` = whole text (chunk-and-mean-pool over 8K-token limit), `paragraph`/`section`/`sentence` = finer splits.
- CRUD/schemas: `api/crud/reference_embedding_crud.py` + `api/schemas/reference_embedding_schemas.py` mirroring `ml_model_crud.py`/`ml_model_schemas.py`; add `upsert_catalog_entry` / `get_by_content_hash`.

### Triggers — SCRUM-6142 / 6140 / 6143 / 6144 / 6145
- **New docs:** conversion completes in `api/utils/conversion_processor.py:run_conversion_job` (after `transition_completed_text_convert_tags` / `complete_job`). Wire embedding as a workflow sub-task mirroring `text_convert_job`:
  - Add `'embedding'` to `jobs_types` in `api/crud/workflow_transition_actions/subtask_process.py` (needs new ATP workflow-tag IDs from the A-team ontology, like classification's `ATP:0000165` family — cross-team dependency).
  - New data module `lit_processing/oneoff_scripts/workflow/data/embedding.py` (`get_data(name_to_atp)`, modeled on `text_conversion.py`); wire into `transitions_add.py` (import + `elif` branch); load via `transitions_add.py -f embedding`. Fan out from text-conversion success by adding a `proceed_on_value::…::embedding_job` action to the `text_conversion.py` `on_success` rows.
  - **Generation as a cron consumer** of `embedding_job` tags (parity with `pdf2md.py`), preferred over inline; new `api/utils/embedding_processor.py:run_embedding_job` mirrors the `conversion_processor` pair, marking done via the existing `/workflow_tag/job/success` path.
- **Scheduling:** add a `crontab` line (sibling of the `pdf2md.py` entry) → `lit_processing/embedding/generate_embeddings.py`; deploy via `make restart-automated-scripts` (container `automated_scripts`).
- **Backfills:** thin argparse CLIs over the shared `generate_embeddings.py` engine, following `pdf2md/pdf2md.py` batching; session via `create_postgres_session(False)`. Abstracts = all refs (cursor by `reference_id`); WB/FB training sets = reuse the `mod_corpus_association` corpus query (`abc_utils.get_all_ref_curies`), param by `--mod-abbreviation`; older files = `--since-year`/`--before-year` over `converted_merged_main`.

### Retrieval endpoint — presigned parquet URL
New `api/routers/reference_embedding_router.py` (registered in `api/main.py` next to `ml_model_router`) with a GET resolving the catalog by `(mod, model, version, level)` and returning a **presigned S3 URL** to the parquet batch(es) + catalog metadata. Reuse the existing `create_presigned_url` helper (`api/s3/download.py`). Auth via standard `Security(get_authenticated_user)` (`api/auth.py`); the classifier's existing `agr_cognito_py` token + `generate_headers` work unchanged. **Classifier-side migration:** replace per-doc word-vector averaging (`utils/embedding.py` + the X-matrix build in `agr_document_classifier_classify.py`) with a parquet fetch (`abc_utils.download_embeddings_for_references`, mirroring `download_abc_model`), looking up vectors by curie and optionally filtering chunks by `section_type`.

### Weaviate (deferred)
Later loader syncs parquet → Weaviate for online search/RAG (converging with `agr_ai_curation`). Catalog already carries model/level/dimension; provenance columns carry section, so the mapping is clean. Not built in this phase.

## Components — new files + existing files to touch

**New files:**
- `api/models/reference_embedding_model.py` — catalog ORM (`Base, AuditedModel`).
- `alembic/versions/<rev>_add_reference_embedding_table.py` — migration (`down_revision='b1f8e7d6c5a4'`).
- `api/schemas/reference_embedding_schemas.py` — Pydantic Post/Show.
- `api/crud/reference_embedding_crud.py` — catalog upsert/query + presigned-URL resolution.
- `api/routers/reference_embedding_router.py` — retrieval (+ optional manual-trigger) endpoints.
- `api/utils/embedding_processor.py` — `run_embedding_job` (mirror of `conversion_processor`).
- `lit_processing/embedding_ingest/` — `embedder.py`, `chunkers.py`, `parquet_store.py`, `pipeline.py`.
- `lit_processing/embedding/generate_embeddings.py` — shared cron/backfill engine + backfill CLIs.
- `lit_processing/oneoff_scripts/workflow/data/embedding.py` — workflow-tag transition data.

**Existing files to touch:**
- `api/models/__init__.py` — register the new model.
- `api/config.py` — add `OPENAI_API_KEY` + `EMBEDDING_MODEL_NAME`/`_VERSION`/`_BATCH_SIZE`.
- `api/main.py` — register `reference_embedding_router`.
- `api/utils/conversion_processor.py` — enqueue/transition the embedding tag on conversion success.
- `api/crud/workflow_transition_actions/subtask_process.py` — add `'embedding'` job type.
- `lit_processing/oneoff_scripts/workflow/transitions_add.py` — wire the `embedding` data module.
- `lit_processing/oneoff_scripts/workflow/data/text_conversion.py` — fan out to `embedding_job` on success.
- `crontab` — schedule `generate_embeddings.py`.
- `requirements.txt` — add `openai`, `pyarrow`, `pandas` (see Risks).

## Dependencies & risks
- **numpy pin (highest risk):** `requirements.txt` pins `numpy<2.0` (Elasticsearch 7.13.4). Pin `pyarrow` (e.g. `>=14,<17`) and `pandas` (e.g. `>=1.5,<2.2`) to keep numpy on 1.x; verify the resolved env after install. `openai` is pure-python, low risk.
- **Keep torch out of core:** SPECTER2/sentence-transformers (future) pull torch — gate behind the pluggable `Embedder` in an optional/extras requirements file, not core `requirements.txt`.
- **Secret handling:** read `OPENAI_API_KEY` from env/config only; never default or log it.
- **New ATP ontology IDs** for the embedding workflow tag must be requested from the A-team (cross-team dependency).
- **License/privacy:** full text is sent to OpenAI — flag that some MOD-supplied PDFs may be license-restricted before backfilling everything.

## Phasing (maps to the SCRUM epic)
1. **Catalog + format foundation** — `reference_embedding` model + migration (SCRUM-6141); confirm parquet layout incl. provenance columns (SCRUM-6137).
2. **Pipeline core** — Embedder + `OpenAIEmbedder`; section-aware chunkers; parquet_store; pipeline. Supports `abstract` + `fulltext` + `paragraph`.
3. **Backfills** — abstracts (SCRUM-6140); WB/FB training sets (SCRUM-6143/6144); older files (SCRUM-6145).
4. **Workflow hook** — embed-on-conversion sub-task (SCRUM-6142).
5. **Retrieval endpoint** — presigned-parquet API + documented cross-repo contract; migrate the classifier to consume ABC embeddings (with section filtering).
6. **Deferred** — Weaviate sync; SPECTER2/local model; sentence-level.

## Verification
- **Unit:** section-aware chunkers (paragraph/section/sentence on sample MD, asserting `section_name`/`section_type`/offsets); parquet round-trip incl. provenance columns; Embedder interface with a **fake/mock embedder** (deterministic fixed vectors — no real OpenAI calls in tests); catalog CRUD upsert/idempotency (`content_hash` skip + re-embed on change).
- **Integration:** pipeline end-to-end with the fake embedder against a test reference that has a converted MD in (mocked) S3 — assert catalog rows + parquet contents + that the retrieval endpoint returns a presigned URL whose parquet loads into a numpy array of the expected shape, and that section filtering drops the right rows.
- **Manual:** run the WB-training-set backfill on a small sample, inspect parquet + catalog, confirm the endpoint's presigned URL works.
- **DB-safety:** never run the pytest suite from the host shell (it wipes the configured DB); run tests only inside the test container. Lint/type gates: `make run-local-flake8 && make run-local-mypy` before any commit.
