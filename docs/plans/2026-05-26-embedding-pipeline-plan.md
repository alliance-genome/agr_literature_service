# Plan: ABC-owned embedding pipeline for reference classification & RAG

## Context

Two downstream systems need vector representations of Alliance references:

- **`agr_automated_information_extraction`** — document classifiers. Today they recompute their own *averaged word embeddings* on every train/classify run (off the markdown), and already support filtering by section.
- **`agr_ai_curation`** — RAG biocuration assistant. Maintains its own embeddings in **Weaviate** at paragraph level, using OpenAI `text-embedding-3-small` (Weaviate auto-embeds each chunk via its `text2vec-openai` module on ingest).

Currently, there is no shared, persisted, reusable embedding store; embeddings are recomputed redundantly and can't be shared. This pipeline makes the **ABC (`agr_literature_service`) the source of truth** for reference embeddings: generate them once from the already-generated markdown (and abstracts), store them durably, and serve them to both consumers.

**Goals / outcomes:**

1. **Speed up the biocuration assistant (`agr_ai_curation`).** It currently has Weaviate re-embed every chunk through OpenAI on ingest. Reusing ABC-precomputed embeddings removes that redundant work and shortens its embedding phase — the vectors are already generated, persisted, and vector-compatible (verified: the assistant defaults to the same `text-embedding-3-small`, 1536-dim). See *Compatibility with `agr_ai_curation`* below.
2. **Improve the document classifiers.** Replace the classifier's averaged BioWordVec word vectors — 2019-era *static* embeddings (one fixed vector per word, context-insensitive) that are then mean-pooled into a single document vector, discarding word order and surrounding context — with modern contextual dense embeddings (OpenAI `text-embedding-3-small` now, maybe SPECTER2 later) for higher-quality classification.

A shared, persisted, reusable embedding store also **enables LLM-based classifiers to replace the current supervised ML ones**: once embeddings are precomputed and reusable, retrieval/LLM-driven classification can be built on the same vectors without re-embedding the corpus. Beyond classification, the same vectors let us start thinking about **other, potentially more useful retrieval pipelines than topic classifiers** — e.g. semantically surfacing references worth curating and identifying research areas to focus on to expand curation.

Aligns with existing Blue Team epic **SCRUM-6139 "Store embeddings file in S3 with metadata stored in ABC tables"** (created 2026-05-26), children: SCRUM-6137 (parquet format spike), SCRUM-6140 (embed abstracts w/ small model), SCRUM-6141 (ABC embedding-metadata table), SCRUM-6142 (workflow to embed MD files), SCRUM-6143/6144 (embed WB/FB training sets), SCRUM-6145 (embed older files for classification).

## Key decisions

1. **Home:** the embedding pipeline + canonical store live in the ABC (`agr_literature_service`).
2. **Embedding model:** OpenAI `text-embedding-3-small` (1536-dim) baseline — cheap (~$0.02/1M tokens; full-corpus full-text backfill ≈$160 one-time), strong, and vector-compatible with `agr_ai_curation`. **Not** BioWordVec/BioSentVec as primary (2019-era static vectors — same generation the classifier already averages). **SPECTER2** is the recommended future challenger, droppable in with no schema change.
3. **Flexible multi-vector schema:** each reference can hold *several* vector sets, keyed by `(model_name, source_scope, embedding_level)` — where `model_name` is a single versioned tag (e.g. `openai:text-embedding-3-small@v1`), plus two **orthogonal** axes: `source_scope` (*what text*: `abstract|fulltext`) and `embedding_level` (*chunk granularity*: `document|section|paragraph|sentence|word`). Core design principle.
4. **Source × granularity:** the two axes vary independently. `source_scope` = `abstract` or `fulltext`; `embedding_level` = `document` (a single pooled vector over the whole source — e.g. the entire markdown for `fulltext`), `section`, `paragraph`, `sentence`, or `word`. **A single reference holds multiple `(source_scope, embedding_level)` combinations at once** — e.g. `(abstract, document)`, `(fulltext, document)`, and `(fulltext, paragraph)` coexist, since the unique key includes both axes. First pass generates `(abstract, document)` + `(fulltext, document)` + `(fulltext, paragraph)` for target sets.
5. **Section provenance:** each chunk vector records which source section it came from (stored in the parquet), so consumers can **exclude unwanted sections** (references/acknowledgments/etc.) at train/query time and re-pool `fulltext` with a section-exclusion list.
6. **Storage/serving:** vectors as **parquet on S3**; a **catalog table in ABC Postgres** holds metadata only (not the float vectors). **Why parquet, not Postgres, in this phase:** the workload is *bulk* training reads, and at ~6 KB per 1536-dim vector the chunked corpus runs to hundreds of GB — storing that in the primary literature DB would bloat backups/WAL/replication/vacuum and burden the OLTP path it shares with the app, Elasticsearch sync, and Debezium CDC. Keeping vectors out of the DB stays far cheaper per GB and compresses well. Weaviate **deferred** — added later as a downstream sync for online search/RAG, parquet remaining the source of truth.
7. **Consumption contract:** consumers call an **ABC API endpoint** that resolves the catalog by `(mod, model_name, source_scope, embedding_level)` and returns per-reference access to the matching parquet files — the same shape as the existing model/file download endpoints.

## Architecture

### Catalog table — `reference_embedding` (SCRUM-6141)
A lightweight index/catalog in Postgres — **not** the vectors — modelled on the existing (unversioned) `ml_model` table. One row per `(reference, model_name, source_scope, embedding_level)`; that tuple is the unique key and maps **1:1 to one parquet file**. The row carries the reference link, `model_name` (a single versioned tag, e.g. `openai:text-embedding-3-small@v1`), `source_scope` (`abstract|fulltext`), `embedding_level` (`document|section|paragraph|sentence|word`), and a source-file provenance hash (null for abstracts). It stores **no S3 path** — the key is derived from the unique tuple, the way `referencefile` derives its location from its md5sum — and **no dimension or chunk count**, which the parquet already carries.

### S3/parquet store (SCRUM-6137 — format spike)
Vectors live as **one parquet per catalog row**, under the existing literature S3 bucket, at a key derived from `(model, source_scope, embedding_level, reference_id)`. Re-embedding overwrites the same key (idempotent). Trade-off vs. batching everything into big files: a training set becomes many parallel small reads rather than one bulk read; per-file overhead is negligible at `paragraph`/`section` level and small for single-vector `document` level.

Each parquet carries the vectors plus **per-chunk section provenance** — source section name/type and character offsets — so consumers can exclude unwanted sections at train/query time and re-pool the `fulltext` vector with a section-exclusion list. (The format spike validates the exact layout.)

### Pluggable embedder
A small `Embedder` interface (`name`, `dimension`, `embed`) with an OpenAI implementation now (`text-embedding-3-small`, batched with rate-limit/retry) and local models (SPECTER2 / sentence-transformers) droppable in later via config — no schema change.

### Generation pipeline (SCRUM-6142)
For each reference: **fetch source text → chunk per level (capturing section provenance) → batch-embed → write parquet to S3 → upsert the catalog row**, idempotent via the unique key.
- **Abstract** source: the reference's abstract.
- **Fulltext** source: the already-converted markdown (the `converted_merged_main` markdown file), reusing the existing file-download path. `fulltext` is the whole text (chunk-and-mean-pool over the model's token limit); `paragraph`/`section`/`sentence` are finer splits, with section provenance falling out of the markdown structure.

### Triggers (SCRUM-6142 / 6140 / 6143 / 6144 / 6145)
- **New docs:** when text-conversion completes, an embedding job is queued as a workflow sub-task (mirroring the existing classification / text-convert jobs) and picked up by a cron worker that runs the generation pipeline and marks it done. The sub-task needs new ATP workflow-tag IDs from the A-team (cross-team dependency).
- **Scheduling:** a cron entry alongside the existing PDF→markdown worker runs the generation engine.
- **Backfills:** the same engine, exposed as CLIs, seeds the existing corpus — abstracts (SCRUM-6140), WB/FB training sets via the MOD corpus (SCRUM-6143/6144), and older converted files (SCRUM-6145).

### Retrieval endpoint
A new authenticated download endpoint following the **same pattern as the existing `referencefile` / `ml_model` file retrieval**: resolve the catalog by `(mod, model_name, source_scope, embedding_level)` and return per-reference access to the matching parquet files. **Classifier-side migration:** swap the per-doc word-vector averaging for fetching these precomputed vectors by curie, optionally filtering chunks by section.

**Training note — document labels vs. chunk vectors:** topic labels are document-level, so the supervised classifier trains on the document-level pooled `(fulltext, document)` vector — labels map 1:1 and it's a drop-in for the current word-averaging. The `(fulltext, paragraph)` vectors stay available for topics whose signal is localized to a few paragraphs (which mean-pooling dilutes): switch to a learned max/attention pool over the paragraph bag (multiple-instance learning) — same document labels, no re-embedding. This is also the bridge to the future retrieval/LLM classifiers, which consume the paragraph vectors directly.

### Compatibility with `agr_ai_curation` (verified against the repo)
- **Model/space match:** the assistant defaults to OpenAI `text-embedding-3-small` at 1536 dims — the same model and vector space as this pipeline — so ABC vectors are directly reusable.
- **Granularity match:** it chunks at paragraph level by default, matching the `paragraph` level here.
- **Reuse requires "bring-your-own" vectors:** today Weaviate auto-embeds each chunk through its `text2vec-openai` vectorizer. To consume ABC vectors instead, the assistant's chunk collection switches to *no* vectorizer and inserts the precomputed vector with each object — a path it already uses for its non-vectorized collections.
- **Precondition — text alignment:** chunk-level reuse is only valid when the embedded text matches, so the two sides must share the chunking (or both read the same converted markdown). Where boundaries can't be aligned, the assistant can still reuse the coarser `document`-level vector.

### Weaviate (deferred)
Later, a loader syncs parquet → Weaviate for online search/RAG (converging with `agr_ai_curation`), parquet remaining the source of truth. Not built in this phase.

**Why Weaviate (vs. Postgres/`pgvector`) for the future online layer:**
- **Purpose-built ANN at scale** — native HNSW with better recall/latency and far less tuning than `pgvector` bolted onto the OLTP DB, and it scales horizontally as the corpus grows.
- **Keeps query load off the literature DB** — online vector search runs on a dedicated engine rather than the Postgres that already serves the app, Elasticsearch sync, and Debezium CDC (same "don't overload the DB" reasoning that favors parquet now).
- **Convergence with `agr_ai_curation`** — that assistant already runs Weaviate, so a shared store reuses its existing RAG/hybrid-search tooling instead of introducing a second pattern.
- **Richer retrieval out of the box** — hybrid search (vector + BM25), multi-tenancy, and dynamic schema for multiple vector spaces.

(`pgvector` remains a lighter-weight fallback — one fewer system to run — if a dedicated vector DB proves overkill.)

## Phasing (maps to the SCRUM epic)
1. **Catalog + format foundation** — `reference_embedding` table (SCRUM-6141); confirm parquet layout incl. provenance columns (SCRUM-6137).
2. **Pipeline core** — embedder + section-aware chunkers + parquet store + pipeline. Supports `abstract` + `fulltext` + `paragraph`.
3. **Backfills** — abstracts (SCRUM-6140); WB/FB training sets (SCRUM-6143/6144); older files (SCRUM-6145).
4. **Workflow hook** — embed-on-conversion sub-task (SCRUM-6142).
5. **Retrieval endpoint** — the download API + documented cross-repo contract; migrate the classifier to consume ABC embeddings (with section filtering).
6. **Deferred** — Weaviate sync; SPECTER2/local model; sentence-level.
