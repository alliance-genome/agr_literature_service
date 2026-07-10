# SCRUM-6141 — `embedding_file` Catalog + referencefile API Extensions — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Amendment (2026-07-02, agreed with Chris Tabone on PR #1220 — supersedes the
> endpoint/access decisions below):**
> 1. **No HTTP create endpoint.** `POST /reference/embedding_file/` was removed;
>    embedding creation happens ABC-internally only, via
>    `embedding_file_crud.create_or_update` (called by the producers, e.g. the
>    file-conversion pipeline — SCRUM-6142). The router keeps only the
>    `GET /{id}` lookup; discovery/download stay on the `referencefile` APIs.
> 2. **Access is inherited from the source file, never caller-chosen.**
>    `mod_abbreviation` was dropped from `EmbeddingFileSchemaCreate`;
>    `create_or_update` syncs the parquet's `referencefile_mod` rows to exactly
>    match the source referencefile's (MOD-specific source → same MODs;
>    open-access/PMC source, i.e. a NULL mod row → open). Abstract embeddings
>    (`source_referencefile_id` NULL) are open access. Re-registering after a
>    source's access changed re-syncs (stale rows dropped), so a derived
>    embedding can never be downloadable more broadly than its source text.
> 3. **Access changes propagate immediately.** Every `referencefile_mod`
>    mutation path — `referencefile_mod_utils.create`/`destroy`,
>    `referencefile_mod_crud.patch`, and the merge-path
>    `merge_referencefiles` — now calls
>    `embedding_file_crud.resync_embeddings_access_for_source` (no-op for
>    non-source files), so derived parquets track their source's access without
>    waiting for a re-registration. Removing a source's *last* association
>    (which deletes the file) and the merge path also clean up derived
>    embeddings first, closing two paths that previously stranded parquets.
> 4. **Direct access edits on embedding files are rejected (422).** The public
>    `referencefile_mod` surface (POST via `referencefile_mod_crud.create`,
>    PATCH — including moving an association onto a parquet — and DELETE via
>    `referencefile_mod_utils.destroy`) refuses `file_class == "embedding"`
>    targets: `embedding_file_crud` is the only writer of embedding access
>    (`_sync_parquet_access` writes the rows directly). The internal upload
>    path (`create_metadata`/`file_upload_single` → `referencefile_mod_utils.create`)
>    stays unguarded — it sets the parquet's first, inherited association.
>    Note: a MOD-scoped `DELETE /reference/referencefile/{parquet_id}` now 422s
>    too (it worked by removing the MOD's association); embeddings are removed
>    via their source or by ALL_ACCESS whole-file delete.
>
> **Future work (SCRUM-6142):** a `create_if_not_exists`-style guard for
> producers — check the catalog for `(reference, profile_name, version,
> source_referencefile_id)` (via `get_embeddings_for_sources` or similar)
> *before* generating, so embeddings are never recomputed when already present.

**Goal:** Add a lean `embedding_file` catalog table (one row per `(reference, profile_name, version, source_referencefile_id)`, mapping 1:1 to a stored parquet) plus the CRUD and referencefile-API surface that lets consumers discover and download reference embeddings — embeddings stored as `referencefile` rows (`file_class='embedding'`) reusing existing file storage.

**Architecture:** The parquet *is* a `referencefile` (md5-derived S3 location, existing storage). `embedding_file` is a non-audited sidecar catalog holding embedding semantics (`profile_name`, `version`, `model_name`) + two FKs into `referencefile`: `source_referencefile_id` (the `converted_merged_*` markdown that was embedded; `NULL` for abstracts) and `parquet_referencefile_id` (the stored parquet). Retrieval reuses the existing `referencefile` APIs: `show_all` gains an upward `source` sub-object on every derived file and **always lists `embedding` rows** (annotated with `profile_name`/`version`/`model_name`) — it returns every file, with downstream filtering on the result; `conversion_request` gains an `embeddings` list per converted file; download is the existing `download_file/{referencefile_id}` unchanged.

**Tech Stack:** Python 3.11, FastAPI 0.95.x, SQLAlchemy 2.0.x, Alembic, PostgreSQL 13, pytest (dockerized test env).

**Design source of record:** `docs/plans/2026-05-26-embedding-pipeline-plan.md` (revised 2026-06-16, on branch `SCRUM-6139`) and Chris Tabone's `2026-06-10-curation-assistant-embedding-profile-spec.md` (attached to epic SCRUM-6139).

## Global Constraints

- **PostgreSQL 13** — no `NULLS NOT DISTINCT`; nullable-column uniqueness MUST use two partial unique indexes (the `referencefile_mod` pattern).
- **`embedding_file` is non-audited** — plain `Base` only (NO `AuditedModel`); no `date_created`/`date_updated`/`created_by`/`updated_by`. Audit/history live on the parquet `referencefile` row.
- **No vectors, no recipe descriptor, no md5 in `embedding_file`** — those live in the parquet file metadata and on the parquet `referencefile` row. Table columns are exactly: `embedding_file_id`, `reference_id`, `profile_name`, `version`, `model_name`, `source_referencefile_id`, `parquet_referencefile_id`. (Pending Chris reply: `embedding_dim`/`chunk_count` deliberately omitted — additive later if needed.)
- **Retrieval/download reuse `referencefile` APIs** — `show_all` / `download_file` / `conversion_request` (download unchanged). **Write/lookup of the catalog itself is exposed via a small dedicated router** (`POST` to register an embedding = parquet upload + catalog upsert, `GET /{id}` to fetch a row) so external generators / manual flows can register embeddings over HTTP — added per request (2026-06-30), superseding the original "no bespoke endpoints" decision. No catalog `DELETE` endpoint: an embedding is removed by deleting its parquet `referencefile` (cascades the catalog row + cleans S3).
- **`show_all` returns EVERY file, always** (2026-06-30 decision) — `embedding` rows are always listed (no `include_embeddings`/`profile_name`/`version` params); it is a flat metadata list and any per-type/profile/version narrowing is done downstream on the result. Negligible perf cost: metadata only, files already loaded, embedding enrichment is a single batched query (no N+1).
- **Parquet upload reuses `referencefile_crud.file_upload_single`** (NOT `file_upload`, which adds main-PDF workflow-tag side effects). `file_class='embedding'`, `file_extension='parquet'`, `file_publication_status='final'`.
- **Conventional Commits**; run `make run-local-flake8 && make run-local-mypy` before every commit. **Tests run only in the dockerized test env** (`make run-test-bash`) — never against a live DB from the host.
- Current single alembic head: **`c4d5e6f7a8b9`** (`20260622_c4d5e6f7a8b9_add_privacy_to_person.py`).

---

## File Structure

- **Create** `agr_literature_service/api/models/embedding_file_model.py` — `EmbeddingFileModel` (plain `Base`, two partial unique indexes).
- **Modify** `agr_literature_service/api/models/__init__.py` — import/register `EmbeddingFileModel`.
- **Create** `alembic/versions/<rev>_create_embedding_file_table.py` — create table + partial unique indexes.
- **Create** `agr_literature_service/api/schemas/embedding_file_schemas.py` — `EmbeddingFileSchemaCreate`, `EmbeddingFileSchemaShow`.
- **Modify** `agr_literature_service/api/schemas/referencefile_schemas.py` — add `ReferencefileSourceSchema`; add `source` + embedding fields to `ReferencefileSchemaRelated`.
- **Create** `agr_literature_service/api/crud/embedding_file_crud.py` — `create_or_update`, `get`, `destroy`, `get_embeddings_for_source`.
- **Modify** `agr_literature_service/api/crud/referencefile_crud.py` — extend `show_all` (source sub-object + `include_embeddings`/`profile_name`/`version`); add `_find_source_for_derived`.
- **Modify** `agr_literature_service/api/routers/referencefile_router.py` — add `show_all` query params.
- **Modify** `agr_literature_service/api/schemas/file_conversion_schemas.py` — add `embeddings` to `ConversionPerFileProgressSchema`.
- **Modify** `agr_literature_service/api/crud/file_conversion_crud.py` — add `_attach_embeddings`, call it in `_status_payload`.
- **Create** `tests/api/test_embedding_file.py` — model/CRUD + API tests.

---

## Task 1: `EmbeddingFileModel`

**Files:**
- Create: `agr_literature_service/api/models/embedding_file_model.py`
- Modify: `agr_literature_service/api/models/__init__.py:35` (after the `referencefile_model` import)
- Test: `tests/api/test_embedding_file.py`

**Interfaces:**
- Produces: `EmbeddingFileModel` with columns `embedding_file_id:int (PK)`, `reference_id:int (FK reference, CASCADE, NOT NULL)`, `profile_name:str (NOT NULL, indexed)`, `version:int (NOT NULL)`, `model_name:str (indexed)`, `source_referencefile_id:int (FK referencefile, CASCADE, NULLABLE)`, `parquet_referencefile_id:int (FK referencefile, CASCADE, NOT NULL)`; relationships `reference`, `source_referencefile`, `parquet_referencefile`.

- [ ] **Step 1: Write the failing test**

In `tests/api/test_embedding_file.py`:

```python
from agr_literature_service.api.models import (
    EmbeddingFileModel, ReferenceModel, ReferencefileModel,
)
from ..fixtures import db  # noqa


def test_embedding_file_model_columns():
    cols = EmbeddingFileModel.__table__.columns
    assert "embedding_file_id" in cols
    assert {"reference_id", "profile_name", "version", "model_name",
            "source_referencefile_id", "parquet_referencefile_id"} <= set(cols.keys())
    # Non-audited: no audit columns
    assert "created_by" not in cols
    assert "date_created" not in cols
    # Nullability per spec
    assert cols["source_referencefile_id"].nullable is True
    assert cols["parquet_referencefile_id"].nullable is False
    assert cols["reference_id"].nullable is False
    # Two partial unique indexes
    partial = [ix for ix in EmbeddingFileModel.__table__.indexes
               if ix.unique and ix.dialect_options["postgresql"]["where"] is not None]
    assert len(partial) == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run (inside dev container): `pytest tests/api/test_embedding_file.py::test_embedding_file_model_columns -v`
Expected: FAIL — `ImportError: cannot import name 'EmbeddingFileModel'`.

- [ ] **Step 3: Write the model**

Create `agr_literature_service/api/models/embedding_file_model.py`:

```python
from sqlalchemy import Column, Integer, String, ForeignKey, Index
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base


class EmbeddingFileModel(Base):
    """Lean, non-audited catalog of reference embedding parquet files.

    One row per (reference, profile_name, version, source_referencefile_id);
    that tuple maps 1:1 to a single stored parquet (an ``embedding``
    referencefile). NO vectors, recipe descriptor, or md5 here — those live
    in the parquet file metadata and on the parquet referencefile row, which
    is the audited/versioned artifact. See SCRUM-6141.
    """
    __tablename__ = "embedding_file"

    embedding_file_id = Column(Integer, primary_key=True, autoincrement=True)

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    reference = relationship("ReferenceModel", foreign_keys="EmbeddingFileModel.reference_id")

    profile_name = Column(String, index=True, nullable=False)
    version = Column(Integer, nullable=False)
    model_name = Column(String, index=True, nullable=True)

    # The converted_merged_* markdown that was embedded; NULL for abstracts.
    source_referencefile_id = Column(
        Integer,
        ForeignKey("referencefile.referencefile_id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    source_referencefile = relationship(
        "ReferencefileModel", foreign_keys="EmbeddingFileModel.source_referencefile_id"
    )

    # The stored embedding parquet (storage + download delegated to it).
    parquet_referencefile_id = Column(
        Integer,
        ForeignKey("referencefile.referencefile_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    parquet_referencefile = relationship(
        "ReferencefileModel", foreign_keys="EmbeddingFileModel.parquet_referencefile_id"
    )

    # PG13 has no NULLS NOT DISTINCT, so enforce the unique key with two
    # partial indexes (the referencefile_mod pattern).
    __table_args__ = (
        Index(
            "uq_embedding_file_with_source",
            "reference_id", "profile_name", "version", "source_referencefile_id",
            unique=True,
            postgresql_where=(source_referencefile_id.isnot(None)),
        ),
        Index(
            "uq_embedding_file_abstract",
            "reference_id", "profile_name", "version",
            unique=True,
            postgresql_where=(source_referencefile_id.is_(None)),
        ),
    )
```

- [ ] **Step 4: Register the model**

In `agr_literature_service/api/models/__init__.py`, after line 35 (`from ...referencefile_model import ...`) add:

```python
from agr_literature_service.api.models.embedding_file_model import EmbeddingFileModel
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/api/test_embedding_file.py::test_embedding_file_model_columns -v`
Expected: PASS.

- [ ] **Step 6: Lint + commit**

```bash
make run-local-flake8 && make run-local-mypy
git add agr_literature_service/api/models/embedding_file_model.py agr_literature_service/api/models/__init__.py tests/api/test_embedding_file.py
git commit -m "feat(embedding): add EmbeddingFileModel catalog table (SCRUM-6141)"
```

---

## Task 2: Alembic migration

**Files:**
- Create: `alembic/versions/<rev>_create_embedding_file_table.py`

**Interfaces:**
- Consumes: `EmbeddingFileModel` (Task 1).
- Produces: `embedding_file` table + two partial unique indexes in the DB; `down_revision = 'c4d5e6f7a8b9'`.

- [ ] **Step 1: Confirm single head**

Run: `python3 -c "import subprocess"` is not needed — verify by file inspection that the most recent migration is `20260622_c4d5e6f7a8b9_add_privacy_to_person.py`. The new migration's `down_revision` MUST be `c4d5e6f7a8b9`.

- [ ] **Step 2: Autogenerate the migration**

Run: `make alembic-create-migration ALEMBIC_COMMENT="create embedding_file table"`
This emits a file under `alembic/versions/`. Because `EmbeddingFileModel` is registered (Task 1, Step 4), autogenerate detects the new table.

- [ ] **Step 3: Verify/adjust the generated migration**

Open the generated file and ensure `upgrade()` creates the table and **both partial unique indexes**. If autogenerate did not emit `postgresql_where`, replace the index section so `upgrade()` reads exactly:

```python
def upgrade():
    op.create_table(
        'embedding_file',
        sa.Column('embedding_file_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('reference_id', sa.Integer(), nullable=False),
        sa.Column('profile_name', sa.String(), nullable=False),
        sa.Column('version', sa.Integer(), nullable=False),
        sa.Column('model_name', sa.String(), nullable=True),
        sa.Column('source_referencefile_id', sa.Integer(), nullable=True),
        sa.Column('parquet_referencefile_id', sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(['reference_id'], ['reference.reference_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['source_referencefile_id'], ['referencefile.referencefile_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['parquet_referencefile_id'], ['referencefile.referencefile_id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('embedding_file_id'),
    )
    op.create_index(op.f('ix_embedding_file_reference_id'), 'embedding_file', ['reference_id'], unique=False)
    op.create_index(op.f('ix_embedding_file_profile_name'), 'embedding_file', ['profile_name'], unique=False)
    op.create_index(op.f('ix_embedding_file_model_name'), 'embedding_file', ['model_name'], unique=False)
    op.create_index(op.f('ix_embedding_file_source_referencefile_id'), 'embedding_file', ['source_referencefile_id'], unique=False)
    op.create_index(op.f('ix_embedding_file_parquet_referencefile_id'), 'embedding_file', ['parquet_referencefile_id'], unique=False)
    op.create_index('uq_embedding_file_with_source', 'embedding_file',
                    ['reference_id', 'profile_name', 'version', 'source_referencefile_id'],
                    unique=True, postgresql_where=sa.text('source_referencefile_id IS NOT NULL'))
    op.create_index('uq_embedding_file_abstract', 'embedding_file',
                    ['reference_id', 'profile_name', 'version'],
                    unique=True, postgresql_where=sa.text('source_referencefile_id IS NULL'))


def downgrade():
    op.drop_index('uq_embedding_file_abstract', table_name='embedding_file')
    op.drop_index('uq_embedding_file_with_source', table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_parquet_referencefile_id'), table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_source_referencefile_id'), table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_model_name'), table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_profile_name'), table_name='embedding_file')
    op.drop_index(op.f('ix_embedding_file_reference_id'), table_name='embedding_file')
    op.drop_table('embedding_file')
```

Confirm `down_revision = 'c4d5e6f7a8b9'` near the top of the file.

- [ ] **Step 4: Apply and verify round-trip (in dockerized DB)**

Run: `make alembic-apply-latest-migration`
Then verify downgrade is reversible: `alembic downgrade -1 && alembic upgrade head` (inside container).
Expected: no errors; `embedding_file` table present with both `uq_embedding_file_*` indexes after upgrade.

- [ ] **Step 5: Commit**

```bash
git add alembic/versions/*create_embedding_file_table.py
git commit -m "feat(embedding): add embedding_file table migration (SCRUM-6141)"
```

---

## Task 3: Pydantic schemas

**Files:**
- Create: `agr_literature_service/api/schemas/embedding_file_schemas.py`
- Modify: `agr_literature_service/api/schemas/referencefile_schemas.py` (after line 103, the `ReferencefileConvertedDerivedSchema` block)
- Test: `tests/api/test_embedding_file.py`

**Interfaces:**
- Produces: `EmbeddingFileSchemaCreate` (fields: `reference_curie:str`, `profile_name:str`, `version:int`, `model_name:Optional[str]`, `source_referencefile_id:Optional[int]`, `mod_abbreviation:Optional[str]`); `EmbeddingFileSchemaShow` (adds `embedding_file_id:int`, `parquet_referencefile_id:int`); `ReferencefileSourceSchema` (`referencefile_id:int`, `display_name:str`, `file_class:str`, `file_extension:str`, `md5sum:str`).

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_embedding_file.py`:

```python
def test_embedding_file_schemas_shapes():
    from agr_literature_service.api.schemas.embedding_file_schemas import (
        EmbeddingFileSchemaCreate, EmbeddingFileSchemaShow,
    )
    create = EmbeddingFileSchemaCreate(
        reference_curie="AGRKB:101000000000001",
        profile_name="ai_curation_assistant_paragraph_chunk_with_filters",
        version=1,
        model_name="openai:text-embedding-3-small",
        source_referencefile_id=42,
    )
    assert create.version == 1
    show = EmbeddingFileSchemaShow(
        embedding_file_id=7,
        reference_curie="AGRKB:101000000000001",
        profile_name="abstract_document",
        version=1,
        model_name="openai:text-embedding-3-small",
        source_referencefile_id=None,
        parquet_referencefile_id=99,
    )
    assert show.embedding_file_id == 7 and show.source_referencefile_id is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/api/test_embedding_file.py::test_embedding_file_schemas_shapes -v`
Expected: FAIL — `ModuleNotFoundError: ... embedding_file_schemas`.

- [ ] **Step 3: Write the embedding_file schemas**

Create `agr_literature_service/api/schemas/embedding_file_schemas.py`:

```python
from typing import Optional

from pydantic import BaseModel, ConfigDict


class EmbeddingFileSchemaBase(BaseModel):
    """Embedding catalog semantics (no vectors, no recipe descriptor)."""
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    reference_curie: str
    profile_name: str
    version: int
    model_name: Optional[str] = None
    source_referencefile_id: Optional[int] = None


class EmbeddingFileSchemaCreate(EmbeddingFileSchemaBase):
    """Posted alongside the parquet file when registering an embedding.

    ``mod_abbreviation`` is forwarded to the parquet referencefile upload
    (referencefile_mod ownership); ``None`` = all-MOD (PMC).
    """
    mod_abbreviation: Optional[str] = None


class EmbeddingFileSchemaShow(EmbeddingFileSchemaBase):
    """Returned representation of a catalog row with its keys."""
    embedding_file_id: int
    parquet_referencefile_id: int
```

- [ ] **Step 4: Add the source sub-object schema and extend `ReferencefileSchemaRelated`**

In `agr_literature_service/api/schemas/referencefile_schemas.py`, after the `ReferencefileConvertedDerivedSchema` class (ends at line 103) add:

```python
class ReferencefileSourceSchema(BaseModel):
    """The referencefile a derived file was produced from (upward lineage).

    md/figure -> their source PDF (display-name-suffix convention);
    embedding -> its converted_merged_* markdown (embedding_file FK).
    """
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    referencefile_id: int
    display_name: str
    file_class: str
    file_extension: str
    md5sum: str
```

Then add these optional fields to `ReferencefileSchemaRelated` (after `referencefile_mods`, line 78):

```python
    # Upward lineage: the referencefile this one was derived from (nullable
    # when unresolved). Added to every derived file by show_all.
    source: Optional[ReferencefileSourceSchema] = None
    # Embedding-only fields (populated from embedding_file when this row is an
    # `embedding` parquet surfaced via ?include_embeddings=true).
    profile_name: Optional[str] = None
    version: Optional[int] = None
    model_name: Optional[str] = None
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/api/test_embedding_file.py::test_embedding_file_schemas_shapes -v`
Expected: PASS.

- [ ] **Step 6: Lint + commit**

```bash
make run-local-flake8 && make run-local-mypy
git add agr_literature_service/api/schemas/embedding_file_schemas.py agr_literature_service/api/schemas/referencefile_schemas.py tests/api/test_embedding_file.py
git commit -m "feat(embedding): add embedding_file + referencefile source schemas (SCRUM-6141)"
```

---

## Task 4: `embedding_file` CRUD

**Files:**
- Create: `agr_literature_service/api/crud/embedding_file_crud.py`
- Test: `tests/api/test_embedding_file.py`

**Interfaces:**
- Consumes: `EmbeddingFileModel` (Task 1); `EmbeddingFileSchemaCreate`/`Show` (Task 3); `referencefile_crud.file_upload_single(db, metadata: dict, file: UploadFile) -> ReferencefileModel`; `reference_utils.get_reference(db, curie_or_reference_id)`.
- Produces:
  - `create_or_update(db, request: EmbeddingFileSchemaCreate, file: UploadFile) -> EmbeddingFileModel` — uploads parquet via `file_upload_single`, upserts the catalog row on the unique key.
  - `get(db, embedding_file_id: int) -> EmbeddingFileModel`
  - `destroy(db, embedding_file_id: int) -> None`
  - `get_embeddings_for_source(db, source_referencefile_id: int) -> list[EmbeddingFileModel]`

- [ ] **Step 1: Write the failing test (upsert idempotency, with file storage mocked)**

Append to `tests/api/test_embedding_file.py`:

```python
import io
from unittest.mock import patch
from fastapi import UploadFile

from agr_literature_service.api.crud import embedding_file_crud
from agr_literature_service.api.schemas.embedding_file_schemas import EmbeddingFileSchemaCreate
from .test_reference import test_reference  # noqa


def _fake_parquet_referencefile(db, reference_id, fixed_id_holder):
    """Create a real ReferencefileModel row to stand in for the parquet that
    file_upload_single would have created + stored in S3."""
    rf = ReferencefileModel(
        reference_id=reference_id, display_name="src_md_profile_v1",
        file_class="embedding", file_publication_status="final",
        file_extension="parquet", md5sum="deadbeef", is_annotation=False,
    )
    db.add(rf)
    db.commit()
    db.refresh(rf)
    fixed_id_holder.append(rf.referencefile_id)
    return rf


def test_create_or_update_is_idempotent(db, test_reference):  # noqa
    curie = test_reference["curie"]
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    holder = []
    req = EmbeddingFileSchemaCreate(
        reference_curie=curie, profile_name="abstract_document",
        version=1, model_name="openai:text-embedding-3-small",
        source_referencefile_id=None,
    )
    upload = UploadFile(filename="e.parquet", file=io.BytesIO(b"PAR1data"))
    with patch.object(embedding_file_crud, "file_upload_single",
                      side_effect=lambda d, m, f: _fake_parquet_referencefile(db, ref.reference_id, holder)):
        row1 = embedding_file_crud.create_or_update(db, req, upload)
        row2 = embedding_file_crud.create_or_update(db, req, upload)
    assert row1.embedding_file_id == row2.embedding_file_id  # same row re-pointed, not duplicated
    assert db.query(EmbeddingFileModel).filter_by(
        reference_id=ref.reference_id, profile_name="abstract_document", version=1,
        source_referencefile_id=None).count() == 1
    assert row2.parquet_referencefile_id == holder[-1]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/api/test_embedding_file.py::test_create_or_update_is_idempotent -v`
Expected: FAIL — `ModuleNotFoundError: ... embedding_file_crud`.

- [ ] **Step 3: Write the CRUD**

Create `agr_literature_service/api/crud/embedding_file_crud.py`:

```python
from typing import List

from fastapi import HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_crud import file_upload_single
from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.models import EmbeddingFileModel
from agr_literature_service.api.schemas.embedding_file_schemas import EmbeddingFileSchemaCreate


def create_or_update(db: Session, request: EmbeddingFileSchemaCreate,
                     file: UploadFile) -> EmbeddingFileModel:
    """Store the parquet as an `embedding` referencefile and upsert the
    catalog row on the unique key (reference, profile_name, version,
    source_referencefile_id). Idempotent: re-running re-points the existing
    row at the (possibly new) parquet instead of duplicating."""
    reference = get_reference(db=db, curie_or_reference_id=request.reference_curie)

    # 1. Store the parquet via the existing single-file uploader (md5/S3/dedup,
    #    no main-PDF workflow-tag side effects).
    metadata = {
        "reference_curie": request.reference_curie,
        "mod_abbreviation": request.mod_abbreviation,
        "display_name": f"embedding_{request.profile_name}_v{request.version}",
        "file_class": "embedding",
        "file_publication_status": "final",
        "file_extension": "parquet",
        "pdf_type": None,
        "is_annotation": False,
    }
    parquet_rf = file_upload_single(db, metadata, file)

    # 2. Upsert the catalog row on the unique key.
    row = db.query(EmbeddingFileModel).filter(
        EmbeddingFileModel.reference_id == reference.reference_id,
        EmbeddingFileModel.profile_name == request.profile_name,
        EmbeddingFileModel.version == request.version,
        EmbeddingFileModel.source_referencefile_id == request.source_referencefile_id,
    ).one_or_none()
    if row is None:
        row = EmbeddingFileModel(
            reference_id=reference.reference_id,
            profile_name=request.profile_name,
            version=request.version,
            model_name=request.model_name,
            source_referencefile_id=request.source_referencefile_id,
            parquet_referencefile_id=parquet_rf.referencefile_id,
        )
        db.add(row)
    else:
        row.model_name = request.model_name
        row.parquet_referencefile_id = parquet_rf.referencefile_id
    db.commit()
    db.refresh(row)
    return row


def get(db: Session, embedding_file_id: int) -> EmbeddingFileModel:
    row = db.query(EmbeddingFileModel).filter(
        EmbeddingFileModel.embedding_file_id == embedding_file_id).one_or_none()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"embedding_file {embedding_file_id} not found")
    return row


def destroy(db: Session, embedding_file_id: int) -> None:
    row = get(db, embedding_file_id)
    db.delete(row)
    db.commit()


def get_embeddings_for_source(db: Session,
                              source_referencefile_id: int) -> List[EmbeddingFileModel]:
    return db.query(EmbeddingFileModel).filter(
        EmbeddingFileModel.source_referencefile_id == source_referencefile_id).all()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/api/test_embedding_file.py::test_create_or_update_is_idempotent -v`
Expected: PASS.

- [ ] **Step 5: Lint + commit**

```bash
make run-local-flake8 && make run-local-mypy
git add agr_literature_service/api/crud/embedding_file_crud.py tests/api/test_embedding_file.py
git commit -m "feat(embedding): add embedding_file CRUD with parquet upload reuse (SCRUM-6141)"
```

---

## Task 5: Extend `referencefile.show_all`

**Files:**
- Modify: `agr_literature_service/api/crud/referencefile_crud.py:244-253` (`show_all`)
- Modify: `agr_literature_service/api/routers/referencefile_router.py:185-191` (`show_all` route)
- Test: `tests/api/test_embedding_file.py`

**Interfaces:**
- Consumes: `EmbeddingFileModel`, `embedding_file_crud.get_embeddings_for_source`; existing `_find_converted_derived_for_source`, `set_referencefile_mods`, `get_reference`.
- Produces: `show_all(db, curie_or_reference_id, include_embeddings=False, profile_name=None, version=None)`. Each returned dict gains `source` (or `None`); embedding rows are excluded unless `include_embeddings`, and when included carry `profile_name`/`version`/`model_name` + `source` (their markdown).

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_embedding_file.py` (uses the API so it exercises the route params):

```python
from starlette.testclient import TestClient
from agr_literature_service.api.main import app
from .fixtures import auth_headers  # noqa


def test_show_all_excludes_embeddings_by_default(db, test_reference, auth_headers):  # noqa
    curie = test_reference["curie"]
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    md = ReferencefileModel(reference_id=ref.reference_id, display_name="paper_main",
                            file_class="converted_merged_main", file_publication_status="final",
                            file_extension="md", md5sum="md5md", is_annotation=False)
    pq = ReferencefileModel(reference_id=ref.reference_id, display_name="emb_v1",
                            file_class="embedding", file_publication_status="final",
                            file_extension="parquet", md5sum="md5pq", is_annotation=False)
    db.add_all([md, pq]); db.commit(); db.refresh(md); db.refresh(pq)
    db.add(EmbeddingFileModel(reference_id=ref.reference_id, profile_name="abstract_document",
                              version=1, model_name="openai:text-embedding-3-small",
                              source_referencefile_id=md.referencefile_id,
                              parquet_referencefile_id=pq.referencefile_id))
    db.commit()
    with TestClient(app) as client:
        default = client.get(f"/reference/referencefile/show_all/{curie}", headers=auth_headers).json()
        assert all(e["file_class"] != "embedding" for e in default)
        withemb = client.get(
            f"/reference/referencefile/show_all/{curie}?include_embeddings=true",
            headers=auth_headers).json()
        emb = [e for e in withemb if e["file_class"] == "embedding"]
        assert len(emb) == 1
        assert emb[0]["profile_name"] == "abstract_document" and emb[0]["version"] == 1
        assert emb[0]["source"]["referencefile_id"] == md.referencefile_id
        assert emb[0]["source"]["md5sum"] == "md5md"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/api/test_embedding_file.py::test_show_all_excludes_embeddings_by_default -v`
Expected: FAIL — embeddings present by default / `source` missing / `profile_name` KeyError.

- [ ] **Step 3: Add the upward source resolver**

In `agr_literature_service/api/crud/referencefile_crud.py`, after `_find_converted_derived_for_source` (ends line 172) add:

```python
# Reverse of _CONVERTED_FILE_CLASS_FOR_SOURCE: a derived md's file_class ->
# the source PDF file_class it was produced from.
_SOURCE_FILE_CLASS_FOR_CONVERTED = {
    v: k for k, v in _CONVERTED_FILE_CLASS_FOR_SOURCE.items()
}


def _find_source_for_derived(db: Session, ref_file, embedding_source_by_id) -> dict:
    """Resolve the referencefile a derived file was produced from (upward).

    - embedding -> its converted_merged_* md, via the embedding_file FK
      (precomputed in ``embedding_source_by_id``: parquet rf id -> source rf).
    - converted_merged_* md / figure -> its source PDF, via the display-name
      suffix convention (reverse of _find_converted_derived_for_source).
    Returns the source dict or None.
    """
    if ref_file.file_class == "embedding":
        src = embedding_source_by_id.get(int(ref_file.referencefile_id))
        if src is None:
            return None
        return {
            "referencefile_id": int(src.referencefile_id),
            "display_name": src.display_name,
            "file_class": src.file_class,
            "file_extension": src.file_extension,
            "md5sum": src.md5sum,
        }
    source_class = _SOURCE_FILE_CLASS_FOR_CONVERTED.get(ref_file.file_class)
    if source_class is None:
        return None
    display_name = ref_file.display_name or ""
    for suffix in _CONVERTED_DISPLAY_NAME_SUFFIXES:
        if display_name.endswith(suffix):
            base = display_name[: len(display_name) - len(suffix)]
            for cand in ref_file.reference.referencefiles or []:
                if cand.file_class == source_class and cand.display_name == base:
                    return {
                        "referencefile_id": int(cand.referencefile_id),
                        "display_name": cand.display_name,
                        "file_class": cand.file_class,
                        "file_extension": cand.file_extension,
                        "md5sum": cand.md5sum,
                    }
    return None
```

> NOTE during implementation: confirm `_CONVERTED_DISPLAY_NAME_SUFFIXES` / `_CONVERTED_FILE_CLASS_FOR_SOURCE` are module-level in `referencefile_crud.py` (they are referenced by `_find_converted_derived_for_source`). Figures use the `_image_NNN` convention, not these suffixes, so figure->PDF lineage resolves only when a matching suffix exists; leave figure `source` as `None` otherwise (acceptable per spec — `source` is nullable).

- [ ] **Step 4: Rewrite `show_all`**

Replace `show_all` (lines 244-253) with:

```python
def show_all(db: Session, curie_or_reference_id: str,
             include_embeddings: bool = False,
             profile_name: Optional[str] = None,
             version: Optional[int] = None) -> List[ReferencefileSchemaRelated]:
    logger.info("Show all referencefiles")
    reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id,
                              load_referencefiles=True)
    all_files = list(reference.referencefiles or [])

    # Map embedding parquet referencefile_id -> its embedding_file row, so we
    # can both filter and enrich embedding entries without per-row queries.
    parquet_ids = [int(rf.referencefile_id) for rf in all_files
                   if rf.file_class == "embedding"]
    emb_by_parquet = {}
    source_by_parquet = {}
    if parquet_ids:
        rf_by_id = {int(rf.referencefile_id): rf for rf in all_files}
        rows = db.query(EmbeddingFileModel).filter(
            EmbeddingFileModel.parquet_referencefile_id.in_(parquet_ids)).all()
        for row in rows:
            emb_by_parquet[row.parquet_referencefile_id] = row
            if row.source_referencefile_id is not None:
                source_by_parquet[row.parquet_referencefile_id] = rf_by_id.get(
                    int(row.source_referencefile_id))

    reference_files = []
    for ref_file in all_files:
        is_embedding = ref_file.file_class == "embedding"
        if is_embedding and not include_embeddings:
            continue
        emb_row = emb_by_parquet.get(int(ref_file.referencefile_id)) if is_embedding else None
        if is_embedding and emb_row is not None:
            if profile_name is not None and emb_row.profile_name != profile_name:
                continue
            if version is not None and emb_row.version != version:
                continue
        ref_file_dict = jsonable_encoder(ref_file)
        set_referencefile_mods(referencefile_obj=ref_file, referencefile_dict=ref_file_dict)
        ref_file_dict["source"] = _find_source_for_derived(db, ref_file, source_by_parquet)
        if is_embedding and emb_row is not None:
            ref_file_dict["profile_name"] = emb_row.profile_name
            ref_file_dict["version"] = emb_row.version
            ref_file_dict["model_name"] = emb_row.model_name
        reference_files.append(ref_file_dict)
    return reference_files
```

Add the imports at the top of `referencefile_crud.py` (alongside existing model imports):

```python
from agr_literature_service.api.models import EmbeddingFileModel
```

(`Optional` is already imported via `from typing import ...` in this module; confirm and add if missing.)

- [ ] **Step 5: Add the route params**

In `agr_literature_service/api/routers/referencefile_router.py`, update the `show_all` endpoint (lines 185-191) signature to forward the params:

```python
@router.get('/show_all/{curie_or_reference_id}',
            response_model=List[ReferencefileSchemaRelated],
            status_code=200)
def show_all(curie_or_reference_id: str,
             include_embeddings: bool = False,
             profile_name: Optional[str] = None,
             version: Optional[int] = None,
             db: Session = db_session,
             user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    return referencefile_crud.show_all(db, curie_or_reference_id,
                                       include_embeddings=include_embeddings,
                                       profile_name=profile_name, version=version)
```

(Match the existing decorator/return exactly — only the signature + crud call change. Keep the existing `response_model`.)

- [ ] **Step 6: Run test to verify it passes**

Run: `pytest tests/api/test_embedding_file.py::test_show_all_excludes_embeddings_by_default -v`
Then run the existing referencefile suite to confirm no regression:
`pytest tests/api/test_referencefile.py -v`
Expected: PASS (existing `test_show_all` still passes — non-embedding rows now also carry `source`, which is additive).

- [ ] **Step 7: Lint + commit**

```bash
make run-local-flake8 && make run-local-mypy
git add agr_literature_service/api/crud/referencefile_crud.py agr_literature_service/api/routers/referencefile_router.py tests/api/test_embedding_file.py
git commit -m "feat(embedding): surface embeddings + source lineage in referencefile show_all (SCRUM-6141)"
```

---

## Task 6: Extend `conversion_request` with an `embeddings` list

**Files:**
- Modify: `agr_literature_service/api/schemas/file_conversion_schemas.py:41-63` (`ConversionPerFileProgressSchema`)
- Modify: `agr_literature_service/api/crud/file_conversion_crud.py` (add `_attach_embeddings`; call in `_status_payload` ~line 558)
- Test: `tests/api/test_embedding_file.py`

**Interfaces:**
- Consumes: `embedding_file_crud.get_embeddings_for_source`; the existing `_status_payload` / `_attach_figures` pattern.
- Produces: each `per_file_progress` entry gains `embeddings: List[ConversionEmbeddingInfo]` — `{parquet_referencefile_id:int, profile_name:str, version:int}` — the embedding files whose `source_referencefile_id` is that entry's `converted.referencefile_id`.

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_embedding_file.py`:

```python
from agr_literature_service.api.crud import file_conversion_crud


def test_attach_embeddings_links_by_source_md(db, test_reference):  # noqa
    curie = test_reference["curie"]
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    md = ReferencefileModel(reference_id=ref.reference_id, display_name="p_main",
                            file_class="converted_merged_main", file_publication_status="final",
                            file_extension="md", md5sum="m1", is_annotation=False)
    pq = ReferencefileModel(reference_id=ref.reference_id, display_name="p_emb",
                            file_class="embedding", file_publication_status="final",
                            file_extension="parquet", md5sum="p1", is_annotation=False)
    db.add_all([md, pq]); db.commit(); db.refresh(md); db.refresh(pq)
    db.add(EmbeddingFileModel(reference_id=ref.reference_id, profile_name="classifier_fulltext_document",
                              version=2, model_name="openai:text-embedding-3-small",
                              source_referencefile_id=md.referencefile_id,
                              parquet_referencefile_id=pq.referencefile_id))
    db.commit(); db.refresh(ref)
    progress = [{"source": None,
                 "converted": {"display_name": "p_main", "file_class": "converted_merged_main",
                               "referencefile_id": md.referencefile_id},
                 "figures": [], "status": "success", "error": None}]
    file_conversion_crud._attach_embeddings(db, ref, progress)
    assert progress[0]["embeddings"] == [
        {"parquet_referencefile_id": pq.referencefile_id,
         "profile_name": "classifier_fulltext_document", "version": 2}
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/api/test_embedding_file.py::test_attach_embeddings_links_by_source_md -v`
Expected: FAIL — `AttributeError: module ... has no attribute '_attach_embeddings'`.

- [ ] **Step 3: Add the schema field**

In `agr_literature_service/api/schemas/file_conversion_schemas.py`, add a new info class after `ConversionFileInfo` (line 12) and the field on `ConversionPerFileProgressSchema` (after `figures`, line 61):

```python
class ConversionEmbeddingInfo(BaseModel):
    model_config = ConfigDict(extra='forbid', from_attributes=True)

    parquet_referencefile_id: int
    profile_name: str
    version: int
```

```python
    embeddings: List[ConversionEmbeddingInfo] = []
```

- [ ] **Step 4: Add `_attach_embeddings` and call it**

In `agr_literature_service/api/crud/file_conversion_crud.py`, after `_attach_figures` (ends line 502) add:

```python
def _attach_embeddings(db: Session, reference: ReferenceModel,
                       progress: List[Dict[str, Any]]) -> None:
    """Mutate ``progress`` in place: set each entry's ``embeddings`` list to
    the embedding_file rows whose source_referencefile_id is that entry's
    converted md row. Same 'derived files for this entry' pattern as figures,
    one step further down the lineage (md -> embeddings)."""
    from agr_literature_service.api.crud.embedding_file_crud import (
        get_embeddings_for_source,
    )
    for entry in progress:
        conv = entry.get("converted") or {}
        conv_id = conv.get("referencefile_id")
        if conv_id is None:
            entry["embeddings"] = []
            continue
        rows = get_embeddings_for_source(db, int(conv_id))
        entry["embeddings"] = [
            {"parquet_referencefile_id": int(r.parquet_referencefile_id),
             "profile_name": r.profile_name, "version": r.version}
            for r in rows
        ]
```

Then in `_status_payload`, immediately after the existing `_attach_figures(reference, progress)` call (line 558), add:

```python
    _attach_embeddings(db, reference, progress)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/api/test_embedding_file.py::test_attach_embeddings_links_by_source_md -v`
Then confirm no regression in the conversion suite: `pytest tests/api/test_file_conversion.py -v` (if present).
Expected: PASS.

- [ ] **Step 6: Lint + commit**

```bash
make run-local-flake8 && make run-local-mypy
git add agr_literature_service/api/schemas/file_conversion_schemas.py agr_literature_service/api/crud/file_conversion_crud.py tests/api/test_embedding_file.py
git commit -m "feat(embedding): report embeddings per converted file in conversion_request (SCRUM-6141)"
```

---

## Pending decision (track, do not block)

Chris Tabone was asked (Slack, 2026-06-24) whether `embedding_dim` + `chunk_count` must be queryable columns on `embedding_file` or can live in the parquet metadata. This plan builds the **lean** table. If Chris needs them: add two nullable columns (`embedding_dim INTEGER`, `chunk_count INTEGER`) to `EmbeddingFileModel`, a small additive migration, the two fields on `EmbeddingFileSchemaShow`, and surface them in the `show_all` embedding entries. Purely additive — no rework of Tasks 1-6.

---

## Self-Review

**1. Spec coverage (Phase 1 of the revised plan):**
- `embedding_file` table, plain Base non-audited, the 7 columns, two partial unique indexes → Tasks 1-2. ✅
- Parquet stored as `embedding` referencefile via existing upload → Task 4 (`file_upload_single`). ✅
- `show_all` upward `source` sub-object on derived files + `?include_embeddings=true` + `profile_name`/`version` filters → Task 5. ✅
- `conversion_request` `embeddings` list per `per_file_progress` → Task 6. ✅
- Download reuses existing `download_file/{referencefile_id}` → no change needed (documented in Architecture). ✅
- `embedding` file_class registered → free (no enum); used in Task 4 metadata. ✅
- Consumer-exclusion audit → intentionally **out of scope** (user decision). ✅
- `embedding_dim`/`chunk_count` → intentionally omitted pending Chris; tracked above. ✅

**2. Placeholder scan:** No TBD/TODO; every code step has full code; test code is concrete. ✅

**3. Type consistency:** `create_or_update`/`get`/`destroy`/`get_embeddings_for_source` signatures used in Tasks 5-6 match Task 4 definitions; `ReferencefileSourceSchema` fields (Task 3) match the dict built by `_find_source_for_derived` (Task 5); `ConversionEmbeddingInfo` fields (Task 6 schema) match the dict built by `_attach_embeddings`. ✅

**Implementation-time verifications flagged:** (a) autogenerate emitting `postgresql_where` (Task 2 Step 3); (b) `_CONVERTED_DISPLAY_NAME_SUFFIXES`/`_CONVERTED_FILE_CLASS_FOR_SOURCE` are module-level (Task 5 Step 3); (c) `Optional` import present in `referencefile_crud.py` (Task 5 Step 4); (d) existing `test_referencefile.py::test_show_all` still green after adding `source` (Task 5 Step 6).
