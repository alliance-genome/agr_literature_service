import io
from unittest.mock import patch

import pytest
from fastapi import HTTPException, UploadFile
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    EmbeddingFileModel,
    ReferenceModel,
    ReferencefileModel,
)
from agr_literature_service.api.crud import embedding_file_crud, file_conversion_crud
from agr_literature_service.api.schemas.embedding_file_schemas import (
    EmbeddingFileSchemaCreate,
)
from .fixtures import auth_headers  # noqa
from .test_reference import test_reference  # noqa
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


def test_embedding_file_schemas_shapes():
    from agr_literature_service.api.schemas.embedding_file_schemas import (
        EmbeddingFileSchemaCreate,
        EmbeddingFileSchemaShow,
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


def _fake_parquet_referencefile(db, reference_id, fixed_id_holder, md5sum="deadbeef"):  # noqa
    """Stand-in for file_upload_single: md5-dedups like the real uploader, so a
    re-post of identical content returns the existing parquet row instead of
    inserting a duplicate (which would violate idx_reference_id_display_name).
    A different md5sum stands in for changed content -> a new parquet row."""
    existing = db.query(ReferencefileModel).filter_by(
        reference_id=reference_id, md5sum=md5sum).one_or_none()
    if existing is not None:
        fixed_id_holder.append(existing.referencefile_id)
        return existing
    rf = ReferencefileModel(
        reference_id=reference_id, display_name=f"src_md_profile_v1_{md5sum}",
        file_class="embedding", file_publication_status="final",
        file_extension="parquet", md5sum=md5sum, is_annotation=False,
    )
    db.add(rf)
    db.commit()
    db.refresh(rf)
    fixed_id_holder.append(rf.referencefile_id)
    return rf


def test_create_or_update_is_idempotent(db, test_reference):  # noqa
    curie = test_reference.new_ref_curie
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


def test_show_all_always_includes_embeddings(db, test_reference, auth_headers):  # noqa
    """show_all returns EVERY file with no opt-in: the markdown and the embedding
    parquet (annotated with catalog fields + source lineage)."""
    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    md = ReferencefileModel(reference_id=ref.reference_id, display_name="paper_main",
                            file_class="converted_merged_main", file_publication_status="final",
                            file_extension="md", md5sum="md5md", is_annotation=False)
    pq = ReferencefileModel(reference_id=ref.reference_id, display_name="emb_v1",
                            file_class="embedding", file_publication_status="final",
                            file_extension="parquet", md5sum="md5pq", is_annotation=False)
    db.add_all([md, pq])
    db.commit()
    db.refresh(md)
    db.refresh(pq)
    db.add(EmbeddingFileModel(reference_id=ref.reference_id, profile_name="abstract_document",
                              version=1, model_name="openai:text-embedding-3-small",
                              source_referencefile_id=md.referencefile_id,
                              parquet_referencefile_id=pq.referencefile_id))
    db.commit()
    with TestClient(app) as client:
        files = client.get(f"/reference/referencefile/show_all/{curie}", headers=auth_headers).json()
    assert any(e["file_class"] == "converted_merged_main" for e in files)
    emb = [e for e in files if e["file_class"] == "embedding"]
    assert len(emb) == 1
    assert emb[0]["profile_name"] == "abstract_document" and emb[0]["version"] == 1
    assert emb[0]["model_name"] == "openai:text-embedding-3-small"
    assert emb[0]["source"]["referencefile_id"] == md.referencefile_id
    assert emb[0]["source"]["md5sum"] == "md5md"


def test_create_or_update_rejects_invalid_source_referencefile_id(db, test_reference):  # noqa
    """A non-existent source_referencefile_id is rejected up front (422) before
    the parquet is uploaded, so no orphaned parquet is left behind."""
    curie = test_reference.new_ref_curie
    req = EmbeddingFileSchemaCreate(
        reference_curie=curie, profile_name="abstract_document",
        version=1, source_referencefile_id=999999999)
    upload = UploadFile(filename="e.parquet", file=io.BytesIO(b"PAR1data"))
    uploaded = []
    with patch.object(embedding_file_crud, "file_upload_single",
                      side_effect=lambda *a, **k: uploaded.append(1)):
        with pytest.raises(HTTPException) as exc:
            embedding_file_crud.create_or_update(db, req, upload)
    assert exc.value.status_code == 422
    assert not uploaded  # validated before upload — no parquet, no orphan


def test_create_or_update_deletes_superseded_parquet_on_repoint(db, test_reference):  # noqa
    """Re-posting changed content for the same key re-points the catalog row at
    the new parquet and deletes the previous (now unreferenced) one (review #1)."""
    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    holder = []
    req = EmbeddingFileSchemaCreate(
        reference_curie=curie, profile_name="abstract_document", version=1,
        model_name="m", source_referencefile_id=None)
    upload = UploadFile(filename="e.parquet", file=io.BytesIO(b"x"))
    deleted = []
    with patch.object(embedding_file_crud, "remove_from_s3_and_db",
                      side_effect=lambda d, rf: deleted.append(rf.referencefile_id)):
        with patch.object(embedding_file_crud, "file_upload_single",
                          side_effect=lambda d, m, f: _fake_parquet_referencefile(
                              db, ref.reference_id, holder, md5sum="aaa")):
            row1 = embedding_file_crud.create_or_update(db, req, upload)
        old_id = row1.parquet_referencefile_id
        with patch.object(embedding_file_crud, "file_upload_single",
                          side_effect=lambda d, m, f: _fake_parquet_referencefile(
                              db, ref.reference_id, holder, md5sum="bbb")):
            row2 = embedding_file_crud.create_or_update(db, req, upload)
    assert row1.embedding_file_id == row2.embedding_file_id      # same catalog row
    assert row2.parquet_referencefile_id != old_id              # re-pointed to new parquet
    assert deleted == [old_id]                                  # superseded parquet cleaned up


def test_create_endpoint_registers_embedding(db, test_reference, auth_headers):  # noqa
    """POST /reference/embedding_file/ uploads the parquet + upserts the catalog
    row, returning it serialized as EmbeddingFileSchemaShow (curie-keyed)."""
    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    holder = []
    # Drop Content-Type so the client sets the multipart boundary itself.
    upload_headers = auth_headers.copy()
    upload_headers.pop("Content-Type", None)
    with patch.object(embedding_file_crud, "file_upload_single",
                      side_effect=lambda d, m, f: _fake_parquet_referencefile(db, ref.reference_id, holder)):
        with TestClient(app) as client:
            resp = client.post(
                "/reference/embedding_file/",
                params={"reference_curie": curie, "profile_name": "abstract_document",
                        "version": 1, "model_name": "openai:text-embedding-3-small"},
                headers=upload_headers,
                files={"file": ("e.parquet", io.BytesIO(b"PAR1data"), "application/octet-stream")},
            )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["reference_curie"] == curie  # @property mapping from reference_id
    assert body["profile_name"] == "abstract_document" and body["version"] == 1
    assert body["parquet_referencefile_id"] == holder[-1]
    assert body["embedding_file_id"] > 0


def test_get_endpoint_returns_catalog_row(db, test_reference, auth_headers):  # noqa
    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    pq = ReferencefileModel(reference_id=ref.reference_id, display_name="emb_get_v1",
                            file_class="embedding", file_publication_status="final",
                            file_extension="parquet", md5sum="md5get", is_annotation=False)
    db.add(pq)
    db.commit()
    db.refresh(pq)
    row = EmbeddingFileModel(
        reference_id=ref.reference_id, profile_name="abstract_document",
        version=1, model_name="openai:text-embedding-3-small",
        source_referencefile_id=None,
        parquet_referencefile_id=pq.referencefile_id)
    db.add(row)
    db.commit()
    db.refresh(row)
    with TestClient(app) as client:
        resp = client.get(f"/reference/embedding_file/{row.embedding_file_id}", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["embedding_file_id"] == row.embedding_file_id
    assert body["reference_curie"] == curie
    assert body["parquet_referencefile_id"] == pq.referencefile_id


def test_attach_embeddings_links_by_source_md(db, test_reference):  # noqa
    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    md = ReferencefileModel(reference_id=ref.reference_id, display_name="p_main",
                            file_class="converted_merged_main", file_publication_status="final",
                            file_extension="md", md5sum="m1", is_annotation=False)
    pq = ReferencefileModel(reference_id=ref.reference_id, display_name="p_emb",
                            file_class="embedding", file_publication_status="final",
                            file_extension="parquet", md5sum="p1", is_annotation=False)
    db.add_all([md, pq])
    db.commit()
    db.refresh(md)
    db.refresh(pq)
    db.add(EmbeddingFileModel(reference_id=ref.reference_id, profile_name="classifier_fulltext_document",
                              version=2, model_name="openai:text-embedding-3-small",
                              source_referencefile_id=md.referencefile_id,
                              parquet_referencefile_id=pq.referencefile_id))
    db.commit()
    db.refresh(ref)
    progress = [{"source": None,
                 "converted": {"display_name": "p_main", "file_class": "converted_merged_main",
                               "referencefile_id": md.referencefile_id},
                 "figures": [], "status": "success", "error": None}]
    file_conversion_crud._attach_embeddings(db, ref, progress)
    assert progress[0]["embeddings"] == [
        {"parquet_referencefile_id": pq.referencefile_id,
         "profile_name": "classifier_fulltext_document", "version": 2}
    ]


def test_attach_embeddings_resolves_when_converted_id_missing(db, test_reference):  # noqa
    """Job-recorded progress entries may omit converted.referencefile_id; the
    embeddings must be resolved from (display_name, file_class) rather than
    silently dropped (review #2)."""
    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    md = ReferencefileModel(reference_id=ref.reference_id, display_name="p_main2",
                            file_class="converted_merged_main", file_publication_status="final",
                            file_extension="md", md5sum="m2", is_annotation=False)
    pq = ReferencefileModel(reference_id=ref.reference_id, display_name="p_emb2",
                            file_class="embedding", file_publication_status="final",
                            file_extension="parquet", md5sum="p2", is_annotation=False)
    db.add_all([md, pq])
    db.commit()
    db.refresh(md)
    db.refresh(pq)
    db.add(EmbeddingFileModel(reference_id=ref.reference_id, profile_name="classifier_fulltext_document",
                              version=2, model_name="m",
                              source_referencefile_id=md.referencefile_id,
                              parquet_referencefile_id=pq.referencefile_id))
    db.commit()
    db.refresh(ref)
    # converted entry carries NO referencefile_id (only display_name/file_class)
    progress = [{"source": None,
                 "converted": {"display_name": "p_main2", "file_class": "converted_merged_main"},
                 "figures": [], "status": "success", "error": None}]
    file_conversion_crud._attach_embeddings(db, ref, progress)
    assert progress[0]["embeddings"] == [
        {"parquet_referencefile_id": pq.referencefile_id,
         "profile_name": "classifier_fulltext_document", "version": 2}
    ]
    assert "_conv_id" not in progress[0]  # scratch key cleaned up


def test_delete_source_md_cleans_up_embedding_parquet(db, test_reference):  # noqa
    """Deleting a source markdown referencefile removes its derived embeddings'
    parquets too (no orphan), not just the cascade-deleted catalog rows (#1)."""
    from agr_literature_service.api.crud import referencefile_crud
    from agr_cognito_py import ModAccess
    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    md = ReferencefileModel(reference_id=ref.reference_id, display_name="src_md",
                            file_class="converted_merged_main", file_publication_status="final",
                            file_extension="md", md5sum="srcmd5", is_annotation=False)
    pq = ReferencefileModel(reference_id=ref.reference_id, display_name="emb_pq",
                            file_class="embedding", file_publication_status="final",
                            file_extension="parquet", md5sum="embpq5", is_annotation=False)
    db.add_all([md, pq])
    db.commit()
    db.refresh(md)
    db.refresh(pq)
    md_id, pq_id = md.referencefile_id, pq.referencefile_id
    db.add(EmbeddingFileModel(reference_id=ref.reference_id, profile_name="p", version=1,
                              source_referencefile_id=md_id, parquet_referencefile_id=pq_id))
    db.commit()
    with patch("agr_literature_service.api.crud.referencefile_utils.remove_file_from_s3"):
        referencefile_crud.destroy(db, md_id, ModAccess.ALL_ACCESS)
    assert db.query(EmbeddingFileModel).filter_by(source_referencefile_id=md_id).count() == 0
    assert db.query(ReferencefileModel).filter_by(referencefile_id=pq_id).count() == 0  # parquet gone, not orphaned
    assert db.query(ReferencefileModel).filter_by(referencefile_id=md_id).count() == 0


def test_create_or_update_falls_back_on_insert_race(db, test_reference):  # noqa
    """A lost insert race (concurrent same-key POST) re-points the winner's row
    instead of raising a 500, and the loser's parquet is not orphaned (#3)."""
    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    holder = []
    winner_pq = _fake_parquet_referencefile(db, ref.reference_id, holder, md5sum="win")
    winner = EmbeddingFileModel(reference_id=ref.reference_id, profile_name="p", version=1,
                                model_name="m0", source_referencefile_id=None,
                                parquet_referencefile_id=winner_pq.referencefile_id)
    db.add(winner)
    db.commit()
    db.refresh(winner)
    winner_id, winner_pq_id = winner.embedding_file_id, winner_pq.referencefile_id

    req = EmbeddingFileSchemaCreate(reference_curie=curie, profile_name="p", version=1,
                                    model_name="m1", source_referencefile_id=None)
    upload = UploadFile(filename="e.parquet", file=io.BytesIO(b"x"))
    with patch("agr_literature_service.api.crud.referencefile_utils.remove_file_from_s3"), \
            patch.object(embedding_file_crud, "file_upload_single",
                         side_effect=lambda d, m, f: _fake_parquet_referencefile(
                             db, ref.reference_id, holder, md5sum="loser")), \
            patch.object(embedding_file_crud, "_find_existing", side_effect=[None, winner]):
        result = embedding_file_crud.create_or_update(db, req, upload)
    loser_pq_id = holder[-1]
    assert result.embedding_file_id == winner_id          # winner's row re-pointed, no duplicate/500
    assert result.parquet_referencefile_id == loser_pq_id  # re-pointed to the uploaded parquet
    # the winner's superseded parquet is cleaned up, not orphaned
    assert db.query(ReferencefileModel).filter_by(referencefile_id=winner_pq_id).count() == 0
