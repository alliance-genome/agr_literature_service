"""Tests for the SCRUM-6142 embedding generation core.

Two always-on tests prove the safe defaults (no key / stack unavailable -> the
conversion job is unaffected) and the "merged Markdown only" source rule. The
full end-to-end test needs the embeddings extra
(agr-abc-document-parsers[embeddings], pinned in requirements.txt) and skips
in environments installed without it.
"""

from unittest.mock import patch

import pytest

from agr_literature_service.api.models import ReferenceModel, ReferencefileModel
from agr_literature_service.lit_processing.embedding import embedding_generation as eg
from ..api.test_reference import test_reference  # noqa
from ..fixtures import db  # noqa


def test_merged_only_source_classes():
    """Only the merged Markdown is embedded — never the per-method
    grobid/marker/docling outputs."""
    assert eg.MERGED_SOURCE_FILE_CLASSES == (
        "converted_merged_main", "converted_merged_supplement"
    )
    assert not any(m in cls for cls in eg.MERGED_SOURCE_FILE_CLASSES
                   for m in ("grobid", "marker", "docling"))


def test_skips_without_api_key():
    """No OPENAI_API_KEY -> no-op (returns before touching the DB or the stack)."""
    with patch.object(eg.config, "OPENAI_API_KEY", None):
        result = eg.generate_classifier_embeddings_for_reference(db=None, reference_id=1)
    assert result == {"skipped": "no_api_key"}


def test_skips_when_stack_unavailable():
    """With a key set but the embedding stack import failing, the step is a
    logged no-op — the conversion job is never broken by a missing dep."""
    real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("agr_abc_document_parsers.embeddings"):
            raise ImportError("embeddings extra not installed")
        return real_import(name, *args, **kwargs)

    with patch.object(eg.config, "OPENAI_API_KEY", "sk-test"), \
            patch("builtins.__import__", side_effect=fake_import):
        result = eg.generate_classifier_embeddings_for_reference(db=None, reference_id=1)
    assert result == {"skipped": "deps_unavailable"}


class _FakeEmbedder:
    """Deterministic stand-in for OpenAIEmbedder (no network)."""

    model = "text-embedding-3-small"
    model_name = "openai:text-embedding-3-small"
    dimension = 4

    def __init__(self, *_args, **_kwargs):
        pass

    def embed(self, texts):
        return [[float(len(t)), 0.0, 1.0, 2.0] for t in texts]

    def embed_chunks(self, chunks):
        for c in chunks:
            c.embedding = [float(len(c.content)), 0.0, 1.0, 2.0]

    def count_tokens(self, text):
        return len((text or "").split())

    def truncate_to_limit(self, text):
        return text


MERGED_MD = """# daf-16 regulates longevity

## Abstract

We studied daf-16 and found it extends lifespan in N2 worms.

## Introduction

The daf-16 gene was studied. We measured expression in N2 worms.

## References

1. Some Author. A paper title. Journal 2020.
"""


@pytest.mark.webtest
def test_end_to_end_embeds_each_merged_markdown(db, test_reference):  # noqa
    """Every merged Markdown (main + supplement) is embedded + registered once;
    a second run skips the already-embedded sources. Gated on the embeddings
    extra (shared package release)."""
    pytest.importorskip("agr_abc_document_parsers.embeddings",
                        reason="embeddings extra / shared release not installed")
    from agr_literature_service.api.crud import embedding_file_crud
    from agr_literature_service.api.models import EmbeddingFileModel

    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()

    # one merged-main + one merged-supplement (embedded) and one method-specific
    # grobid output (must be ignored).
    main = ReferencefileModel(reference_id=ref.reference_id, display_name="paper_merged",
                              file_class="converted_merged_main", file_publication_status="final",
                              file_extension="md", md5sum="mainmd5", is_annotation=False)
    supp = ReferencefileModel(reference_id=ref.reference_id, display_name="supp_merged",
                              file_class="converted_merged_supplement", file_publication_status="final",
                              file_extension="md", md5sum="suppmd5", is_annotation=False)
    grobid = ReferencefileModel(reference_id=ref.reference_id, display_name="paper_grobid",
                                file_class="converted_grobid_main", file_publication_status="final",
                                file_extension="md", md5sum="grobidmd5", is_annotation=False)
    db.add_all([main, supp, grobid])
    db.commit()

    def fake_upload(_db, metadata, _file):
        rf = ReferencefileModel(
            reference_id=ref.reference_id, display_name=metadata["display_name"],
            file_class="embedding", file_publication_status="final",
            file_extension="parquet", md5sum=metadata["display_name"], is_annotation=False)
        db.add(rf)
        db.commit()
        db.refresh(rf)
        return rf

    with patch.object(eg.config, "OPENAI_API_KEY", "sk-test"), \
            patch("agr_literature_service.lit_processing.embedding.openai_embedder.OpenAIEmbedder",
                  _FakeEmbedder), \
            patch.object(eg, "download_file", return_value=MERGED_MD), \
            patch.object(embedding_file_crud, "file_upload_single", side_effect=fake_upload):
        result = eg.generate_classifier_embeddings_for_reference(db, ref.reference_id, curie)
        assert result["embedded"] == 2 and result["skipped_existing"] == 0
        # only the two merged sources embedded (grobid ignored)
        rows = db.query(EmbeddingFileModel).filter_by(reference_id=ref.reference_id).all()
        assert len(rows) == 2
        assert {r.source_referencefile_id for r in rows} == {
            main.referencefile_id, supp.referencefile_id}
        # second run is idempotent
        result2 = eg.generate_classifier_embeddings_for_reference(db, ref.reference_id, curie)
        assert result2["embedded"] == 0 and result2["skipped_existing"] == 2
