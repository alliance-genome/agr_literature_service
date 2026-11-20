"""Tests for the SCRUM-6142 embedding generation core.

Always-on tests prove the safe defaults (no key / stack unavailable -> the
conversion job is unaffected), the "merged Markdown only" source rule, and the
SCRUM-6273 classifier-MOD filter skip. The full end-to-end tests need the
embeddings extra (agr-abc-document-parsers[embeddings], pinned in
requirements.txt) and skip in environments installed without it.
"""

from unittest.mock import MagicMock, patch

import pytest

from agr_literature_service.api.models import (
    ModCorpusAssociationModel,
    ModModel,
    ReferenceModel,
    ReferencefileModel,
)
from agr_literature_service.api.schemas.mod_corpus_sort_source_type import ModCorpusSortSourceType
from agr_literature_service.lit_processing.embedding import embedding_generation as eg
from ..api.test_reference import test_reference  # noqa
from ..fixtures import db  # noqa


def _add_classifier_mod_corpus(db, reference_id):  # noqa
    """Put a reference in a classifier MOD's (WB) corpus so it passes the
    SCRUM-6273 filter. Reuses an existing WB row if the test DB already has one."""
    mod = db.query(ModModel).filter(ModModel.abbreviation == "WB").first()
    if mod is None:
        mod = ModModel(abbreviation="WB", short_name="WB", full_name="WormBase")
        db.add(mod)
        db.commit()
        db.refresh(mod)
    db.add(ModCorpusAssociationModel(
        reference_id=reference_id, mod_id=mod.mod_id, corpus=True,
        mod_corpus_sort_source=ModCorpusSortSourceType.Dqm_files,
    ))
    db.commit()


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


def test_skips_when_no_classifier_mod():
    """A reference not in a classifier MOD's corpus is skipped before any
    embedding work — so we never spend OpenAI on embeddings no classifier uses."""
    pytest.importorskip("agr_abc_document_parsers.embeddings",
                        reason="embeddings extra / shared release not installed")
    with patch.object(eg.config, "OPENAI_API_KEY", "sk-test"), \
            patch.object(eg, "_reference_has_classifier_mod", return_value=False):
        result = eg.generate_classifier_embeddings_for_reference(db=object(), reference_id=1)
    assert result == {"skipped": "no_classifier_mod"}


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
    # Reference must be in a classifier MOD's corpus to pass the SCRUM-6273 filter.
    _add_classifier_mod_corpus(db, ref.reference_id)

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


@pytest.mark.webtest
def test_end_to_end_skips_reference_without_classifier_mod(db, test_reference):  # noqa
    """A reference with merged Markdown but NOT in a classifier MOD's corpus is
    skipped end-to-end (SCRUM-6273): no embeddings are generated or registered."""
    pytest.importorskip("agr_abc_document_parsers.embeddings",
                        reason="embeddings extra / shared release not installed")
    from agr_literature_service.api.models import EmbeddingFileModel

    curie = test_reference.new_ref_curie
    ref = db.query(ReferenceModel).filter(ReferenceModel.curie == curie).one()
    # Intentionally NO classifier-MOD corpus association for this reference.
    main = ReferencefileModel(reference_id=ref.reference_id, display_name="paper_merged",
                              file_class="converted_merged_main", file_publication_status="final",
                              file_extension="md", md5sum="mainmd5nomod", is_annotation=False)
    db.add(main)
    db.commit()

    with patch.object(eg.config, "OPENAI_API_KEY", "sk-test"), \
            patch("agr_literature_service.lit_processing.embedding.openai_embedder.OpenAIEmbedder",
                  _FakeEmbedder), \
            patch.object(eg, "download_file", return_value=MERGED_MD):
        result = eg.generate_classifier_embeddings_for_reference(db, ref.reference_id, curie)
    assert result == {"skipped": "no_classifier_mod"}
    rows = db.query(EmbeddingFileModel).filter_by(reference_id=ref.reference_id).all()
    assert rows == []


def _masked_pyarrow_error():
    """The error agr_abc_document_parsers raises when pyarrow 26 refuses to load
    under numpy<2: a friendly ImportError chained from the real one."""
    try:
        try:
            raise ImportError("pyarrow requires NumPy 2.0 or newer, found 1.26.4")
        except ImportError as exc:
            raise ImportError(
                "reading/writing embedding parquet needs pyarrow; install the optional "
                "extra: pip install 'agr-abc-document-parsers[embeddings]'"
            ) from exc
    except ImportError as masked:
        return masked


def test_describe_includes_chained_cause():
    """The logged text keeps the underlying reason a library re-raised over."""
    text = eg._describe(_masked_pyarrow_error())
    assert text.startswith("reading/writing embedding parquet needs pyarrow")
    assert "caused by ImportError: pyarrow requires NumPy 2.0 or newer, found 1.26.4" in text
    assert "\n" not in text
    assert eg._describe(ValueError("plain")) == "plain"


def test_describe_skips_suppressed_context():
    """`raise ... from None` deliberately hides the context; so do we."""
    try:
        try:
            raise KeyError("internal")
        except KeyError:
            raise RuntimeError("outer") from None
    except RuntimeError as exc:
        assert eg._describe(exc) == "outer"


def _logged(mock_error):
    """The text of each logger.error call, %-formatted like the log line."""
    return [call.args[0] % call.args[1:] for call in mock_error.call_args_list]


def test_maybe_generate_logs_the_cause():
    """The fire-and-forget wrapper logs the cause, not just the masking message."""
    with patch.object(eg, "generate_classifier_embeddings_for_reference",
                      side_effect=_masked_pyarrow_error()), \
            patch.object(eg.logger, "error") as error:
        eg.maybe_generate_classifier_embeddings(db=None, reference_id=1, reference_curie="AGRKB:1")
    (line,) = _logged(error)
    assert "caused by ImportError: pyarrow requires NumPy 2.0 or newer" in line


def test_per_source_failure_logs_the_cause():
    """A per-source failure is counted and its log line carries the chained cause.
    The prod alert of 2026-10-19 showed only the masking message."""
    pytest.importorskip("agr_abc_document_parsers.embeddings",
                        reason="embeddings extra / shared release not installed")
    source = MagicMock(referencefile_id=5203578)
    fake_db = MagicMock()
    fake_db.query.return_value.filter.return_value.all.return_value = [source]
    with patch.object(eg.config, "OPENAI_API_KEY", "sk-test"), \
            patch("agr_literature_service.lit_processing.embedding.openai_embedder.OpenAIEmbedder",
                  _FakeEmbedder), \
            patch.object(eg, "_reference_has_classifier_mod", return_value=True), \
            patch.object(eg.embedding_file_crud, "get_embeddings_for_sources", return_value={}), \
            patch.object(eg, "_embed_and_register", side_effect=_masked_pyarrow_error()), \
            patch.object(eg.logger, "error") as error:
        result = eg.generate_classifier_embeddings_for_reference(fake_db, 1, "AGRKB:101000001310104")
    assert result["failed"] == 1 and result["embedded"] == 0
    (line,) = _logged(error)
    assert line.startswith("Embedding generation failed for referencefile 5203578 (AGRKB:101000001310104)")
    assert "caused by ImportError: pyarrow requires NumPy 2.0 or newer" in line
