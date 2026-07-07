"""Generate + register classifier embeddings for a reference's merged Markdown
(SCRUM-6142).

For each **merged** Markdown of a reference — ``converted_merged_main`` and every
``converted_merged_supplement`` (NOT the method-specific grobid/marker/docling
outputs) — chunk it (``paragraph_pack``: paragraph-level, non-overlapping,
references excluded), embed the chunks with OpenAI, write the canonical parquet,
and register it via :func:`embedding_file_crud.create_or_update`. So a reference
yields one embedding file per merged Markdown (main + each supplement).

Idempotent: a source that already has an ``embedding_file`` row for this
``(profile, version)`` is skipped, so re-running the conversion job never
re-spends OpenAI. Per-source isolation: a failure on one source is logged and
counted, never aborting the others.

The embedding stack (``agr_abc_document_parsers.embeddings`` + ``openai``) is
imported lazily; when it or ``OPENAI_API_KEY`` is missing the whole step is a
logged no-op, so the pdf2md conversion job runs unchanged until the feature is
deliberately enabled.
"""

import logging
import os
import tempfile
from io import BytesIO
from typing import Mapping, Optional

from fastapi import UploadFile

from agr_cognito_py import ModAccess

from agr_literature_service.api.config import config
from agr_literature_service.api.crud import embedding_file_crud
from agr_literature_service.api.crud.referencefile_crud import download_file
from agr_literature_service.api.models import (
    ModCorpusAssociationModel,
    ModModel,
    ReferencefileModel,
)
from agr_literature_service.api.schemas.embedding_file_schemas import EmbeddingFileSchemaCreate

logger = logging.getLogger(__name__)

# Only the MERGED Markdown is embedded — not the per-method grobid/marker/docling
# outputs. One embedding_file row per merged source (main + each supplement).
MERGED_SOURCE_FILE_CLASSES = ("converted_merged_main", "converted_merged_supplement")

VERSION = 1

# MODs whose ML topic classifiers consume these classifier embeddings. Only
# references in the corpus of one of these MODs are embedded, so we don't spend
# OpenAI generating classifier embeddings that no classifier will use. This is
# intentionally a hardcoded list for now — extend it as more MODs get
# classifiers, or replace it with an ml_model-table lookup. NOTE: this filter is
# specific to *classifier* embeddings; future embeddings for other tools produce
# separate embedding files and must not reuse it.
ML_CLASSIFIER_MODS = ("WB", "FB")


def _reference_has_classifier_mod(db, reference_id: int) -> bool:
    """True iff the reference is in corpus for at least one MOD that has an ML
    classifier (see :data:`ML_CLASSIFIER_MODS`)."""
    return (
        db.query(ModCorpusAssociationModel)
        .join(ModModel, ModModel.mod_id == ModCorpusAssociationModel.mod_id)
        .filter(
            ModCorpusAssociationModel.reference_id == reference_id,
            ModCorpusAssociationModel.corpus.is_(True),
            ModModel.abbreviation.in_(ML_CLASSIFIER_MODS),
        )
        .first()
        is not None
    )


def generate_classifier_embeddings_for_reference(
    db,
    reference_id: int,
    reference_curie: Optional[str] = None,
    *,
    api_key: Optional[str] = None,
    include_document_vector: bool = True,
) -> Mapping[str, object]:
    """Embed every merged Markdown of ``reference_id`` and register the parquets.

    Returns a small result dict (counts / skip reason). Never raises for the
    normal disabled/unavailable paths; per-source errors are caught and counted.
    """
    api_key = api_key or config.OPENAI_API_KEY
    if not api_key:
        logger.debug("OPENAI_API_KEY not set; skipping embedding generation")
        return {"skipped": "no_api_key"}

    try:
        from agr_abc_document_parsers.embeddings import (
            DEFAULT_PROFILE,
            ParagraphPackChunker,
        )

        from agr_literature_service.lit_processing.embedding.openai_embedder import OpenAIEmbedder
    except ImportError as exc:
        logger.warning(
            "Embedding stack unavailable (%s); skipping. Install "
            "'agr-abc-document-parsers[embeddings]' and 'openai' to enable.", exc
        )
        return {"skipped": "deps_unavailable"}

    # Only embed references belonging to a MOD that actually has an ML classifier
    # — otherwise the embeddings would never be consumed.
    if not _reference_has_classifier_mod(db, reference_id):
        logger.debug(
            "Reference %s is not in a classifier MOD corpus (%s); skipping "
            "classifier embedding generation", reference_id, ML_CLASSIFIER_MODS
        )
        return {"skipped": "no_classifier_mod"}

    sources = (
        db.query(ReferencefileModel)
        .filter(
            ReferencefileModel.reference_id == reference_id,
            ReferencefileModel.file_class.in_(MERGED_SOURCE_FILE_CLASSES),
            ReferencefileModel.file_extension == "md",
            ReferencefileModel.file_publication_status == "final",
        )
        .all()
    )
    if not sources:
        return {"skipped": "no_merged_markdown"}

    profile_name = DEFAULT_PROFILE
    chunker = ParagraphPackChunker(profile_name=profile_name)
    embedder = OpenAIEmbedder(api_key)

    existing_by_source = embedding_file_crud.get_embeddings_for_sources(
        db, [int(s.referencefile_id) for s in sources]
    )

    embedded = skipped_existing = failed = 0
    for source in sources:
        source_id = int(source.referencefile_id)
        existing = existing_by_source.get(source_id, [])
        if any(e.profile_name == profile_name and e.version == VERSION for e in existing):
            skipped_existing += 1
            continue
        curie = reference_curie or (source.reference.curie if source.reference else None)
        if curie is None:
            logger.error("No reference curie for referencefile %s; skipping", source_id)
            failed += 1
            continue
        try:
            _embed_and_register(
                db, curie, source, chunker, embedder, profile_name,
                include_document_vector=include_document_vector,
            )
            embedded += 1
        except Exception as exc:
            logger.error(
                "Embedding generation failed for referencefile %s (%s): %s",
                source_id, curie, exc
            )
            failed += 1
    return {
        "embedded": embedded, "skipped_existing": skipped_existing,
        "failed": failed, "sources": len(sources),
    }


def _embed_and_register(db, reference_curie, source, chunker, embedder, profile_name,
                        *, include_document_vector: bool) -> None:
    """Chunk + embed one merged Markdown and register its parquet."""
    from agr_abc_document_parsers.embeddings import EmbeddingRecipe, write_chunks_parquet
    from agr_abc_document_parsers.embeddings.models import Chunk

    source_id = int(source.referencefile_id)
    content = download_file(
        db=db, referencefile_id=source_id, mod_access=ModAccess.ALL_ACCESS, use_in_api=False
    )
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")
    if not content or not content.strip():
        logger.info("Empty markdown for referencefile %s; skipping", source_id)
        return

    chunks = chunker.chunk(content, reference_curie=reference_curie)
    if not chunks:
        logger.info("No chunks produced for referencefile %s; skipping", source_id)
        return
    embedder.embed_chunks(chunks)

    if include_document_vector:
        doc_text = chunker.document_text(chunks)
        doc_input = embedder.truncate_to_limit(doc_text)
        doc_vector = embedder.embed([doc_input])[0]
        chunks.append(Chunk(
            reference_curie=reference_curie, chunk_index=-1, content=doc_input,
            profile_name=profile_name, chunking_strategy="document",
            section_title="__document__", is_document_level=True,
            n_tokens=embedder.count_tokens(doc_input), embedding=doc_vector,
        ))

    recipe = EmbeddingRecipe(
        profile_name=profile_name, version=VERSION,
        embedding_model=embedder.model, embedding_dim=embedder.dimension,
        chunker_name=chunker.name, chunk_target_tokens=chunker.target_tokens,
        chunk_overlap_tokens=0, source="fulltext", references_excluded=True,
    )

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name
        write_chunks_parquet(tmp_path, chunks, recipe, reference_curie=reference_curie)
        with open(tmp_path, "rb") as handle:
            parquet_bytes = handle.read()
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    request = EmbeddingFileSchemaCreate(
        reference_curie=reference_curie, profile_name=profile_name, version=VERSION,
        model_name=embedder.model_name, source_referencefile_id=source_id,
    )
    upload = UploadFile(
        filename=f"embedding_{profile_name}_v{VERSION}.parquet",
        file=BytesIO(parquet_bytes),
    )
    embedding_file_crud.create_or_update(db, request, upload)
    logger.info(
        "Registered %s embedding for referencefile %s (%s): %d chunks",
        profile_name, source_id, reference_curie, len(chunks)
    )


def maybe_generate_classifier_embeddings(
    db, reference_id: int, reference_curie: Optional[str] = None
) -> None:
    """Fire-and-forget wrapper around
    :func:`generate_classifier_embeddings_for_reference` for conversion callers.

    Fully isolated: any failure is logged and swallowed so classifier embedding
    generation can never flip the outcome of a conversion. The underlying
    function already no-ops (returns a skip reason) when the feature is disabled,
    the embedding stack is missing, or the reference is not in a classifier MOD's
    corpus."""
    try:
        result = generate_classifier_embeddings_for_reference(
            db, reference_id, reference_curie
        )
        logger.debug(
            "Classifier embedding result for reference %s: %s", reference_id, result
        )
    except Exception as exc:
        logger.error(
            "Classifier embedding generation failed for reference %s (%s): %s",
            reference_id, reference_curie, exc
        )
