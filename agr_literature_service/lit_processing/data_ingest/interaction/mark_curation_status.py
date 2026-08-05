"""
mark_curation_status.py
=======================

When interaction papers are loaded from the FMS interaction files, the papers
are, by that fact, curated for the corresponding interaction topic. This module
records that assertion in the ``curation_status`` table so a curator does not
have to set the status by hand: the "Curation Status" column of the workflow
editor's Curation table is populated straight from these rows.

Mapping of interaction file type to the topic it completes:
    GEN -> genetic interaction  (ATP:0000068)
    MOL -> physical interaction (ATP:0000069)

The status written is "curation complete" (ATP:0000239) -- the same value the
curation UI's "mark curated" action writes.

Attribution: each curation_status row is attributed (created_by / updated_by)
to the interaction source that contributed the most rows for the paper (e.g.
"biogrid", "IntAct"), so the workflow editor's "Curator" column shows the
source. All sources for the paper (with their row counts) are recorded in the
``note`` field. Papers with no source in the file fall back to the
``load_interactions`` user.
"""
import logging
from typing import Dict, List, Optional, Set

from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    CrossReferenceModel,
    CurationStatusModel,
    ModModel,
)
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_papers

logger = logging.getLogger(__name__)

# "curation complete" -- the status the curation UI writes when a topic is
# marked curated (see updateCurationStatusToCurated in the UI's BiblioWorkflow).
CURATION_COMPLETE_STATUS = "ATP:0000239"

# Fallback created_by / updated_by when the interaction file carries no source
# for a paper. Setting it here auto-creates the users row (AuditedModel
# before_insert -> ensure_user_exists_on_connection), as do the per-source names.
INTERACTION_LOAD_USER = "load_interactions"

# Which interaction file type completes which curation topic.
DATATYPE_TO_TOPIC = {
    "GEN": "ATP:0000068",  # genetic interaction
    "MOL": "ATP:0000069",  # physical interaction
}

# MODs for which the interaction load marks curation status complete. Kept as a
# set so FB (and others) can be turned on once they confirm they want the same
# behavior; the topic mapping above already applies to any MOD.
CURATION_COMPLETE_MODS = {"WB"}


def get_top_source(source_counts: Dict[str, int]) -> Optional[str]:
    """Return the source with the most interaction rows for a paper.

    Ties are broken alphabetically so the result is deterministic. Returns None
    when there are no sources.
    """
    if not source_counts:
        return None
    return max(sorted(source_counts), key=lambda src: source_counts[src])


def format_all_sources(source_counts: Dict[str, int]) -> Optional[str]:
    """Format all sources (with row counts) for the curation_status note, e.g.
    ``"BioGRID (5), IntAct (2)"``. Ordered by row count descending, then name.
    Returns None when there are no sources."""
    if not source_counts:
        return None
    ordered = sorted(source_counts, key=lambda src: (-source_counts[src], src))
    return ", ".join(f"{src} ({source_counts[src]})" for src in ordered)


def _get_reference_ids_for_pmids(db_session: Session, pmids: Set[str]) -> Dict[str, int]:
    """Bulk-resolve PMIDs (without the ``PMID:`` prefix) to reference_ids."""
    pmid_to_reference_id: Dict[str, int] = {}
    pmid_list = list(pmids)
    for i in range(0, len(pmid_list), 1000):
        chunk = pmid_list[i:i + 1000]
        curies = ["PMID:" + pmid for pmid in chunk]
        rows = (
            db_session.query(CrossReferenceModel.curie, CrossReferenceModel.reference_id)
            .filter(
                CrossReferenceModel.curie.in_(curies),
                CrossReferenceModel.is_obsolete.is_(False),
            )
            .all()
        )
        for curie, reference_id in rows:
            pmid_to_reference_id[curie.replace("PMID:", "")] = reference_id
    return pmid_to_reference_id


def _get_reference_ids_with_status(db_session: Session, mod_id: int, topic: str,
                                   reference_ids: List[int]) -> Set[int]:
    """Return reference_ids that already have a curation_status row for this
    mod/topic. Used to fill blanks only -- never overwrite a curator's value."""
    existing: Set[int] = set()
    for i in range(0, len(reference_ids), 1000):
        chunk = reference_ids[i:i + 1000]
        rows = (
            db_session.query(CurationStatusModel.reference_id)
            .filter(
                CurationStatusModel.mod_id == mod_id,
                CurationStatusModel.topic == topic,
                CurationStatusModel.reference_id.in_(chunk),
            )
            .all()
        )
        existing.update(row[0] for row in rows)
    return existing


def mark_interaction_curation_complete(db_session: Session, datasetName: str,
                                       dataType: str, all_pmids: Set[str],
                                       pmid_to_src_counts: Dict[str, Dict[str, int]]) -> Optional[Dict]:
    """Mark the interaction topic "curation complete" for the MOD's interaction
    papers.

    Only papers already in the MOD's corpus are marked (those are the papers the
    MOD curates and that appear in its workflow editor). Existing curation_status
    rows are left untouched (fill-blanks-only), so a curator's manual value is
    never overwritten. Each new row is attributed to the paper's dominant
    interaction source, with all sources recorded in the note.

    A no-op for MODs not in ``CURATION_COMPLETE_MODS`` or unmapped data types.
    """
    if datasetName not in CURATION_COMPLETE_MODS:
        return None

    topic = DATATYPE_TO_TOPIC.get(dataType)
    if topic is None:
        logger.warning(f"No interaction curation topic mapped for data type "
                       f"{dataType}; skipping curation status for {datasetName}.")
        return None

    mod = db_session.query(ModModel).filter_by(abbreviation=datasetName).one_or_none()
    if mod is None:
        logger.error(f"MOD {datasetName} not found; skipping interaction curation status.")
        return None
    mod_id = mod.mod_id

    # Only papers in this MOD's corpus: those are the papers it curates.
    in_corpus_set, _ = get_mod_papers(db_session, datasetName)
    target_pmids = set(all_pmids) & in_corpus_set

    pmid_to_reference_id = _get_reference_ids_for_pmids(db_session, target_pmids)
    reference_ids = list(set(pmid_to_reference_id.values()))
    already_set = _get_reference_ids_with_status(db_session, mod_id, topic, reference_ids)

    added = 0
    inserted_reference_ids: Set[int] = set()
    for pmid in target_pmids:
        reference_id = pmid_to_reference_id.get(pmid)
        # Skip: no reference, already has a status, or already inserted this run
        # (guards the (topic, reference_id, mod_id) unique constraint when two
        # PMIDs resolve to the same reference).
        if reference_id is None or reference_id in already_set \
                or reference_id in inserted_reference_ids:
            continue
        source_counts = pmid_to_src_counts.get(pmid, {})
        author = get_top_source(source_counts) or INTERACTION_LOAD_USER
        note = format_all_sources(source_counts)
        db_session.add(
            CurationStatusModel(
                reference_id=reference_id,
                mod_id=mod_id,
                topic=topic,
                curation_status=CURATION_COMPLETE_STATUS,
                note=note,
                created_by=author,
                updated_by=author,
            )
        )
        inserted_reference_ids.add(reference_id)
        added += 1

    db_session.commit()
    skipped = len(reference_ids) - added
    logger.info(
        f"{datasetName} {dataType} interaction curation status (topic {topic}): "
        f"{added} paper(s) marked 'curation complete', "
        f"{skipped} already had a status."
    )
    return {"topic": topic, "added": added, "skipped": skipped}
