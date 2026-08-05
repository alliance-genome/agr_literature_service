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
# Emit the INFO summary of what was marked (root stays at WARNING otherwise).
logger.setLevel(logging.INFO)

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

# MOD abbreviations for which the interaction load marks curation status
# complete. The membership test normalizes the dataset name first, so to enable
# Xenbase add "XB" here (not "XBXL"/"XBXT"). Kept as a set so FB (and others)
# can be turned on once they confirm they want the same behavior; the topic
# mapping above already applies to any MOD.
CURATION_COMPLETE_MODS = {"WB"}

# Commit in batches so a first (large) run does not build one giant transaction.
BATCH_COMMIT_SIZE = 500


def get_top_source(source_counts: Dict[str, int]) -> Optional[str]:
    """Return the source with the most interaction rows for a paper.

    Ties are broken by ASCII order of the source name (so the result is
    deterministic; note uppercase sorts before lowercase). Returns None when
    there are no sources.
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


def _mod_abbreviation(dataset_name: str) -> str:
    """Map an interaction dataset name to its MOD abbreviation.

    Xenbase datasets (XBXL / XBXT) share the ``XB`` abbreviation; every other
    dataset name is already the abbreviation.
    """
    return "XB" if dataset_name.startswith("XB") else dataset_name


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


def _get_existing_status_by_reference(db_session: Session, mod_id: int, topic: str,
                                      reference_ids: List[int]) -> Dict[int, CurationStatusModel]:
    """Return ``{reference_id: CurationStatusModel}`` for the existing rows of this
    mod/topic. Lets the caller fill a blank (NULL) status while never overwriting
    a status a curator has already set."""
    existing: Dict[int, CurationStatusModel] = {}
    for i in range(0, len(reference_ids), 1000):
        chunk = reference_ids[i:i + 1000]
        rows = (
            db_session.query(CurationStatusModel)
            .filter(
                CurationStatusModel.mod_id == mod_id,
                CurationStatusModel.topic == topic,
                CurationStatusModel.reference_id.in_(chunk),
            )
            .all()
        )
        for row in rows:
            existing[row.reference_id] = row
    return existing


def mark_interaction_curation_complete(db_session: Session, datasetName: str,
                                       dataType: str, all_pmids: Set[str],
                                       pmid_to_src_counts: Dict[str, Dict[str, int]],
                                       in_corpus_set: Optional[Set[str]] = None) -> Optional[Dict]:
    """Mark the interaction topic "curation complete" for the MOD's interaction
    papers.

    Only papers already in the MOD's corpus are marked (those are the papers the
    MOD curates and that appear in its workflow editor). Fill-blanks-only: a row
    whose curation_status a curator has already set is never touched, and a row
    that exists with a NULL status has only its status filled (any curator note /
    curation_tag is left intact). Each newly created row is attributed to the
    paper's dominant interaction source, with all sources recorded in the note.

    ``in_corpus_set`` may be passed in to reuse a corpus membership set the caller
    already computed; otherwise it is fetched. A failure is logged and rolled back
    rather than raised, so curation-status marking degrades independently of the
    interaction load. A no-op for MODs not in ``CURATION_COMPLETE_MODS`` or
    unmapped data types.
    """
    abbreviation = _mod_abbreviation(datasetName)
    if abbreviation not in CURATION_COMPLETE_MODS:
        return None

    topic = DATATYPE_TO_TOPIC.get(dataType)
    if topic is None:
        logger.warning(f"No interaction curation topic mapped for data type "
                       f"{dataType}; skipping curation status for {datasetName}.")
        return None

    mod = db_session.query(ModModel).filter_by(abbreviation=abbreviation).one_or_none()
    if mod is None:
        logger.error(f"MOD {abbreviation} not found; skipping interaction curation status.")
        return None
    mod_id = mod.mod_id

    # Only papers in this MOD's corpus: those are the papers it curates.
    if in_corpus_set is None:
        in_corpus_set, _ = get_mod_papers(db_session, abbreviation)
    target_pmids = set(all_pmids) & in_corpus_set

    pmid_to_reference_id = _get_reference_ids_for_pmids(db_session, target_pmids)
    reference_ids = list(set(pmid_to_reference_id.values()))
    existing_by_reference = _get_existing_status_by_reference(
        db_session, mod_id, topic, reference_ids)
    # Snapshot the decision fields as plain values now, before any mid-loop
    # commit expires the ORM instances (expire_on_commit defaults to True) --
    # otherwise reading them later in the loop would fire per-row refresh SELECTs.
    existing_status = {rid: row.curation_status for rid, row in existing_by_reference.items()}
    existing_note = {rid: row.note for rid, row in existing_by_reference.items()}

    added = updated = skipped = 0
    processed: Set[int] = set()
    try:
        pending = 0
        for pmid in target_pmids:
            reference_id = pmid_to_reference_id.get(pmid)
            # Guard the (topic, reference_id, mod_id) unique constraint when two
            # PMIDs resolve to the same reference within this run.
            if reference_id is None or reference_id in processed:
                continue
            processed.add(reference_id)

            source_counts = pmid_to_src_counts.get(pmid, {})
            author = get_top_source(source_counts) or INTERACTION_LOAD_USER
            note = format_all_sources(source_counts)

            existing = existing_by_reference.get(reference_id)
            if existing is not None:
                if existing_status[reference_id] is not None:
                    # A curator (or an earlier run) already set a status: leave it.
                    skipped += 1
                    continue
                # Row exists but the status is blank -- fill the status, and add
                # the source provenance note only if the curator left none (never
                # overwrite a curator-entered note / curation_tag).
                existing.curation_status = CURATION_COMPLETE_STATUS
                existing.updated_by = author
                if existing_note[reference_id] is None:
                    existing.note = note
                updated += 1
            else:
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
                added += 1

            pending += 1
            if pending >= BATCH_COMMIT_SIZE:
                db_session.commit()
                pending = 0
        db_session.commit()
    except Exception as e:
        # Rolls back only the uncommitted batch (session-wide, but nothing else
        # is pending on this session at this point). Already-committed batches
        # stay; the step is fill-blanks-only/idempotent, so the next run picks up
        # whatever remains. The interaction load report is unaffected.
        db_session.rollback()
        logger.error(
            f"{datasetName} {dataType} interaction curation status failed after "
            f"{added} inserted / {updated} filled; the uncommitted batch was rolled "
            f"back and the remainder will be retried on the next run. Error: {e}"
        )
        return None

    logger.info(
        f"{datasetName} {dataType} interaction curation status (topic {topic}): "
        f"{added} marked 'curation complete', {updated} blank status filled, "
        f"{skipped} already had a status."
    )
    return {"topic": topic, "added": added, "updated": updated, "skipped": skipped}
