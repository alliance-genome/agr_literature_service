import logging
import os
import time
from os import path
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple

import requests
from fastapi import HTTPException
from sqlalchemy import delete
from sqlalchemy.orm import Session

from agr_literature_service.api.crud import cross_reference_crud
from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag
from agr_literature_service.api.models import (
    CrossReferenceModel,
    ModModel,
    ReferenceModel,
    TopicEntityTagModel,
    TopicEntityTagSourceModel,
)
from agr_literature_service.api.schemas.cross_reference_schemas import (
    CrossReferenceSchemaPost,
)
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (
    TopicEntityTagSchemaPost,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.db_read_utils import (
    get_reference_id_by_pmid,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

CURIE_PREFIX = "PDB"
PAGE_REFERENCE = "reference"

# Topic-only TET: flags each reference that has any PDB cross_reference as
# being about protein structure, so downstream curation workflows still see
# these papers via the TET system.
PROTEIN_STRUCTURE_ATP = "ATP:0000091"
DATA_NOVELTY_NOT_NEW = "ATP:0000335"
ECO_AUTOMATIC_ASSERTION = "ECO_0006156"
SOURCE_METHOD = "PDB association pipeline"
SOURCE_DATA_PROVIDER = "PDB"
SECONDARY_DATA_PROVIDER_ABBR = "AGR"
SOURCE_DESCRIPTION = (
    "Protein-structure topic tag asserted for references with PDB cross_references "
    "loaded by load_pdb_associations.py. PDB IDs themselves live in cross_reference."
)

RCSB_SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"
RCSB_GRAPHQL_URL = "https://data.rcsb.org/graphql"
SEARCH_PAGE_SIZE = 10000
GRAPHQL_BATCH_SIZE = 500
PAGE_PAUSE_SECONDS = 0.5
HTTP_MAX_ATTEMPTS = 4
HTTP_BACKOFF_BASE_SECONDS = 5
HTTP_TIMEOUT_SECONDS = 60

# Cleanup of stale PDB cross_references and topic-only TETs (PDB references no
# longer represented in RCSB) is gated behind a sanity threshold: if RCSB
# returns fewer than this many pairs we treat the fetch as suspect and skip
# the cleanup so a bad run can't wipe out the table.
PDB_CLEANUP_MIN_PAIRS = int(os.environ.get("PDB_XREF_CLEANUP_MIN_PAIRS", "1000"))

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _post_with_retry(url: str, body: dict) -> dict:
    """POST with exponential backoff on 429, 5xx, and connection/timeout errors."""
    for attempt in range(HTTP_MAX_ATTEMPTS):
        try:
            response = requests.post(url, json=body, timeout=HTTP_TIMEOUT_SECONDS)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == HTTP_MAX_ATTEMPTS - 1:
                raise
            logger.warning(
                "Network error contacting %s (attempt %d/%d): %s",
                url, attempt + 1, HTTP_MAX_ATTEMPTS, e,
            )
            time.sleep(HTTP_BACKOFF_BASE_SECONDS * (2 ** attempt))
            continue
        if response.status_code == 429 or response.status_code >= 500:
            if attempt == HTTP_MAX_ATTEMPTS - 1:
                response.raise_for_status()
            logger.warning(
                "HTTP %d from %s (attempt %d/%d); backing off",
                response.status_code, url, attempt + 1, HTTP_MAX_ATTEMPTS,
            )
            time.sleep(HTTP_BACKOFF_BASE_SECONDS * (2 ** attempt))
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError("_post_with_retry exhausted attempts")  # pragma: no cover


def _fetch_all_pdb_ids_with_pubmed() -> Iterator[str]:
    """Page through RCSB Search returning PDB entry IDs that have any pubmed_id."""
    start = 0
    while True:
        body = {
            "query": {
                "type": "terminal",
                "service": "text",
                "parameters": {
                    "attribute": "rcsb_pubmed_container_identifiers.pubmed_id",
                    "operator": "exists",
                },
            },
            "return_type": "entry",
            "request_options": {
                "paginate": {"start": start, "rows": SEARCH_PAGE_SIZE},
                "results_content_type": ["experimental"],
            },
        }
        payload = _post_with_retry(RCSB_SEARCH_URL, body)
        results = payload.get("result_set", [])
        if not results:
            return
        for entry in results:
            yield entry["identifier"]
        if len(results) < SEARCH_PAGE_SIZE:
            return
        start += SEARCH_PAGE_SIZE
        time.sleep(PAGE_PAUSE_SECONDS)


def _fetch_pubmed_ids_graphql(pdb_ids: List[str]) -> Dict[str, Optional[str]]:
    """Resolve a batch of PDB IDs to their PubMed IDs via the RCSB Data GraphQL API."""
    query = """
    query($ids: [String!]!) {
      entries(entry_ids: $ids) {
        rcsb_id
        pubmed { rcsb_pubmed_container_identifiers { pubmed_id } }
      }
    }
    """
    body = {"query": query, "variables": {"ids": pdb_ids}}
    payload = _post_with_retry(RCSB_GRAPHQL_URL, body)
    errors = payload.get("errors")
    if errors:
        msg = (errors[0] or {}).get("message", "<no message>") if errors else "<no message>"
        logger.error("RCSB GraphQL returned errors: %s", msg)
        raise RuntimeError(f"RCSB GraphQL error: {msg}")
    entries = (payload.get("data") or {}).get("entries") or []
    out: Dict[str, Optional[str]] = {}
    for entry in entries:
        rcsb_id = entry.get("rcsb_id")
        pubmed = entry.get("pubmed") or {}
        pmid_container = pubmed.get("rcsb_pubmed_container_identifiers") or {}
        pmid = pmid_container.get("pubmed_id")
        if rcsb_id and pmid is not None:
            out[rcsb_id] = str(pmid)
    return out


def _chunks(seq: List[str], size: int) -> Iterator[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def fetch_pdb_pubmed_pairs() -> Iterable[Tuple[str, str]]:
    """Yield (pdb_id, pmid) for every RCSB entry with a PubMed ID."""
    pdb_ids = list(_fetch_all_pdb_ids_with_pubmed())
    logger.info("RCSB returned %d PDB entries with PubMed IDs", len(pdb_ids))
    for batch in _chunks(pdb_ids, GRAPHQL_BATCH_SIZE):
        pdb_to_pmid = _fetch_pubmed_ids_graphql(batch)
        for pdb_id, pmid in pdb_to_pmid.items():
            if pmid:
                yield (pdb_id, pmid)
        time.sleep(PAGE_PAUSE_SECONDS)


def get_or_create_source(db: Session) -> int:
    """Return the topic_entity_tag_source.id for the PDB pipeline, creating it if absent."""
    mod = db.query(ModModel).filter_by(abbreviation=SECONDARY_DATA_PROVIDER_ABBR).one()
    existing = db.query(TopicEntityTagSourceModel).filter_by(
        source_evidence_assertion=ECO_AUTOMATIC_ASSERTION,
        source_method=SOURCE_METHOD,
        data_provider=SOURCE_DATA_PROVIDER,
        secondary_data_provider_id=mod.mod_id,
    ).one_or_none()
    if existing:
        return existing.topic_entity_tag_source_id
    source = TopicEntityTagSourceModel(
        source_evidence_assertion=ECO_AUTOMATIC_ASSERTION,
        source_method=SOURCE_METHOD,
        data_provider=SOURCE_DATA_PROVIDER,
        secondary_data_provider_id=mod.mod_id,
        validation_type=None,
        description=SOURCE_DESCRIPTION,
    )
    db.add(source)
    db.commit()
    db.refresh(source)
    logger.info("Created PDB pipeline TET source id=%d", source.topic_entity_tag_source_id)
    return source.topic_entity_tag_source_id


def _delete_stale_xrefs(db: Session, current_curies: Set[str]) -> int:
    """Delete non-obsolete PDB cross_references whose curie is no longer present
    in the current RCSB view. Returns the deletion count."""
    existing = db.query(
        CrossReferenceModel.cross_reference_id,
        CrossReferenceModel.curie,
    ).filter(
        CrossReferenceModel.curie_prefix == CURIE_PREFIX,
        CrossReferenceModel.is_obsolete.is_(False),
    ).all()
    stale_ids = [
        row.cross_reference_id
        for row in existing
        if row.curie not in current_curies
    ]
    if not stale_ids:
        return 0
    delete_stmt = delete(CrossReferenceModel).where(
        CrossReferenceModel.cross_reference_id.in_(stale_ids)
    )
    result = db.execute(delete_stmt)
    db.commit()
    return result.rowcount or 0  # type: ignore[attr-defined]


def _delete_stale_topic_tets(
    db: Session,
    source_id: int,
    current_reference_ids: Set[int],
) -> int:
    """Delete topic-only TETs from this source whose reference_id is no longer
    in the current RCSB view. Returns the deletion count."""
    existing = db.query(
        TopicEntityTagModel.topic_entity_tag_id,
        TopicEntityTagModel.reference_id,
    ).filter(
        TopicEntityTagModel.topic_entity_tag_source_id == source_id,
    ).all()
    stale_ids = [
        row.topic_entity_tag_id
        for row in existing
        if row.reference_id not in current_reference_ids
    ]
    if not stale_ids:
        return 0
    delete_stmt = delete(TopicEntityTagModel).where(
        TopicEntityTagModel.topic_entity_tag_id.in_(stale_ids)
    )
    result = db.execute(delete_stmt)
    db.commit()
    return result.rowcount or 0  # type: ignore[attr-defined]


def _build_xref_payload(reference_curie: str, pdb_id: str) -> CrossReferenceSchemaPost:
    return CrossReferenceSchemaPost(
        curie=f"{CURIE_PREFIX}:{pdb_id.upper()}",
        reference_curie=reference_curie,
        pages=[PAGE_REFERENCE],
    )


def _build_topic_tet_payload(reference_curie: str, source_id: int) -> TopicEntityTagSchemaPost:
    return TopicEntityTagSchemaPost(
        reference_curie=reference_curie,
        topic=PROTEIN_STRUCTURE_ATP,
        topic_entity_tag_source_id=source_id,
        data_novelty=DATA_NOVELTY_NOT_NEW,
        negated=False,
    )


def _sync_topic_tets(
    db: Session,
    source_id: int,
    reference_curies: Dict[int, str],
    counts: Dict[str, int],
) -> None:
    """For each reference that has any PDB cross_reference this run, ensure a
    topic-only protein-structure TET exists. ``create_tag`` is idempotent:
    it raises HTTPException(409) for true duplicates, and returns
    ``(tag_id, was_upsert=True)`` when an existing tag absorbed the request."""
    for reference_id, reference_curie in reference_curies.items():
        try:
            _new_id, was_upsert = create_tag(db, _build_topic_tet_payload(reference_curie, source_id))
        except HTTPException as e:
            if e.status_code == 409:
                counts["topic_tet_skipped_duplicate"] += 1
                continue
            counts["errors"] += 1
            logger.warning(
                "topic-only TET create failed for reference_id=%s: %s",
                reference_id, e,
            )
            continue
        except Exception as e:
            counts["errors"] += 1
            logger.warning(
                "topic-only TET create failed for reference_id=%s: %s",
                reference_id, e,
            )
            continue
        if was_upsert:
            counts["topic_tet_skipped_duplicate"] += 1
        else:
            counts["topic_tet_created"] += 1


def load(
    db: Optional[Session] = None,
    pairs: Optional[Iterable[Tuple[str, str]]] = None,
) -> Dict[str, int]:
    own_session = db is None
    if own_session:
        db = create_postgres_session(False)
    assert db is not None
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)
    counts = {
        "created": 0,
        "skipped_duplicate": 0,
        "missing_reference": 0,
        "errors": 0,
        "deleted_stale": 0,
        "topic_tet_created": 0,
        "topic_tet_skipped_duplicate": 0,
        "topic_tet_deleted_stale": 0,
    }
    # Many PDB entries cite the same paper; cache PMID -> (reference_id, curie)
    # to avoid redundant DB lookups across the ~220K-pair stream.
    pmid_cache: Dict[str, Optional[Tuple[int, str]]] = {}
    # Tracks every PDB curie we'd assert this run. After the loop we diff
    # against the DB and delete PDB cross_references no longer in this set
    # (i.e. PDB entries removed from RCSB since the last run).
    current_curies: Set[str] = set()
    # reference_id -> reference_curie for every reference we've touched this
    # run; used to (a) sync topic-only TETs, (b) drive stale TET cleanup.
    current_reference_curies: Dict[int, str] = {}
    try:
        source_id = get_or_create_source(db)
        pair_iter = pairs if pairs is not None else fetch_pdb_pubmed_pairs()
        for pdb_id, pmid in pair_iter:
            if pmid not in pmid_cache:
                reference_id = get_reference_id_by_pmid(db, pmid)
                if reference_id is None:
                    pmid_cache[pmid] = None
                else:
                    reference = db.query(ReferenceModel).filter_by(reference_id=reference_id).one()
                    pmid_cache[pmid] = (reference_id, reference.curie)
            cached = pmid_cache[pmid]
            if cached is None:
                counts["missing_reference"] += 1
                continue
            reference_id, reference_curie = cached
            curie = f"{CURIE_PREFIX}:{pdb_id.upper()}"
            current_curies.add(curie)
            current_reference_curies[reference_id] = reference_curie
            try:
                cross_reference_crud.create(db, _build_xref_payload(reference_curie, pdb_id))
                counts["created"] += 1
            except HTTPException as e:
                if e.status_code == 409:
                    counts["skipped_duplicate"] += 1
                else:
                    counts["errors"] += 1
                    logger.warning(
                        "cross_reference create failed for PMID:%s / %s: %s",
                        pmid, pdb_id, e.detail,
                    )
            except Exception as e:
                counts["errors"] += 1
                logger.warning(
                    "cross_reference create failed for PMID:%s / %s: %s",
                    pmid, pdb_id, e,
                )

        _sync_topic_tets(db, source_id, current_reference_curies, counts)

        total_pairs_seen = (
            counts["created"] + counts["skipped_duplicate"]
            + counts["missing_reference"] + counts["errors"]
        )
        if total_pairs_seen >= PDB_CLEANUP_MIN_PAIRS:
            counts["deleted_stale"] = _delete_stale_xrefs(db, current_curies)
            counts["topic_tet_deleted_stale"] = _delete_stale_topic_tets(
                db, source_id, set(current_reference_curies.keys()),
            )
        else:
            logger.warning(
                "Skipping stale cleanup: only %d pairs seen from RCSB "
                "(below safety threshold %d). Set PDB_XREF_CLEANUP_MIN_PAIRS to override.",
                total_pairs_seen, PDB_CLEANUP_MIN_PAIRS,
            )

        logger.info(
            "PDB pipeline done: created=%d skipped_duplicate=%d missing_reference=%d "
            "errors=%d deleted_stale=%d topic_tet_created=%d "
            "topic_tet_skipped_duplicate=%d topic_tet_deleted_stale=%d",
            counts["created"], counts["skipped_duplicate"],
            counts["missing_reference"], counts["errors"], counts["deleted_stale"],
            counts["topic_tet_created"], counts["topic_tet_skipped_duplicate"],
            counts["topic_tet_deleted_stale"],
        )
        return counts
    finally:
        if own_session:
            db.close()


if __name__ == "__main__":
    load()
