import logging
import os
import time
from os import path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import requests
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag
from agr_literature_service.api.models import (
    ModModel,
    ReferenceModel,
    TopicEntityTagSourceModel,
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

PROTEIN_STRUCTURE_ATP = "ATP:0000091"
DATA_NOVELTY_NOT_NEW = "ATP:0000335"
ECO_AUTOMATIC_ASSERTION = "ECO_0006156"
SOURCE_METHOD = "PDB association pipeline"
SOURCE_DATA_PROVIDER = "PDB"
SECONDARY_DATA_PROVIDER_ABBR = "AGR"
SOURCE_DESCRIPTION = (
    "High throughput data from the PDB database associated with references "
    "via load_pdb_associations.py"
)

RCSB_SEARCH_URL = "https://search.rcsb.org/rcsbsearch/v2/query"
RCSB_GRAPHQL_URL = "https://data.rcsb.org/graphql"
SEARCH_PAGE_SIZE = 10000
GRAPHQL_BATCH_SIZE = 500
PAGE_PAUSE_SECONDS = 0.5
HTTP_MAX_ATTEMPTS = 4
HTTP_BACKOFF_BASE_SECONDS = 5
HTTP_TIMEOUT_SECONDS = 60

# Entity-mode flag (see SCRUM-3982). Default is entity mode; flip via env
# `PDB_TET_INCLUDE_ENTITY=false` to fall back to topic-only TETs while the
# exact entity field semantics are confirmed with curators.
INCLUDE_ENTITY_FIELDS = os.environ.get("PDB_TET_INCLUDE_ENTITY", "true").lower() == "true"

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
        rcsb_pubmed_container_identifiers { pubmed_id }
      }
    }
    """
    body = {"query": query, "variables": {"ids": pdb_ids}}
    payload = _post_with_retry(RCSB_GRAPHQL_URL, body)
    entries = (payload.get("data") or {}).get("entries") or []
    out: Dict[str, Optional[str]] = {}
    for entry in entries:
        rcsb_id = entry.get("rcsb_id")
        pmid_container = entry.get("rcsb_pubmed_container_identifiers") or {}
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


def _build_payload(reference_curie: str, pdb_id: str, source_id: int) -> TopicEntityTagSchemaPost:
    data: Dict = {
        "reference_curie": reference_curie,
        "topic": PROTEIN_STRUCTURE_ATP,
        "topic_entity_tag_source_id": source_id,
        "data_novelty": DATA_NOVELTY_NOT_NEW,
        "negated": False,
    }
    if INCLUDE_ENTITY_FIELDS:
        data["entity_type"] = PROTEIN_STRUCTURE_ATP
        data["entity"] = pdb_id
        data["entity_id_validation"] = "alliance"
    return TopicEntityTagSchemaPost(**data)


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
    counts = {"created": 0, "skipped_duplicate": 0, "missing_reference": 0, "errors": 0}
    # Many PDB entries cite the same paper; cache PMID -> (reference_id, curie)
    # to avoid redundant DB lookups across the ~220K-pair stream.
    pmid_cache: Dict[str, Optional[Tuple[int, str]]] = {}
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
            _, reference_curie = cached
            try:
                result = create_tag(db, _build_payload(reference_curie, pdb_id, source_id))
            except Exception as e:
                counts["errors"] += 1
                logger.warning("create_tag failed for PMID:%s / %s: %s", pmid, pdb_id, e)
                continue
            if result.get("status") == "success":
                counts["created"] += 1
            else:
                counts["skipped_duplicate"] += 1
        logger.info(
            "PDB pipeline done: created=%d skipped_duplicate=%d missing_reference=%d errors=%d",
            counts["created"], counts["skipped_duplicate"],
            counts["missing_reference"], counts["errors"],
        )
        return counts
    finally:
        if own_session:
            db.close()


if __name__ == "__main__":
    load()
