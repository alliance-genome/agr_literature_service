"""
update_sgd_gene_reference_tags.py
=================================

Incremental updater for the SGD entity-reference associations loaded by
load_sgd_entity_reference_tags.py (SCRUM-6404). Instead of a file dump, it
calls the SGD backend API for references recently added to SGD:

    https://backend.yeastgenome.org/references_with_entities/days_added={days}

which returns only references that have associated entities, each as
    {"sgdid": "S100004374", "date_created": "2026-07-08",
     "entities": [{"entity_type": "gene", "entity_name": "CDC48",
                   "entity_sgdid": "S000002284"}, ...]}
with entity_type one of gene/allele/complex/pathway.

Every association becomes a "pure entity" tag (topic == entity_type) from the
same shared SGD reference-curation source as the one-off load, gated on SGD
corpus membership. Idempotent and add-only, so it is safe to run on a cron
(default window of 7 days overlaps comfortably with a weekly schedule;
already-loaded associations are skipped up front).
"""
import argparse
import logging
from os import environ, path
from typing import Dict, Iterator, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv

# sgd_reference_tag_utils (which initializes the api models via the crud
# imports) must be imported before api.user, or user.py's import of user_crud
# hits a circular import through the versioning plugins.
from agr_literature_service.lit_processing.data_ingest.for_migration.sgd_reference_tag_utils import (
    ENTITY_TYPE_TO_ATP,
    MAX_ASSOCIATIONS_PER_PAPER,
    SGD_CURIE_PREFIX,
    build_sgd_corpus_ref_curies,
    build_sgd_ref_curie_map,
    create_entity_tags,
    deliver_report,
    format_report_counts,
    get_or_create_source,
    load_existing_entity_tags,
    log_run_summary,
    new_counts,
    write_id_log,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

SGD_BACKEND_URL = environ.get("SGD_BACKEND_URL", "https://backend.yeastgenome.org")

DEFAULT_DAYS_ADDED = 7
REQUEST_TIMEOUT = 300

MISSING_LOG = "sgd_entity_reference_update_missing_ref_ids.log"
NOT_IN_CORPUS_LOG = "sgd_entity_reference_update_not_in_corpus.log"


def fetch_references_with_entities(days_added: int) -> Optional[List[Dict]]:
    """Fetch the recently added references (with their entities) from the SGD
    backend API. Returns None if the request fails."""
    url = f"{SGD_BACKEND_URL}/references_with_entities/days_added={days_added}"
    logger.info(f"Fetching {url}")
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        references = response.json().get("references", [])
        logger.info(f"SGD returned {len(references)} references with entities")
        return references
    except (requests.RequestException, ValueError) as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def _sgd_curie(sgdid: str) -> str:
    return sgdid if sgdid.startswith(f"{SGD_CURIE_PREFIX}:") else f"{SGD_CURIE_PREFIX}:{sgdid}"


def update_sgd_gene_reference_tags(days_added: int) -> Dict:
    """Create entity TETs for SGD references added in the last ``days_added`` days.

    Returns:
        A dict of counts describing the run (also used to build the report).
    """
    counts = new_counts()

    references = fetch_references_with_entities(days_added)
    if references is None:
        counts["fetch_failed"] = True
        return counts

    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    missing_ref_ids: Set[str] = set()
    not_in_corpus_refs: Dict[str, str] = {}

    try:
        source_id = get_or_create_source(db)
        sgd_to_ref_curie = build_sgd_ref_curie_map(db)
        logger.info(f"Loaded {len(sgd_to_ref_curie)} SGD reference cross_references")
        sgd_corpus_ref_curies = build_sgd_corpus_ref_curies(db)
        logger.info(f"Loaded {len(sgd_corpus_ref_curies)} references in the SGD corpus")
        existing_tags = load_existing_entity_tags(db, source_id)
        logger.info(f"Loaded {len(existing_tags)} entity tags already present for this source")

        def associations() -> Iterator[Tuple[str, str, str]]:
            for reference in references:
                ref_token = _sgd_curie(reference.get("sgdid") or "")
                entities = reference.get("entities") or []
                counts["total_associations"] += len(entities)

                reference_curie = sgd_to_ref_curie.get(ref_token)
                if reference_curie is None:
                    counts["missing_reference"] += len(entities)
                    missing_ref_ids.add(ref_token)
                    continue
                if reference_curie not in sgd_corpus_ref_curies:
                    counts["not_in_corpus"] += len(entities)
                    not_in_corpus_refs.setdefault(reference_curie, ref_token)
                    continue

                entities_by_type: Dict[str, Set[str]] = {}
                for entity in entities:
                    entity_type = entity.get("entity_type") or ""
                    if entity_type not in ENTITY_TYPE_TO_ATP:
                        counts["unknown_entity_type"] += 1
                        continue
                    entities_by_type.setdefault(entity_type, set()).add(
                        _sgd_curie(entity.get("entity_sgdid") or ""))

                for entity_type, entity_curies in entities_by_type.items():
                    if len(entity_curies) > MAX_ASSOCIATIONS_PER_PAPER:
                        counts["skipped_over_cap"] += len(entity_curies)
                        counts["papers_over_cap"] += 1
                        logger.info(
                            "Skipping %d %s associations for %s (over the %d cap)",
                            len(entity_curies), entity_type, ref_token,
                            MAX_ASSOCIATIONS_PER_PAPER,
                        )
                        continue
                    for entity_curie in sorted(entity_curies):
                        yield reference_curie, ENTITY_TYPE_TO_ATP[entity_type], entity_curie

        create_entity_tags(db, associations(), source_id, existing_tags, counts)

        counts["not_in_corpus_refs"] = not_in_corpus_refs
        log_run_summary(counts, f"SGD entity-reference update (days_added={days_added})")
        write_id_log(MISSING_LOG,
                     f"SGD reference ids not found in ABC ({len(missing_ref_ids)})",
                     sorted(missing_ref_ids))
        write_id_log(NOT_IN_CORPUS_LOG,
                     f"References not in the SGD corpus ({len(not_in_corpus_refs)})",
                     [f"{tok}\t{ref}" for ref, tok in sorted(not_in_corpus_refs.items())])
        return counts
    finally:
        db.close()


def compose_report_message(counts: Dict, days_added: int) -> str:
    """Compose the HTML email report message from the run counts."""
    message = "<b>SGD Entity-Reference Association Update Report</b><p>"
    message += f"References added to SGD in the last {days_added} days<p>"
    if counts.get("fetch_failed"):
        message += "<ul><li>Failed to fetch references_with_entities from the SGD API</ul>"
        return message
    message += format_report_counts(counts, "the SGD API response")
    return message


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Create entity topic entity tags for references recently added "
                    "to SGD, from the SGD references_with_entities API"
    )
    parser.add_argument(
        "-d", "--days-added",
        type=int,
        default=DEFAULT_DAYS_ADDED,
        help=f"Tag references added to SGD in the last N days (default: {DEFAULT_DAYS_ADDED})",
    )
    parser.add_argument(
        "-n", "--no-email",
        action="store_true",
        help="Do not email the report (for testing)",
    )
    args = parser.parse_args()

    run_counts = update_sgd_gene_reference_tags(days_added=args.days_added)
    report = compose_report_message(run_counts, args.days_added)
    deliver_report("SGD Entity-Reference Association Update Report", report, args.no_email)
