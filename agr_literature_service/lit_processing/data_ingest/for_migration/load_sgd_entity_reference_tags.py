"""
load_sgd_entity_reference_tags.py
=================================

One-off load of SGD entity-reference associations as entity topic entity tags
(TETs) so the associations display on the reference pages of the public
website (SCRUM-6404).

Data source: references_with_entities.tsv, exported from SGD by
SGDBackend-Nex2 scripts/dumping/reference/dump_references_with_entities.py.
Tab-delimited columns (with a header line):
    1. reference_sgdid  (e.g. S000039113)              -> reference
    2. entity_type      (gene | allele | complex | pathway)
    3. entity_name      (e.g. ACT1, act1-1, CPX-2921, PWY3O-46; unused here)
    4. entity_sgdid     (e.g. S000002284)              -> entity
    5. date_created     (YYYY-MM-DD the reference was added to SGD) -> date_created
    6. created_by       (SGD curator database id)             -> created_by/updated_by
    7. topic            (SGD literature topic: Primary Literature |
                         Additional Literature | Reviews | Omics) -> display_tag
                        (see sgd_display_tag)

Every association becomes a "pure entity" tag (topic == entity_type, one of
gene/allele/complex/pathway) from the shared SGD reference-curation source.
The tag's created_by/updated_by is the SGD curator who added the reference,
resolved to a users.id by first/last name or Stanford email local-part (see
sgd_reference_tag_utils.resolve_sgd_created_by; unresolved ids are stored
verbatim and get an automation users row). Every tag gets a display_tag mapped
from the association's SGD literature topic, so it shows under the right
section (Primary/Additional Lit For, Reviews, ...) on the reference pages.
Each tag's date_created is preserved from SGD (when the reference was added
there) with the load time as date_updated. Rows from older dumps that predate
a column degrade gracefully: no created_by falls back to the script's
automation user, no topic leaves the display_tag to create_tag's topic-ATP
stamping (complex primary, allele/pathway additional, gene none), and no
date_created stamps both dates with the load time.

Only references already in the SGD corpus are tagged; associations whose paper
is not in the SGD corpus (or not in the ABC at all) are skipped and listed in
the report so a curator can follow up.

Idempotent and cheap to re-run: already-loaded associations are read once up
front, corrected in place when their display_tag/date_created disagree with
the dump (see maybe_update_existing_tag -- no delete-and-reload needed after
a mapping fix), and skipped otherwise. Add-only: if SGD drops an association,
the previously created tag persists (same trade-off as the ZFIN loaders).

The incremental counterpart is update_sgd_entity_reference_tags.py, which pulls
recently added references from the SGD API instead of a file dump.
"""
import argparse
import logging
from os import path
from typing import Dict, Iterator, Optional, Set, Tuple

from dotenv import load_dotenv

# sgd_reference_tag_utils (which initializes the api models via the crud
# imports) must be imported before api.user, or user.py's import of user_crud
# hits a circular import through the versioning plugins.
from agr_literature_service.lit_processing.data_ingest.for_migration.sgd_reference_tag_utils import (
    ENTITY_TYPE_TO_ATP,
    MAX_ASSOCIATIONS_PER_PAPER,
    PROGRESS_LOG_INTERVAL,
    SGD_CURIE_PREFIX,
    build_sgd_corpus_ref_curies,
    build_sgd_ref_curie_map,
    create_entity_tags,
    deliver_report,
    format_report_counts,
    sgd_display_tag,
    get_or_create_source,
    load_existing_entity_tags,
    log_run_summary,
    new_counts,
    new_entities_by_paper,
    resolve_sgd_created_by,
    select_over_cap_papers,
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

DEFAULT_INPUT_FILE = "data/references_with_entities.tsv"

MISSING_LOG = "sgd_entity_reference_missing_ref_ids.log"
NOT_IN_CORPUS_LOG = "sgd_entity_reference_not_in_corpus.log"
OVER_CAP_LOG = "sgd_entity_reference_over_cap.log"


def parse_references_with_entities(file_with_path: str) -> Iterator[Tuple[str, str, str, str, str, str]]:
    """Yield (reference_sgdid, entity_type, entity_sgdid, date_created,
    created_by, sgd_topic) for each data row. The header line and any malformed
    row are skipped; entity_type is yielded as-is so the caller can count
    unknown types. date_created, created_by, and sgd_topic are "" for dumps
    that predate their columns."""
    with open(file_with_path) as f:
        for line in f:
            pieces = line.rstrip("\n").split("\t")
            if len(pieces) < 4:
                continue
            reference_sgdid = pieces[0].strip()
            if reference_sgdid == "reference_sgdid":
                continue
            date_created = pieces[4].strip() if len(pieces) > 4 else ""
            created_by = pieces[5].strip() if len(pieces) > 5 else ""
            sgd_topic = pieces[6].strip() if len(pieces) > 6 else ""
            yield reference_sgdid, pieces[1].strip(), pieces[3].strip(), date_created, created_by, sgd_topic


def _sgd_curie(sgdid: str) -> str:
    return sgdid if sgdid.startswith(f"{SGD_CURIE_PREFIX}:") else f"{SGD_CURIE_PREFIX}:{sgdid}"


def count_associations_per_paper(file_with_path: str) -> Dict[Tuple[str, str], Set[str]]:
    """First pass over the file: map (reference SGD curie, entity type) to the
    set of distinct entity sgdids associated with it, to identify papers whose
    association count for a type exceeds MAX_ASSOCIATIONS_PER_PAPER."""
    entities_by_paper = new_entities_by_paper()
    for reference_sgdid, entity_type, entity_sgdid, _date_created, _created_by, _sgd_topic \
            in parse_references_with_entities(file_with_path):
        if entity_type not in ENTITY_TYPE_TO_ATP or not entity_sgdid:
            continue
        entities_by_paper[(_sgd_curie(reference_sgdid), entity_type)].add(entity_sgdid)
    return entities_by_paper


def load_sgd_entity_reference_tags(input_file: str) -> Dict:
    """Load SGD entity-reference associations from the tsv dump as entity TETs.

    Returns:
        A dict of counts describing the run (also used to build the report).
    """
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    counts = new_counts()
    missing_ref_ids: Set[str] = set()
    # resolved reference curie -> SGD reference curie, for references found in
    # the ABC but not in the SGD corpus (skipped, reported for curator follow-up).
    not_in_corpus_refs: Dict[str, str] = {}
    # SGD curator database id -> users.id (resolve_sgd_created_by memoization).
    user_id_cache: Dict[str, str] = {}

    try:
        source_id = get_or_create_source(db)
        sgd_to_ref_curie = build_sgd_ref_curie_map(db)
        logger.info(f"Loaded {len(sgd_to_ref_curie)} SGD reference cross_references")
        sgd_corpus_ref_curies = build_sgd_corpus_ref_curies(db)
        logger.info(f"Loaded {len(sgd_corpus_ref_curies)} references in the SGD corpus")
        existing_tags = load_existing_entity_tags(db, source_id)
        logger.info(f"Loaded {len(existing_tags)} entity tags already present for this source")

        over_cap_papers = select_over_cap_papers(count_associations_per_paper(input_file))
        counts["papers_over_cap"] = len({token for token, _type in over_cap_papers})
        counts["skipped_over_cap"] = sum(over_cap_papers.values())
        logger.info(
            "%d paper/type groups exceed %d associations and will be skipped",
            len(over_cap_papers), MAX_ASSOCIATIONS_PER_PAPER,
        )

        def associations() -> Iterator[Tuple[str, str, str, Optional[str], Optional[str], Optional[str]]]:
            for reference_sgdid, entity_type, entity_sgdid, date_created, sgd_created_by, sgd_topic \
                    in parse_references_with_entities(input_file):
                counts["total_associations"] += 1
                if counts["total_associations"] % PROGRESS_LOG_INTERVAL == 0:
                    logger.info(
                        "Processed %d associations: created=%d skipped_duplicate=%d "
                        "missing_reference=%d not_in_corpus=%d errors=%d",
                        counts["total_associations"], counts["created"],
                        counts["skipped_duplicate"], counts["missing_reference"],
                        counts["not_in_corpus"], counts["errors"],
                    )
                if entity_type not in ENTITY_TYPE_TO_ATP:
                    counts["unknown_entity_type"] += 1
                    continue
                if not entity_sgdid:
                    # would otherwise become a real tag with entity "SGD:"
                    counts["missing_entity_id"] += 1
                    continue
                ref_token = _sgd_curie(reference_sgdid)
                if (ref_token, entity_type) in over_cap_papers:
                    continue
                reference_curie = sgd_to_ref_curie.get(ref_token)
                if reference_curie is None:
                    counts["missing_reference"] += 1
                    missing_ref_ids.add(ref_token)
                    continue
                if reference_curie not in sgd_corpus_ref_curies:
                    counts["not_in_corpus"] += 1
                    not_in_corpus_refs.setdefault(reference_curie, ref_token)
                    continue
                yield (reference_curie, ENTITY_TYPE_TO_ATP[entity_type], _sgd_curie(entity_sgdid),
                       resolve_sgd_created_by(db, sgd_created_by, user_id_cache),
                       sgd_display_tag(sgd_topic),
                       date_created or None)

        create_entity_tags(db, associations(), source_id, existing_tags, counts)

        counts["not_in_corpus_refs"] = not_in_corpus_refs
        log_run_summary(counts, "SGD entity-reference load")
        write_id_log(MISSING_LOG,
                     f"SGD reference ids not found in ABC ({len(missing_ref_ids)})",
                     sorted(missing_ref_ids))
        write_id_log(NOT_IN_CORPUS_LOG,
                     f"References not in the SGD corpus ({len(not_in_corpus_refs)})",
                     [f"{tok}\t{ref}" for ref, tok in sorted(not_in_corpus_refs.items())])
        write_id_log(OVER_CAP_LOG,
                     f"Paper/type groups skipped for exceeding {MAX_ASSOCIATIONS_PER_PAPER} "
                     f"associations ({len(over_cap_papers)})",
                     [f"{tok}\t{entity_type}\t{count}"
                      for (tok, entity_type), count in sorted(over_cap_papers.items())])
        return counts
    finally:
        db.close()


def compose_report_message(counts: Dict, input_file: str) -> str:
    """Compose the HTML email report message from the run counts."""
    message = "<b>SGD Entity-Reference Association Loading Report</b><p>"
    message += f"Input file: {input_file}<p>"
    message += format_report_counts(counts, "file")
    return message


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Load SGD entity-reference associations (gene/allele/complex/pathway) "
                    "from references_with_entities.tsv as entity topic entity tags"
    )
    parser.add_argument(
        "-f", "--input-file",
        default=DEFAULT_INPUT_FILE,
        help=f"Path to references_with_entities.tsv (default: {DEFAULT_INPUT_FILE})",
    )
    parser.add_argument(
        "-n", "--no-email",
        action="store_true",
        help="Do not email the report (for testing)",
    )
    args = parser.parse_args()

    run_counts = load_sgd_entity_reference_tags(input_file=args.input_file)
    report = compose_report_message(run_counts, args.input_file)
    deliver_report("SGD Entity-Reference Association Loading Report", report, args.no_email)
