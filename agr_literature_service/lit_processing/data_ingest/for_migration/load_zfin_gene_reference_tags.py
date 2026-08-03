"""
load_zfin_gene_reference_tags.py
================================

Load ZFIN gene-reference associations as entity topic entity tags (TETs) so the
associations display on the reference pages of the public website (SCRUM-6362).

Data source: https://zfin.org/downloads/gene_publication.txt
Tab-delimited columns:
    1. Gene Symbol
    2. Gene ID          (e.g. ZDB-GENE-990415-72)   -> entity
    3. Publication ID   (e.g. ZDB-PUB-070425-4)      -> reference
    4. Publication Type
    5. PubMed ID                                     -> reference (fallback)

Per the ticket we only use columns 2 and 3 (falling back to the PubMed ID in
column 5 when a ZFIN publication ID is not found in the ABC). Every association
becomes a "pure entity" gene tag (topic == entity_type == gene) from the shared
ZFIN reference-curation source. This is intentionally a simple load: created_by
and updated_by are the script's automation user and the dates are the load date;
if ZFIN's full data is ever loaded into the Alliance these tags would be dropped
and reloaded with real history.

Only references already in the ZFIN corpus are tagged; associations whose paper is
not in the ZFIN corpus (or not in the ABC at all) are skipped and listed in the
report so a curator can follow up.

Idempotent and cheap to re-run: already-loaded pairs are read once up front and
skipped before create_tag, so the weekly cron only does real work for new
associations.

Limitation: this loader is add-only. If ZFIN drops a gene-publication
association, the previously created tag persists -- no run retracts it. That is an
accepted trade-off for this "simple, temporary" load (the tags would be dropped
and reloaded wholesale if ZFIN's full data is ever loaded into the Alliance).
"""
import argparse
import logging
from collections import defaultdict
from os import environ, makedirs, path
from typing import Dict, Iterator, Optional, Set, Tuple

from dotenv import load_dotenv
from fastapi import HTTPException

from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (
    TopicEntityTagSchemaPost,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.for_migration.zfin_reference_tag_utils import (
    DANIO_RERIO_TAXON,
    ENTITY_ID_VALIDATION,
    EXISTING_DATA_NOVELTY_ATP,
    MAX_ASSOCIATIONS_PER_PAPER,
    PROGRESS_LOG_INTERVAL,
    ZFIN_CURIE_PREFIX,
    build_zfin_corpus_ref_curies,
    build_zfin_pub_to_ref_curie,
    deliver_report,
    download_file,
    format_not_in_corpus_section,
    get_or_create_source,
    load_existing_entity_pairs,
    new_unresolved_prefix_counter,
    resolve_reference_curie,
    select_over_cap_papers,
    write_id_log,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

ZFIN_GENE_PUBLICATION_URL = "https://zfin.org/downloads/gene_publication.txt"

ZFIN_GENE_ID_PREFIX = "ZDB-GENE"
ZFIN_PUB_ID_PREFIX = "ZDB-PUB"

# ATP:0000005 = "gene". Both the topic and the entity_type are gene.
GENE_ATP = "ATP:0000005"

# Abort the run if this many create_tag calls fail in a row (a sign the DB
# connection or session is wedged, rather than a few bad rows).
ABORT_AFTER_CONSECUTIVE_ERRORS = 25

base_path = environ.get("XML_PATH", "")
file_path = base_path + "zfin_data/"

MISSING_LOG = "zfin_gene_reference_missing_ref_ids.log"
NOT_IN_CORPUS_LOG = "zfin_gene_reference_not_in_corpus.log"
OVER_CAP_LOG = "zfin_gene_reference_over_cap.log"


def parse_gene_publication(file_with_path: str) -> Iterator[Tuple[str, str, str]]:
    """Yield (gene_id, pub_id, pmid) for each row with a ZFIN publication id in
    column 3. The header and any row without a ZDB-PUB publication id are skipped;
    non-gene entity rows (gene_id not ZDB-GENE) are yielded so the caller can
    count them. ``pmid`` is an empty string when column 5 is absent or blank.
    """
    with open(file_with_path) as f:
        for line in f:
            pieces = line.rstrip("\n").split("\t")
            if len(pieces) < 3:
                continue
            gene_id = pieces[1].strip()
            pub_id = pieces[2].strip()
            if not pub_id.startswith(ZFIN_PUB_ID_PREFIX):
                continue
            pmid = pieces[4].strip() if len(pieces) >= 5 else ""
            yield gene_id, pub_id, pmid


def count_gene_associations_per_paper(file_with_path: str) -> Dict[str, Set[str]]:
    """First pass over the file: map each ZFIN publication token to the set of
    distinct gene ids associated with it. Used to identify papers whose gene
    association count exceeds MAX_ASSOCIATIONS_PER_PAPER so they can be skipped
    wholesale (they would otherwise overflow the Elasticsearch nested-object
    limit on the reference page)."""
    genes_by_paper: Dict[str, Set[str]] = defaultdict(set)
    for gene_id, pub_id, _pmid in parse_gene_publication(file_with_path):
        if not gene_id.startswith(ZFIN_GENE_ID_PREFIX):
            continue
        genes_by_paper[f"{ZFIN_CURIE_PREFIX}:{pub_id}"].add(gene_id)
    return genes_by_paper


def _build_tag_payload(reference_curie: str, entity_curie: str,
                       source_id: int) -> TopicEntityTagSchemaPost:
    return TopicEntityTagSchemaPost(
        reference_curie=reference_curie,
        topic=GENE_ATP,
        entity_type=GENE_ATP,
        entity=entity_curie,
        entity_id_validation=ENTITY_ID_VALIDATION,
        species=DANIO_RERIO_TAXON,
        data_novelty=EXISTING_DATA_NOVELTY_ATP,
        negated=False,
        topic_entity_tag_source_id=source_id,
    )


def load_zfin_gene_reference_tags(input_file: Optional[str] = None) -> Dict:
    """Load ZFIN gene-reference associations as entity TETs.

    Args:
        input_file: Optional path to a local gene_publication.txt. When omitted
            the file is downloaded from ZFIN.

    Returns:
        A dict of counts describing the run (also used to build the report).
    """
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    counts: Dict = {
        "total_pairs": 0,
        "created": 0,
        "skipped_duplicate": 0,
        "duplicate_in_file": 0,
        "skipped_non_gene": 0,
        "missing_reference": 0,
        "not_in_corpus": 0,
        "skipped_over_cap": 0,
        "papers_over_cap": 0,
        "errors": 0,
    }
    missing_ref_ids: Set[str] = set()
    # resolved reference curie -> ZFIN publication curie, for references found in
    # the ABC but not in the ZFIN corpus (skipped, reported for curator follow-up).
    not_in_corpus_refs: Dict[str, str] = {}
    unresolved_prefixes = new_unresolved_prefix_counter()

    if input_file:
        file_with_path = input_file
    else:
        makedirs(file_path, exist_ok=True)
        file_with_path = f"{file_path}gene_publication.txt"
        if not download_file(ZFIN_GENE_PUBLICATION_URL, file_with_path):
            db.close()
            counts["download_failed"] = True
            return counts

    try:
        source_id = get_or_create_source(db)
        pub_to_ref_curie = build_zfin_pub_to_ref_curie(db)
        logger.info(f"Loaded {len(pub_to_ref_curie)} ZFIN publication cross_references")
        zfin_corpus_ref_curies = build_zfin_corpus_ref_curies(db)
        logger.info(f"Loaded {len(zfin_corpus_ref_curies)} references in the ZFIN corpus")
        existing_pairs = load_existing_entity_pairs(db, source_id, GENE_ATP)
        logger.info(f"Loaded {len(existing_pairs)} gene tags already present for this source")

        genes_by_paper = count_gene_associations_per_paper(file_with_path)
        over_cap_papers = select_over_cap_papers(genes_by_paper)
        counts["papers_over_cap"] = len(over_cap_papers)
        # Distinct associations withheld (per-paper counts already dedup within
        # the file), so this is the tag count we refuse -- not raw file rows.
        counts["skipped_over_cap"] = sum(over_cap_papers.values())
        logger.info(
            "%d papers exceed %d gene associations and will be skipped",
            len(over_cap_papers), MAX_ASSOCIATIONS_PER_PAPER,
        )

        pmid_cache: Dict[str, Optional[str]] = {}
        seen_pairs: Set[Tuple[str, str]] = set()
        consecutive_errors = 0

        for gene_id, pub_id, pmid in parse_gene_publication(file_with_path):
            counts["total_pairs"] += 1
            if counts["total_pairs"] % PROGRESS_LOG_INTERVAL == 0:
                logger.info(
                    "Processed %d pairs: created=%d skipped_duplicate=%d "
                    "missing_reference=%d not_in_corpus=%d errors=%d",
                    counts["total_pairs"], counts["created"], counts["skipped_duplicate"],
                    counts["missing_reference"], counts["not_in_corpus"], counts["errors"],
                )

            if not gene_id.startswith(ZFIN_GENE_ID_PREFIX):
                counts["skipped_non_gene"] += 1
                continue

            entity_curie = f"{ZFIN_CURIE_PREFIX}:{gene_id}"
            ref_token = f"{ZFIN_CURIE_PREFIX}:{pub_id}"
            if ref_token in over_cap_papers:
                continue
            reference_curie = resolve_reference_curie(
                db, ref_token, pmid, pub_to_ref_curie, pmid_cache, unresolved_prefixes
            )
            if reference_curie is None:
                counts["missing_reference"] += 1
                missing_ref_ids.add(ref_token)
                continue

            if reference_curie not in zfin_corpus_ref_curies:
                counts["not_in_corpus"] += 1
                not_in_corpus_refs.setdefault(reference_curie, ref_token)
                continue

            pair = (reference_curie, entity_curie)
            if pair in existing_pairs:
                counts["skipped_duplicate"] += 1
                continue
            if pair in seen_pairs:
                counts["duplicate_in_file"] += 1
                continue
            seen_pairs.add(pair)

            try:
                _tag_id, was_upsert = create_tag(
                    db, _build_tag_payload(reference_curie, entity_curie, source_id),
                    validate_on_insert=False,
                )
                counts["skipped_duplicate" if was_upsert else "created"] += 1
                consecutive_errors = 0
            except HTTPException as e:
                if e.status_code == 409:
                    counts["skipped_duplicate"] += 1
                    consecutive_errors = 0
                else:
                    db.rollback()
                    counts["errors"] += 1
                    consecutive_errors += 1
                    logger.warning(
                        f"TET create failed for {reference_curie} / {entity_curie}: {e.detail}"
                    )
            except Exception as e:
                db.rollback()
                counts["errors"] += 1
                consecutive_errors += 1
                logger.warning(
                    f"TET create failed for {reference_curie} / {entity_curie}: {e}"
                )

            if consecutive_errors >= ABORT_AFTER_CONSECUTIVE_ERRORS:
                logger.error("Aborting after %d consecutive create_tag errors",
                             consecutive_errors)
                counts["aborted"] = True
                break

        counts["not_in_corpus_refs"] = not_in_corpus_refs
        counts["unresolved_prefixes"] = dict(unresolved_prefixes)
        logger.info(
            "ZFIN gene-reference load done: total_pairs=%d created=%d "
            "skipped_duplicate=%d duplicate_in_file=%d skipped_non_gene=%d "
            "missing_reference=%d not_in_corpus=%d skipped_over_cap=%d "
            "papers_over_cap=%d errors=%d",
            counts["total_pairs"], counts["created"], counts["skipped_duplicate"],
            counts["duplicate_in_file"], counts["skipped_non_gene"],
            counts["missing_reference"], counts["not_in_corpus"],
            counts["skipped_over_cap"], counts["papers_over_cap"], counts["errors"],
        )
        if unresolved_prefixes:
            logger.info("Unresolved reference curie prefixes: %s", dict(unresolved_prefixes))
        write_id_log(MISSING_LOG,
                     f"ZFIN reference ids not found in ABC ({len(missing_ref_ids)})",
                     sorted(missing_ref_ids))
        write_id_log(NOT_IN_CORPUS_LOG,
                     f"References not in the ZFIN corpus ({len(not_in_corpus_refs)})",
                     [f"{tok}\t{ref}" for ref, tok in sorted(not_in_corpus_refs.items())])
        write_id_log(OVER_CAP_LOG,
                     f"Papers skipped for exceeding {MAX_ASSOCIATIONS_PER_PAPER} "
                     f"gene associations ({len(over_cap_papers)})",
                     [f"{tok}\t{count}" for tok, count in sorted(over_cap_papers.items())])
        return counts
    finally:
        db.close()


def compose_report_message(counts: Dict) -> str:
    """Compose the HTML email report message from the run counts."""
    message = "<b>ZFIN Gene-Reference Association Loading Report</b><p>"
    if counts.get("download_failed"):
        message += "<ul><li>Failed to download ZFIN gene_publication.txt</ul>"
        return message
    message += "<ul>"
    if counts.get("aborted"):
        message += "<li><b>RUN ABORTED early after consecutive create_tag errors</b>"
    message += f"<li>Total gene-reference pairs in file: {counts['total_pairs']}"
    message += f"<li>Entity tags created: {counts['created']}"
    message += f"<li>Already present (skipped): {counts['skipped_duplicate']}"
    message += f"<li>Duplicate pairs within file: {counts['duplicate_in_file']}"
    message += f"<li>Non-gene entity rows skipped: {counts['skipped_non_gene']}"
    message += f"<li>References not found in ABC: {counts['missing_reference']}"
    message += f"<li>Associations skipped (paper not in ZFIN corpus): {counts['not_in_corpus']}"
    message += (f"<li>Papers skipped (&gt; {MAX_ASSOCIATIONS_PER_PAPER} gene associations): "
                f"{counts['papers_over_cap']} papers, {counts['skipped_over_cap']} associations")
    message += f"<li>Errors: {counts['errors']}"
    message += format_not_in_corpus_section(counts.get("not_in_corpus_refs", {}),
                                            NOT_IN_CORPUS_LOG)
    message += "</ul>"
    return message


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Load ZFIN gene-reference associations as entity topic entity tags"
    )
    parser.add_argument(
        "-f", "--input-file",
        help="Path to a local gene_publication.txt (default: download from ZFIN)",
    )
    parser.add_argument(
        "-n", "--no-email",
        action="store_true",
        help="Do not email the report (for testing)",
    )
    args = parser.parse_args()

    run_counts = load_zfin_gene_reference_tags(input_file=args.input_file)
    report = compose_report_message(run_counts)
    deliver_report("ZFIN Gene-Reference Association Loading Report", report, args.no_email)
