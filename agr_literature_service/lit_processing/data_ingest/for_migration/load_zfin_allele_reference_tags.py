"""
load_zfin_allele_reference_tags.py
==================================

Load ZFIN allele-reference associations as entity topic entity tags (TETs) so the
associations display on the reference pages of the public website.

Data source: https://zfin.org/downloads/ZFIN_1.0.1.4_Allele_ml.json
(Alliance LinkML allele submission file.) Each allele stanza provides:
    primary_external_id   e.g. "ZFIN:ZDB-ALT-000209-24"   -> entity
    taxon_curie           e.g. "NCBITaxon:7955"           -> species
    reference_curies      e.g. ["ZFIN:ZDB-PUB-150729-10", ...] -> references
    internal / obsolete   booleans (internal/obsolete records are skipped)

Every (allele, reference) pair becomes a "pure entity" allele tag
(topic == entity_type == allele) from the shared ZFIN reference-curation source
(the same source used by the gene load). This is intentionally a simple load:
created_by and updated_by are the script's automation user and the dates are the
load date; if ZFIN's full data is ever loaded into the Alliance these tags would
be dropped and reloaded with real history.

Only references already in the ZFIN corpus are tagged; associations whose paper is
not in the ZFIN corpus (or not in the ABC at all) are skipped and listed in the
report so a curator can follow up. ``reference_curies`` may contain PMID: curies
as well as ZFIN publication curies; both are resolved.

Idempotent and cheap to re-run: already-loaded pairs are read once up front and
skipped before create_tag, so the weekly cron only does real work for new
associations.

Limitation: this loader is add-only. If ZFIN drops an allele-publication
association, the previously created tag persists -- no run retracts it. That is an
accepted trade-off for this "simple, temporary" load (the tags would be dropped
and reloaded wholesale if ZFIN's full data is ever loaded into the Alliance).
"""
import argparse
import json
import logging
from collections import defaultdict
from os import environ, makedirs, path
from typing import Dict, Iterator, List, Optional, Set, Tuple

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
    EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP,
    MAX_ASSOCIATIONS_PER_PAPER,
    PROGRESS_LOG_INTERVAL,
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

ZFIN_ALLELE_JSON_URL = "https://zfin.org/downloads/ZFIN_1.0.1.4_Allele_ml.json"

# ATP:0000006 = "allele". Both the topic and the entity_type are allele.
ALLELE_ATP = "ATP:0000006"

# Abort the run if this many create_tag calls fail in a row.
ABORT_AFTER_CONSECUTIVE_ERRORS = 25

base_path = environ.get("XML_PATH", "")
file_path = base_path + "zfin_data/"

MISSING_LOG = "zfin_allele_reference_missing_ref_curies.log"
NOT_IN_CORPUS_LOG = "zfin_allele_reference_not_in_corpus.log"
OVER_CAP_LOG = "zfin_allele_reference_over_cap.log"


def _extract_ingest_records(data) -> List[Dict]:
    """Return the list of allele records from the parsed JSON, tolerating either a
    bare list or the Alliance ``*_ingest_set`` wrapper object. Logs which key was
    used so a wrapper-key rename becomes diagnosable rather than silent."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("allele_ingest_set", "data"):
            if isinstance(data.get(key), list):
                return data[key]
        for key, value in data.items():
            if isinstance(value, list):
                logger.warning("Allele records read from unexpected top-level key '%s'", key)
                return value
    return []


def parse_allele_records(file_with_path: str) -> Iterator[Tuple[str, str, List[str]]]:
    """Yield (allele_curie, taxon_curie, reference_curies) for each non-internal,
    non-obsolete allele stanza that has a primary_external_id."""
    with open(file_with_path) as f:
        data = json.load(f)
    for rec in _extract_ingest_records(data):
        if not isinstance(rec, dict):
            continue
        if rec.get("internal") is True or rec.get("obsolete") is True:
            continue
        allele_curie = rec.get("primary_external_id")
        if not allele_curie:
            continue
        taxon_curie = rec.get("taxon_curie") or DANIO_RERIO_TAXON
        reference_curies = rec.get("reference_curies") or []
        yield allele_curie, taxon_curie, reference_curies


def count_allele_associations_per_paper(file_with_path: str) -> Dict[str, Set[str]]:
    """First pass over the file: map each reference curie to the set of distinct
    allele curies associated with it. Used to identify papers whose allele
    association count exceeds MAX_ASSOCIATIONS_PER_PAPER so they can be skipped
    wholesale (they would otherwise overflow the Elasticsearch nested-object
    limit on the reference page)."""
    alleles_by_paper: Dict[str, Set[str]] = defaultdict(set)
    for allele_curie, _taxon_curie, reference_curies in parse_allele_records(file_with_path):
        for zfin_ref_curie in reference_curies:
            alleles_by_paper[zfin_ref_curie].add(allele_curie)
    return alleles_by_paper


def _build_tag_payload(reference_curie: str, allele_curie: str, species_curie: str,
                       source_id: int) -> TopicEntityTagSchemaPost:
    return TopicEntityTagSchemaPost(
        reference_curie=reference_curie,
        topic=ALLELE_ATP,
        entity_type=ALLELE_ATP,
        entity=allele_curie,
        entity_id_validation=ENTITY_ID_VALIDATION,
        species=species_curie,
        data_novelty=EXISTING_DATA_NOVELTY_ATP,
        data_context=EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP,
        negated=False,
        topic_entity_tag_source_id=source_id,
    )


def _resolve_input_file(input_file: Optional[str], db, counts: Dict) -> Optional[str]:
    """Return the local allele JSON path to read, downloading it from ZFIN when
    ``input_file`` is not given. Returns None (after closing ``db`` and marking
    ``counts['download_failed']``) if the download fails."""
    if input_file:
        return input_file
    makedirs(file_path, exist_ok=True)
    file_with_path = f"{file_path}ZFIN_Allele_ml.json"
    if download_file(ZFIN_ALLELE_JSON_URL, file_with_path):
        return file_with_path
    db.close()
    counts["download_failed"] = True
    return None


def load_zfin_allele_reference_tags(input_file: Optional[str] = None) -> Dict:
    """Load ZFIN allele-reference associations as entity TETs.

    Args:
        input_file: Optional path to a local ZFIN allele JSON file. When omitted
            the file is downloaded from ZFIN.

    Returns:
        A dict of counts describing the run (also used to build the report).
    """
    db = create_postgres_session(False)
    script_name = path.basename(__file__).replace(".py", "")
    set_global_user_id(db, script_name)

    counts: Dict = {
        "total_alleles": 0,
        "total_pairs": 0,
        "created": 0,
        "skipped_duplicate": 0,
        "duplicate_in_file": 0,
        "missing_reference": 0,
        "not_in_corpus": 0,
        "skipped_over_cap": 0,
        "papers_over_cap": 0,
        "errors": 0,
    }
    missing_ref_curies: Set[str] = set()
    # resolved reference curie -> ZFIN publication curie, for references found in
    # the ABC but not in the ZFIN corpus (skipped, reported for curator follow-up).
    not_in_corpus_refs: Dict[str, str] = {}
    unresolved_prefixes = new_unresolved_prefix_counter()

    file_with_path = _resolve_input_file(input_file, db, counts)
    if file_with_path is None:
        return counts

    try:
        source_id = get_or_create_source(db)
        pub_to_ref_curie = build_zfin_pub_to_ref_curie(db)
        logger.info(f"Loaded {len(pub_to_ref_curie)} ZFIN publication cross_references")
        zfin_corpus_ref_curies = build_zfin_corpus_ref_curies(db)
        logger.info(f"Loaded {len(zfin_corpus_ref_curies)} references in the ZFIN corpus")
        existing_pairs = load_existing_entity_pairs(db, source_id, ALLELE_ATP)
        logger.info(f"Loaded {len(existing_pairs)} allele tags already present for this source")

        alleles_by_paper = count_allele_associations_per_paper(file_with_path)
        over_cap_papers = select_over_cap_papers(alleles_by_paper)
        counts["papers_over_cap"] = len(over_cap_papers)
        # Distinct associations withheld (per-paper counts already dedup within
        # the file), so this is the tag count we refuse -- not raw file pairs.
        counts["skipped_over_cap"] = sum(over_cap_papers.values())
        logger.info(
            "%d papers exceed %d allele associations and will be skipped",
            len(over_cap_papers), MAX_ASSOCIATIONS_PER_PAPER,
        )

        pmid_cache: Dict[str, Optional[str]] = {}
        seen_pairs: Set[Tuple[str, str]] = set()
        consecutive_errors = 0

        for allele_curie, taxon_curie, reference_curies in parse_allele_records(file_with_path):
            counts["total_alleles"] += 1
            for zfin_ref_curie in reference_curies:
                counts["total_pairs"] += 1
                if counts["total_pairs"] % PROGRESS_LOG_INTERVAL == 0:
                    logger.info(
                        "Processed %d pairs (%d alleles): created=%d skipped_duplicate=%d "
                        "missing_reference=%d not_in_corpus=%d errors=%d",
                        counts["total_pairs"], counts["total_alleles"], counts["created"],
                        counts["skipped_duplicate"], counts["missing_reference"],
                        counts["not_in_corpus"], counts["errors"],
                    )

                if zfin_ref_curie in over_cap_papers:
                    continue

                reference_curie = resolve_reference_curie(
                    db, zfin_ref_curie, None, pub_to_ref_curie, pmid_cache,
                    unresolved_prefixes,
                )
                if reference_curie is None:
                    counts["missing_reference"] += 1
                    missing_ref_curies.add(zfin_ref_curie)
                    continue

                if reference_curie not in zfin_corpus_ref_curies:
                    counts["not_in_corpus"] += 1
                    not_in_corpus_refs.setdefault(reference_curie, zfin_ref_curie)
                    continue

                pair = (reference_curie, allele_curie)
                if pair in existing_pairs:
                    counts["skipped_duplicate"] += 1
                    continue
                if pair in seen_pairs:
                    counts["duplicate_in_file"] += 1
                    continue
                seen_pairs.add(pair)

                try:
                    _tag_id, was_upsert = create_tag(
                        db, _build_tag_payload(reference_curie, allele_curie,
                                               taxon_curie, source_id),
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
                            f"TET create failed for {reference_curie} / {allele_curie}: {e.detail}"
                        )
                except Exception as e:
                    db.rollback()
                    counts["errors"] += 1
                    consecutive_errors += 1
                    logger.warning(
                        f"TET create failed for {reference_curie} / {allele_curie}: {e}"
                    )

                if consecutive_errors >= ABORT_AFTER_CONSECUTIVE_ERRORS:
                    logger.error("Aborting after %d consecutive create_tag errors",
                                 consecutive_errors)
                    counts["aborted"] = True
                    break
            if counts.get("aborted"):
                break

        counts["not_in_corpus_refs"] = not_in_corpus_refs
        counts["unresolved_prefixes"] = dict(unresolved_prefixes)
        logger.info(
            "ZFIN allele-reference load done: total_alleles=%d total_pairs=%d created=%d "
            "skipped_duplicate=%d duplicate_in_file=%d missing_reference=%d "
            "not_in_corpus=%d skipped_over_cap=%d papers_over_cap=%d errors=%d",
            counts["total_alleles"], counts["total_pairs"], counts["created"],
            counts["skipped_duplicate"], counts["duplicate_in_file"],
            counts["missing_reference"], counts["not_in_corpus"],
            counts["skipped_over_cap"], counts["papers_over_cap"], counts["errors"],
        )
        if unresolved_prefixes:
            logger.info("Unresolved reference curie prefixes: %s", dict(unresolved_prefixes))
        write_id_log(MISSING_LOG,
                     f"ZFIN reference curies not found in ABC ({len(missing_ref_curies)})",
                     sorted(missing_ref_curies))
        write_id_log(NOT_IN_CORPUS_LOG,
                     f"References not in the ZFIN corpus ({len(not_in_corpus_refs)})",
                     [f"{tok}\t{ref}" for ref, tok in sorted(not_in_corpus_refs.items())])
        write_id_log(OVER_CAP_LOG,
                     f"Papers skipped for exceeding {MAX_ASSOCIATIONS_PER_PAPER} "
                     f"allele associations ({len(over_cap_papers)})",
                     [f"{curie}\t{count}" for curie, count in sorted(over_cap_papers.items())])
        return counts
    finally:
        db.close()


def compose_report_message(counts: Dict) -> str:
    """Compose the HTML email report message from the run counts."""
    message = "<b>ZFIN Allele-Reference Association Loading Report</b><p>"
    if counts.get("download_failed"):
        message += "<ul><li>Failed to download ZFIN allele JSON file</ul>"
        return message
    message += "<ul>"
    if counts.get("aborted"):
        message += "<li><b>RUN ABORTED early after consecutive create_tag errors</b>"
    message += f"<li>Alleles in file: {counts['total_alleles']}"
    message += f"<li>Total allele-reference pairs in file: {counts['total_pairs']}"
    message += f"<li>Entity tags created: {counts['created']}"
    message += f"<li>Already present (skipped): {counts['skipped_duplicate']}"
    message += f"<li>Duplicate pairs within file: {counts['duplicate_in_file']}"
    message += f"<li>References not found in ABC: {counts['missing_reference']}"
    message += f"<li>Associations skipped (paper not in ZFIN corpus): {counts['not_in_corpus']}"
    message += (f"<li>Papers skipped (&gt; {MAX_ASSOCIATIONS_PER_PAPER} allele associations): "
                f"{counts['papers_over_cap']} papers, {counts['skipped_over_cap']} associations")
    message += f"<li>Errors: {counts['errors']}"
    message += format_not_in_corpus_section(counts.get("not_in_corpus_refs", {}),
                                            NOT_IN_CORPUS_LOG)
    message += "</ul>"
    return message


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Load ZFIN allele-reference associations as entity topic entity tags"
    )
    parser.add_argument(
        "-f", "--input-file",
        help="Path to a local ZFIN allele JSON file (default: download from ZFIN)",
    )
    parser.add_argument(
        "-n", "--no-email",
        action="store_true",
        help="Do not email the report (for testing)",
    )
    args = parser.parse_args()

    run_counts = load_zfin_allele_reference_tags(input_file=args.input_file)
    report = compose_report_message(run_counts)
    deliver_report("ZFIN Allele-Reference Association Loading Report", report, args.no_email)
