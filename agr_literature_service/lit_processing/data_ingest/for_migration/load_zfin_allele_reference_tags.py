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
(the same source used by the gene load). This is intentionally a simple load: the
created and updated dates are just the load date, and if ZFIN's full data is ever
loaded into the Alliance these tags would be dropped and reloaded with real
history.

Only references already in the ZFIN corpus are tagged; associations whose paper is
not in the ZFIN corpus (or not in the ABC at all) are skipped and listed in the
report so a curator can follow up.

Idempotent: ``create_tag`` skips tags that already exist, so this can be run for
the initial load and then re-run weekly against the same file to pick up only the
newly added associations.
"""
import argparse
import json
import logging
from os import environ, makedirs, path
from typing import Dict, Iterator, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv
from fastapi import HTTPException

from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (
    TopicEntityTagSchemaPost,
)
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.for_migration.load_zfin_gene_reference_tags import (
    DANIO_RERIO_TAXON,
    ENTITY_ID_VALIDATION,
    EXISTING_DATA_NOVELTY_ATP,
    PROGRESS_LOG_INTERVAL,
    build_zfin_corpus_ref_curies,
    build_zfin_pub_to_ref_curie,
    get_or_create_source,
)
from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

ZFIN_ALLELE_JSON_URL = "https://zfin.org/downloads/ZFIN_1.0.1.4_Allele_ml.json"

# ATP:0000006 = "allele". Both the topic and the entity_type are allele, so each
# tag is a pure entity tag whose data_novelty is the "existing data" term.
ALLELE_ATP = "ATP:0000006"

base_path = environ.get("XML_PATH", "")
file_path = base_path + "zfin_data/"
log_path = environ.get("LOG_PATH", "")


def download_zfin_allele_json(file_with_path: str) -> bool:  # pragma: no cover
    """Download the ZFIN allele LinkML JSON file. Returns True on success."""
    logger.info(f"Downloading ZFIN allele file from {ZFIN_ALLELE_JSON_URL}")
    try:
        response = requests.get(ZFIN_ALLELE_JSON_URL, timeout=300, stream=True)
        response.raise_for_status()
        with open(file_with_path, "wb") as outfile:
            for chunk in response.iter_content(chunk_size=8192):
                outfile.write(chunk)
        logger.info(f"Downloaded ZFIN allele file to {file_with_path}")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to download ZFIN allele file: {e}")
        return False


def _extract_ingest_records(data) -> List[Dict]:
    """Return the list of allele records from the parsed JSON, tolerating either a
    bare list or the Alliance ``*_ingest_set`` wrapper object."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("allele_ingest_set", "data"):
            if isinstance(data.get(key), list):
                return data[key]
        for value in data.values():
            if isinstance(value, list):
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
        negated=False,
        topic_entity_tag_source_id=source_id,
    )


def load_zfin_allele_reference_tags(input_file: Optional[str] = None) -> Dict:  # pragma: no cover
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
        "errors": 0,
    }
    missing_ref_curies: Set[str] = set()
    # resolved reference curie -> ZFIN publication curie, for references found in
    # the ABC but not in the ZFIN corpus (skipped, reported for curator follow-up).
    not_in_corpus_refs: Dict[str, str] = {}

    if input_file:
        file_with_path = input_file
    else:
        makedirs(file_path, exist_ok=True)
        file_with_path = f"{file_path}ZFIN_Allele_ml.json"
        if not download_zfin_allele_json(file_with_path):
            db.close()
            counts["download_failed"] = True
            return counts

    try:
        source_id = get_or_create_source(db)
        pub_to_ref_curie = build_zfin_pub_to_ref_curie(db)
        logger.info(f"Loaded {len(pub_to_ref_curie)} ZFIN publication cross_references")
        zfin_corpus_ref_curies = build_zfin_corpus_ref_curies(db)
        logger.info(f"Loaded {len(zfin_corpus_ref_curies)} references in the ZFIN corpus")

        seen_pairs: Set[Tuple[str, str]] = set()

        for allele_curie, taxon_curie, reference_curies in parse_allele_records(file_with_path):
            counts["total_alleles"] += 1
            for zfin_pub_curie in reference_curies:
                counts["total_pairs"] += 1
                if counts["total_pairs"] % PROGRESS_LOG_INTERVAL == 0:
                    logger.info(
                        "Processed %d pairs (%d alleles): created=%d skipped_duplicate=%d "
                        "missing_reference=%d not_in_corpus=%d errors=%d",
                        counts["total_pairs"], counts["total_alleles"], counts["created"],
                        counts["skipped_duplicate"], counts["missing_reference"],
                        counts["not_in_corpus"], counts["errors"],
                    )

                reference_curie = pub_to_ref_curie.get(zfin_pub_curie)
                if reference_curie is None:
                    counts["missing_reference"] += 1
                    missing_ref_curies.add(zfin_pub_curie)
                    continue

                if reference_curie not in zfin_corpus_ref_curies:
                    counts["not_in_corpus"] += 1
                    not_in_corpus_refs.setdefault(reference_curie, zfin_pub_curie)
                    continue

                pair = (reference_curie, allele_curie)
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
                    if was_upsert:
                        counts["skipped_duplicate"] += 1
                    else:
                        counts["created"] += 1
                except HTTPException as e:
                    if e.status_code == 409:
                        counts["skipped_duplicate"] += 1
                    else:
                        counts["errors"] += 1
                        logger.warning(
                            f"TET create failed for {reference_curie} / {allele_curie}: {e.detail}"
                        )
                except Exception as e:
                    counts["errors"] += 1
                    logger.warning(
                        f"TET create failed for {reference_curie} / {allele_curie}: {e}"
                    )

        counts["not_in_corpus_refs"] = not_in_corpus_refs
        logger.info(
            "ZFIN allele-reference load done: total_alleles=%d total_pairs=%d created=%d "
            "skipped_duplicate=%d duplicate_in_file=%d missing_reference=%d "
            "not_in_corpus=%d errors=%d",
            counts["total_alleles"], counts["total_pairs"], counts["created"],
            counts["skipped_duplicate"], counts["duplicate_in_file"],
            counts["missing_reference"], counts["not_in_corpus"], counts["errors"],
        )
        write_missing_ref_curies_log(missing_ref_curies)
        write_not_in_corpus_log(not_in_corpus_refs)
        return counts
    finally:
        db.close()


def write_missing_ref_curies_log(missing_ref_curies: Set[str]) -> None:  # pragma: no cover
    """Write the ZFIN reference curies that could not be matched to an ABC reference."""
    if not log_path or not missing_ref_curies:
        return
    logfile_name = "zfin_allele_reference_missing_ref_curies.log"
    with open(log_path + logfile_name, "w") as fw:
        fw.write(f"ZFIN reference curies not found in ABC ({len(missing_ref_curies)}):\n\n")
        for ref_curie in sorted(missing_ref_curies):
            fw.write(f"{ref_curie}\n")


def write_not_in_corpus_log(not_in_corpus_refs: Dict[str, str]) -> None:  # pragma: no cover
    """Write the references found in the ABC but not in the ZFIN corpus (skipped)."""
    if not log_path or not not_in_corpus_refs:
        return
    logfile_name = "zfin_allele_reference_not_in_corpus.log"
    with open(log_path + logfile_name, "w") as fw:
        fw.write(f"References not in the ZFIN corpus ({len(not_in_corpus_refs)}):\n\n")
        for reference_curie, zfin_pub_curie in sorted(not_in_corpus_refs.items()):
            fw.write(f"{zfin_pub_curie}\t{reference_curie}\n")


def compose_report_message(counts: Dict) -> str:  # pragma: no cover
    """Compose the HTML Slack report message from the run counts."""
    message = "<b>ZFIN Allele-Reference Association Loading Report</b><p>"
    if counts.get("download_failed"):
        message += "<ul><li>Failed to download ZFIN allele JSON file</ul>"
        return message
    message += "<ul>"
    message += f"<li>Alleles in file: {counts['total_alleles']}"
    message += f"<li>Total allele-reference pairs in file: {counts['total_pairs']}"
    message += f"<li>Entity tags created: {counts['created']}"
    message += f"<li>Already present (skipped): {counts['skipped_duplicate']}"
    message += f"<li>Duplicate pairs within file: {counts['duplicate_in_file']}"
    message += f"<li>References not found in ABC: {counts['missing_reference']}"
    message += f"<li>Associations skipped (paper not in ZFIN corpus): {counts['not_in_corpus']}"
    message += f"<li>Errors: {counts['errors']}"

    not_in_corpus_refs: Dict[str, str] = counts.get("not_in_corpus_refs", {})
    if not_in_corpus_refs:
        message += (f"<li>Papers not in ZFIN corpus "
                    f"({len(not_in_corpus_refs)}):<br>")
        for reference_curie, zfin_pub_curie in sorted(not_in_corpus_refs.items()):
            message += f"{zfin_pub_curie} ({reference_curie})<br>"

    message += "</ul>"
    return message


def send_slack_report(message: str):  # pragma: no cover
    """Send the report to Slack."""
    send_report("ZFIN Allele-Reference Association Loading Report", message)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Load ZFIN allele-reference associations as entity topic entity tags"
    )
    parser.add_argument(
        "-f", "--input-file",
        help="Path to a local ZFIN allele JSON file (default: download from ZFIN)",
    )
    parser.add_argument(
        "-n", "--no-slack",
        action="store_true",
        help="Do not send the Slack report (for testing)",
    )
    args = parser.parse_args()

    run_counts = load_zfin_allele_reference_tags(input_file=args.input_file)
    report = compose_report_message(run_counts)

    if args.no_slack:
        logger.info("Slack report disabled. Message content:")
        logger.info(report)
    else:
        send_slack_report(report)
