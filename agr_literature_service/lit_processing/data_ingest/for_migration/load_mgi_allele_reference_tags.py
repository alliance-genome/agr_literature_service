"""
load_mgi_allele_reference_tags.py
=================================

Load MGI allele-reference associations as entity topic entity tags (TETs) so
the associations display on the reference pages of the public website
(SCRUM-6495). Same shape as ``load_zfin_allele_reference_tags.py``.

Data source: the Alliance curation (A-team) "MGI Allele Load" bulk-load file —
the latest file is discovered through the curation API and downloaded from the
public agr-curation-files S3 bucket (gzipped LinkML JSON). Each allele stanza
provides:
    primary_external_id   e.g. "MGI:1855930"                -> entity
    taxon_curie           e.g. "NCBITaxon:10090"            -> species
    reference_curies      e.g. ["PMID:41414675", "MGI:6414854"] -> references
    internal / obsolete   booleans (internal/obsolete records are skipped)

Every (allele, reference) pair becomes a "pure entity" allele tag
(topic == entity_type == allele) from the shared MGI reference-curation source.
created_by and updated_by are the script's automation user and the dates are
the load date (the file only says MGI:curation_staff).

MGI lists most papers under BOTH their MGI (J-number) and PMID curies, so
references are resolved to ABC reference curies FIRST and everything downstream
— the per-paper association cap, in-file dedup, and the already-loaded skip set
— operates on resolved curies. Only references already in the MGI corpus are
tagged; associations whose paper is not in the MGI corpus (or not in the ABC at
all) are skipped and listed in the report so a curator can follow up.

Idempotent and cheap to re-run: already-loaded pairs are read once up front and
skipped before create_tag, so a scheduled re-run only does real work for new
associations.

Limitation: this loader is add-only. If MGI drops an allele-publication
association, the previously created tag persists — no run retracts it (same
accepted trade-off as the ZFIN loaders).
"""
import argparse
import gzip
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
from agr_literature_service.lit_processing.data_ingest.for_migration.mgi_reference_tag_utils import (
    ALLELE_ATP,
    MUS_MUSCULUS_TAXON,
    build_mgi_corpus_ref_curies,
    build_pub_to_ref_curie,
    find_latest_allele_file_url,
    format_not_in_corpus_section,
    get_or_create_source,
)
from agr_literature_service.lit_processing.data_ingest.for_migration.zfin_reference_tag_utils import (
    ENTITY_ID_VALIDATION,
    EXISTING_DATA_NOVELTY_ATP,
    MAX_ASSOCIATIONS_PER_PAPER,
    NOT_IN_CORPUS_REPORT_CAP,
    PROGRESS_LOG_INTERVAL,
    deliver_report,
    download_file,
    load_existing_entity_pairs,
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

# Abort the run if this many create_tag calls fail in a row.
ABORT_AFTER_CONSECUTIVE_ERRORS = 25

base_path = environ.get("XML_PATH", "")
file_path = base_path + "mgi_data/"

MISSING_LOG = "mgi_allele_reference_missing_ref_curies.log"
NOT_IN_CORPUS_LOG = "mgi_allele_reference_not_in_corpus.log"
OVER_CAP_LOG = "mgi_allele_reference_over_cap.log"


def _open_maybe_gzip(file_with_path: str):
    """Open a bulk-load file for text reading, transparently handling .gz."""
    if file_with_path.endswith(".gz"):
        return gzip.open(file_with_path, "rt")
    return open(file_with_path)


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
    with _open_maybe_gzip(file_with_path) as f:
        data = json.load(f)
    for rec in _extract_ingest_records(data):
        if not isinstance(rec, dict):
            continue
        if rec.get("internal") is True or rec.get("obsolete") is True:
            continue
        allele_curie = rec.get("primary_external_id")
        if not allele_curie:
            continue
        taxon_curie = rec.get("taxon_curie") or MUS_MUSCULUS_TAXON
        reference_curies = rec.get("reference_curies") or []
        yield allele_curie, taxon_curie, reference_curies


def count_allele_associations_per_paper(
        file_with_path: str,
        pub_to_ref_curie: Dict[str, str]) -> Dict[str, Set[str]]:
    """First pass over the file: map each RESOLVED reference curie to the set of
    distinct allele curies associated with it, so papers exceeding
    MAX_ASSOCIATIONS_PER_PAPER can be skipped wholesale (they would otherwise
    overflow the Elasticsearch nested-object limit on the reference page).

    Counting resolved curies — not raw tokens — matters for MGI: most papers
    appear under both an MGI J-number and a PMID, and counting the raw tokens
    would split one paper's associations across two counters, letting it slip
    under the cap. Unresolvable tokens are ignored here; the main pass reports
    them as missing references."""
    alleles_by_paper: Dict[str, Set[str]] = defaultdict(set)
    for allele_curie, _taxon_curie, reference_curies in parse_allele_records(file_with_path):
        for ref_token in reference_curies:
            reference_curie = pub_to_ref_curie.get(ref_token)
            if reference_curie:
                alleles_by_paper[reference_curie].add(allele_curie)
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
        negated=False,
        topic_entity_tag_source_id=source_id,
    )


def _resolve_input_file(input_file: Optional[str], file_url: Optional[str],
                        db, counts: Dict) -> Optional[str]:
    """Return the local allele JSON(.gz) path to read. When ``input_file`` is not
    given, discover the latest A-team file URL (unless ``file_url`` overrides it)
    and download it. Returns None (after closing ``db`` and marking
    ``counts['download_failed']``) when discovery or download fails."""
    if input_file:
        return input_file
    url = file_url or find_latest_allele_file_url()
    if url:
        makedirs(file_path, exist_ok=True)
        file_with_path = f"{file_path}MGI_Allele_ml.json.gz"
        if download_file(url, file_with_path):
            return file_with_path
    db.close()
    counts["download_failed"] = True
    return None


def load_mgi_allele_reference_tags(input_file: Optional[str] = None,
                                   file_url: Optional[str] = None) -> Dict:
    """Load MGI allele-reference associations as entity TETs.

    Args:
        input_file: Optional path to a local MGI allele JSON(.gz) file. When
            omitted the latest A-team bulk-load file is discovered and
            downloaded.
        file_url: Optional explicit file URL, bypassing the curation-API lookup.

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
    # resolved reference curie -> file reference token, for references found in
    # the ABC but not in the MGI corpus (skipped, reported for curator follow-up).
    not_in_corpus_refs: Dict[str, str] = {}
    unresolved_prefixes: Dict[str, int] = defaultdict(int)

    file_with_path = _resolve_input_file(input_file, file_url, db, counts)
    if file_with_path is None:
        return counts

    try:
        source_id = get_or_create_source(db)
        pub_to_ref_curie = build_pub_to_ref_curie(db)
        logger.info(f"Loaded {len(pub_to_ref_curie)} MGI/PMID publication cross_references")
        mgi_corpus_ref_curies = build_mgi_corpus_ref_curies(db)
        logger.info(f"Loaded {len(mgi_corpus_ref_curies)} references in the MGI corpus")
        existing_pairs = load_existing_entity_pairs(db, source_id, ALLELE_ATP)
        logger.info(f"Loaded {len(existing_pairs)} allele tags already present for this source")

        alleles_by_paper = count_allele_associations_per_paper(file_with_path, pub_to_ref_curie)
        over_cap_papers = select_over_cap_papers(alleles_by_paper)
        counts["papers_over_cap"] = len(over_cap_papers)
        # Distinct associations withheld (per-paper counts already dedup within
        # the file), so this is the tag count we refuse -- not raw file pairs.
        counts["skipped_over_cap"] = sum(over_cap_papers.values())
        logger.info(
            "%d papers exceed %d allele associations and will be skipped",
            len(over_cap_papers), MAX_ASSOCIATIONS_PER_PAPER,
        )

        seen_pairs: Set[Tuple[str, str]] = set()
        consecutive_errors = 0

        for allele_curie, taxon_curie, reference_curies in parse_allele_records(file_with_path):
            counts["total_alleles"] += 1
            for ref_token in reference_curies:
                counts["total_pairs"] += 1
                if counts["total_pairs"] % PROGRESS_LOG_INTERVAL == 0:
                    logger.info(
                        "Processed %d pairs (%d alleles): created=%d skipped_duplicate=%d "
                        "duplicate_in_file=%d missing_reference=%d not_in_corpus=%d errors=%d",
                        counts["total_pairs"], counts["total_alleles"], counts["created"],
                        counts["skipped_duplicate"], counts["duplicate_in_file"],
                        counts["missing_reference"], counts["not_in_corpus"], counts["errors"],
                    )

                reference_curie = pub_to_ref_curie.get(ref_token)
                if reference_curie is None:
                    counts["missing_reference"] += 1
                    missing_ref_curies.add(ref_token)
                    prefix = ref_token.split(":", 1)[0] if ":" in ref_token else ref_token
                    unresolved_prefixes[prefix] += 1
                    continue

                if reference_curie in over_cap_papers:
                    continue

                if reference_curie not in mgi_corpus_ref_curies:
                    counts["not_in_corpus"] += 1
                    not_in_corpus_refs.setdefault(reference_curie, ref_token)
                    continue

                pair = (reference_curie, allele_curie)
                if pair in existing_pairs:
                    counts["skipped_duplicate"] += 1
                    continue
                # Also collapses the same paper listed under both its MGI J-number
                # and its PMID within one allele stanza.
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
            "MGI allele-reference load done: total_alleles=%d total_pairs=%d created=%d "
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
                     f"MGI reference curies not found in ABC ({len(missing_ref_curies)})",
                     sorted(missing_ref_curies))
        write_id_log(NOT_IN_CORPUS_LOG,
                     f"References not in the MGI corpus ({len(not_in_corpus_refs)})",
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
    message = "<b>MGI Allele-Reference Association Loading Report</b><p>"
    if counts.get("download_failed"):
        message += "<ul><li>Failed to discover/download the MGI allele bulk-load file</ul>"
        return message
    message += "<ul>"
    if counts.get("aborted"):
        message += "<li><b>RUN ABORTED early after consecutive create_tag errors</b>"
    message += f"<li>Alleles in file: {counts['total_alleles']}"
    message += f"<li>Total allele-reference pairs in file: {counts['total_pairs']}"
    message += f"<li>Entity tags created: {counts['created']}"
    message += f"<li>Already present (skipped): {counts['skipped_duplicate']}"
    message += f"<li>Duplicate pairs within file (incl. MGI+PMID double listings): {counts['duplicate_in_file']}"
    message += f"<li>References not found in ABC: {counts['missing_reference']}"
    message += f"<li>Associations skipped (paper not in MGI corpus): {counts['not_in_corpus']}"
    message += (f"<li>Papers skipped (&gt; {MAX_ASSOCIATIONS_PER_PAPER} allele associations): "
                f"{counts['papers_over_cap']} papers, {counts['skipped_over_cap']} associations")
    message += f"<li>Errors: {counts['errors']}"
    message += format_not_in_corpus_section(counts.get("not_in_corpus_refs", {}),
                                            NOT_IN_CORPUS_LOG, NOT_IN_CORPUS_REPORT_CAP)
    message += "</ul>"
    return message


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Load MGI allele-reference associations as entity topic entity tags"
    )
    parser.add_argument(
        "-f", "--input-file",
        help="Path to a local MGI allele JSON(.gz) file "
             "(default: discover + download the latest A-team bulk-load file)",
    )
    parser.add_argument(
        "-u", "--file-url",
        help="Explicit bulk-load file URL, bypassing the curation-API lookup",
    )
    parser.add_argument(
        "-n", "--no-email",
        action="store_true",
        help="Do not email the report (for testing)",
    )
    args = parser.parse_args()

    run_counts = load_mgi_allele_reference_tags(input_file=args.input_file,
                                                file_url=args.file_url)
    report = compose_report_message(run_counts)
    deliver_report("MGI Allele-Reference Association Loading Report", report, args.no_email)
