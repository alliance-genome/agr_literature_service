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
becomes a "pure entity" gene tag (topic == entity_type == gene) from a single
ZFIN reference-curation source. This is intentionally a simple load: the created
and updated dates are just the load date, and if ZFIN's full data is ever loaded
into the Alliance these tags would be dropped and reloaded with real history.

Only references already in the ZFIN corpus are tagged; associations whose paper is
not in the ZFIN corpus (or not in the ABC at all) are skipped and listed in the
report so a curator can follow up.

Idempotent: ``create_tag`` skips tags that already exist, so this can be run for
the initial load and then re-run weekly against the same file to pick up only the
newly added associations.
"""
import argparse
import logging
from os import environ, makedirs, path
from typing import Dict, Iterator, Optional, Set, Tuple

import requests
from dotenv import load_dotenv
from fastapi import HTTPException
from sqlalchemy import text
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
from agr_literature_service.lit_processing.utils.report_utils import send_report
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session,
)

logging.basicConfig(format="%(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

ZFIN_GENE_PUBLICATION_URL = "https://zfin.org/downloads/gene_publication.txt"

# Curie prefix shared by ZFIN publication and gene identifiers in the ABC.
ZFIN_CURIE_PREFIX = "ZFIN"
ZFIN_GENE_ID_PREFIX = "ZDB-GENE"
ZFIN_PUB_ID_PREFIX = "ZDB-PUB"

# ATP:0000005 = "gene". Both the topic and the entity_type are gene, so each tag
# is a pure entity tag whose data_novelty is the "existing data" term.
GENE_ATP = "ATP:0000005"
EXISTING_DATA_NOVELTY_ATP = "ATP:0000334"
DANIO_RERIO_TAXON = "NCBITaxon:7955"
# entity_id_validation "alliance" resolves ZFIN gene curies to names via the
# Alliance persistent store (same as the SGD triage load).
ENTITY_ID_VALIDATION = "alliance"

# The new ZFIN reference-curation source (SCRUM-6362).
# ATP:0000036 = assertion by professional curator.
SOURCE_EVIDENCE_ASSERTION = "ATP:0000036"
SOURCE_METHOD = "zfin_reference_curation"
SOURCE_DATA_PROVIDER = "ZFIN"
SECONDARY_DATA_PROVIDER_ABBR = "ZFIN"
SOURCE_DESCRIPTION = (
    "Manual association of entities with reference by ZFIN curators using the "
    "ZFIN curation interface."
)

# Created/updated user for every tag in this load (ticket: "ZFIN:Curator").
CREATED_BY = "ZFIN:Curator"

base_path = environ.get("XML_PATH", "")
file_path = base_path + "zfin_data/"
log_path = environ.get("LOG_PATH", "")


def download_zfin_gene_publication(file_with_path: str) -> bool:  # pragma: no cover
    """Download the ZFIN gene_publication.txt file. Returns True on success."""
    logger.info(f"Downloading ZFIN gene_publication file from {ZFIN_GENE_PUBLICATION_URL}")
    try:
        response = requests.get(ZFIN_GENE_PUBLICATION_URL, timeout=300, stream=True)
        response.raise_for_status()
        with open(file_with_path, "wb") as outfile:
            for chunk in response.iter_content(chunk_size=8192):
                outfile.write(chunk)
        logger.info(f"Downloaded ZFIN gene_publication file to {file_with_path}")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to download ZFIN gene_publication file: {e}")
        return False


def parse_gene_publication(file_with_path: str) -> Iterator[Tuple[str, str, str]]:
    """Yield (gene_id, pub_id, pmid) for each valid data row.

    Header/comment rows are skipped by requiring the ZDB-GENE / ZDB-PUB prefixes.
    ``pmid`` is an empty string when column 5 is absent or blank.
    """
    with open(file_with_path) as f:
        for line in f:
            pieces = line.rstrip("\n").split("\t")
            if len(pieces) < 3:
                continue
            gene_id = pieces[1].strip()
            pub_id = pieces[2].strip()
            if not gene_id.startswith(ZFIN_GENE_ID_PREFIX):
                continue
            if not pub_id.startswith(ZFIN_PUB_ID_PREFIX):
                continue
            pmid = pieces[4].strip() if len(pieces) >= 5 else ""
            yield gene_id, pub_id, pmid


def build_zfin_pub_to_ref_curie(db: Session) -> Dict[str, str]:
    """Map every non-obsolete ZFIN publication cross_reference curie to its
    reference curie, e.g. {"ZFIN:ZDB-PUB-070425-4": "AGRKB:101000000..."}.
    """
    rows = db.execute(text(
        "SELECT cr.curie, r.curie "
        "FROM   cross_reference cr, reference r "
        "WHERE  cr.reference_id = r.reference_id "
        "AND    cr.curie_prefix = :prefix "
        "AND    cr.is_obsolete IS FALSE"
    ), {"prefix": ZFIN_CURIE_PREFIX}).fetchall()
    return {row[0]: row[1] for row in rows}


def build_zfin_corpus_ref_curies(db: Session) -> Set[str]:
    """Return the set of reference curies currently in the ZFIN corpus
    (mod_corpus_association.corpus IS TRUE for the ZFIN mod)."""
    rows = db.execute(text(
        "SELECT r.curie "
        "FROM   mod_corpus_association mca "
        "JOIN   reference r ON mca.reference_id = r.reference_id "
        "JOIN   mod m ON mca.mod_id = m.mod_id "
        "WHERE  m.abbreviation = :abbr "
        "AND    mca.corpus IS TRUE"
    ), {"abbr": SECONDARY_DATA_PROVIDER_ABBR}).fetchall()
    return {row[0] for row in rows}


def get_or_create_source(db: Session) -> int:
    """Return the topic_entity_tag_source.id for the ZFIN reference-curation
    source, creating it if it does not yet exist."""
    mod = db.query(ModModel).filter_by(abbreviation=SECONDARY_DATA_PROVIDER_ABBR).one()
    existing = db.query(TopicEntityTagSourceModel).filter_by(
        source_evidence_assertion=SOURCE_EVIDENCE_ASSERTION,
        source_method=SOURCE_METHOD,
        data_provider=SOURCE_DATA_PROVIDER,
        secondary_data_provider_id=mod.mod_id,
    ).one_or_none()
    if existing:
        return existing.topic_entity_tag_source_id
    source = TopicEntityTagSourceModel(
        source_evidence_assertion=SOURCE_EVIDENCE_ASSERTION,
        source_method=SOURCE_METHOD,
        data_provider=SOURCE_DATA_PROVIDER,
        secondary_data_provider_id=mod.mod_id,
        validation_type=None,
        description=SOURCE_DESCRIPTION,
    )
    db.add(source)
    db.commit()
    db.refresh(source)
    logger.info(f"Created ZFIN reference-curation TET source id={source.topic_entity_tag_source_id}")
    return source.topic_entity_tag_source_id


def resolve_reference_curie(db: Session, pub_id: str, pmid: str,
                            pub_to_ref_curie: Dict[str, str],
                            pmid_cache: Dict[str, Optional[str]]) -> Optional[str]:
    """Resolve a ZFIN publication id (with PMID fallback) to a reference curie."""
    ref_curie = pub_to_ref_curie.get(f"{ZFIN_CURIE_PREFIX}:{pub_id}")
    if ref_curie:
        return ref_curie
    if not pmid:
        return None
    if pmid not in pmid_cache:
        reference_id = get_reference_id_by_pmid(db, pmid)
        if reference_id is None:
            pmid_cache[pmid] = None
        else:
            reference = db.query(ReferenceModel).filter_by(reference_id=reference_id).one()
            pmid_cache[pmid] = reference.curie
    return pmid_cache[pmid]


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
        created_by=CREATED_BY,
        updated_by=CREATED_BY,
    )


def load_zfin_gene_reference_tags(input_file: Optional[str] = None) -> Dict:  # pragma: no cover
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
        "missing_reference": 0,
        "not_in_corpus": 0,
        "errors": 0,
    }
    missing_pub_ids: Set[str] = set()
    # reference_curie -> ZFIN publication id, for references found in the ABC but
    # not in the ZFIN corpus (skipped, reported for curator follow-up).
    not_in_corpus_refs: Dict[str, str] = {}

    if input_file:
        file_with_path = input_file
    else:
        makedirs(file_path, exist_ok=True)
        file_with_path = f"{file_path}gene_publication.txt"
        if not download_zfin_gene_publication(file_with_path):
            db.close()
            counts["download_failed"] = True
            return counts

    try:
        source_id = get_or_create_source(db)
        pub_to_ref_curie = build_zfin_pub_to_ref_curie(db)
        logger.info(f"Loaded {len(pub_to_ref_curie)} ZFIN publication cross_references")
        zfin_corpus_ref_curies = build_zfin_corpus_ref_curies(db)
        logger.info(f"Loaded {len(zfin_corpus_ref_curies)} references in the ZFIN corpus")

        pmid_cache: Dict[str, Optional[str]] = {}
        seen_pairs: Set[Tuple[str, str]] = set()

        for gene_id, pub_id, pmid in parse_gene_publication(file_with_path):
            counts["total_pairs"] += 1
            entity_curie = f"{ZFIN_CURIE_PREFIX}:{gene_id}"
            reference_curie = resolve_reference_curie(
                db, pub_id, pmid, pub_to_ref_curie, pmid_cache
            )
            if reference_curie is None:
                counts["missing_reference"] += 1
                missing_pub_ids.add(pub_id)
                continue

            if reference_curie not in zfin_corpus_ref_curies:
                counts["not_in_corpus"] += 1
                not_in_corpus_refs.setdefault(reference_curie, pub_id)
                continue

            pair = (reference_curie, entity_curie)
            if pair in seen_pairs:
                counts["duplicate_in_file"] += 1
                continue
            seen_pairs.add(pair)

            try:
                _tag_id, was_upsert = create_tag(
                    db, _build_tag_payload(reference_curie, entity_curie, source_id),
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
                        f"TET create failed for {reference_curie} / {entity_curie}: {e.detail}"
                    )
            except Exception as e:
                counts["errors"] += 1
                logger.warning(
                    f"TET create failed for {reference_curie} / {entity_curie}: {e}"
                )

        counts["not_in_corpus_refs"] = not_in_corpus_refs
        logger.info(
            "ZFIN gene-reference load done: total_pairs=%d created=%d "
            "skipped_duplicate=%d duplicate_in_file=%d missing_reference=%d "
            "not_in_corpus=%d errors=%d",
            counts["total_pairs"], counts["created"], counts["skipped_duplicate"],
            counts["duplicate_in_file"], counts["missing_reference"],
            counts["not_in_corpus"], counts["errors"],
        )
        write_missing_pub_ids_log(missing_pub_ids)
        write_not_in_corpus_log(not_in_corpus_refs)
        return counts
    finally:
        db.close()


def write_missing_pub_ids_log(missing_pub_ids: Set[str]) -> None:  # pragma: no cover
    """Write the ZFIN publication ids that could not be matched to a reference."""
    if not log_path or not missing_pub_ids:
        return
    logfile_name = "zfin_gene_reference_missing_pub_ids.log"
    with open(log_path + logfile_name, "w") as fw:
        fw.write(f"ZFIN publication IDs not found in ABC ({len(missing_pub_ids)}):\n\n")
        for pub_id in sorted(missing_pub_ids):
            fw.write(f"{ZFIN_CURIE_PREFIX}:{pub_id}\n")


def write_not_in_corpus_log(not_in_corpus_refs: Dict[str, str]) -> None:  # pragma: no cover
    """Write the references found in the ABC but not in the ZFIN corpus (skipped)."""
    if not log_path or not not_in_corpus_refs:
        return
    logfile_name = "zfin_gene_reference_not_in_corpus.log"
    with open(log_path + logfile_name, "w") as fw:
        fw.write(f"References not in the ZFIN corpus ({len(not_in_corpus_refs)}):\n\n")
        for reference_curie, pub_id in sorted(not_in_corpus_refs.items()):
            fw.write(f"{ZFIN_CURIE_PREFIX}:{pub_id}\t{reference_curie}\n")


def compose_report_message(counts: Dict) -> str:  # pragma: no cover
    """Compose the HTML Slack report message from the run counts."""
    message = "<b>ZFIN Gene-Reference Association Loading Report</b><p>"
    if counts.get("download_failed"):
        message += "<ul><li>Failed to download ZFIN gene_publication.txt</ul>"
        return message
    message += "<ul>"
    message += f"<li>Total gene-reference pairs in file: {counts['total_pairs']}"
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
        for reference_curie, pub_id in sorted(not_in_corpus_refs.items()):
            message += f"{ZFIN_CURIE_PREFIX}:{pub_id} ({reference_curie})<br>"

    message += "</ul>"
    return message


def send_slack_report(message: str):  # pragma: no cover
    """Send the report to Slack."""
    send_report("ZFIN Gene-Reference Association Loading Report", message)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Load ZFIN gene-reference associations as entity topic entity tags"
    )
    parser.add_argument(
        "-f", "--input-file",
        help="Path to a local gene_publication.txt (default: download from ZFIN)",
    )
    parser.add_argument(
        "-n", "--no-slack",
        action="store_true",
        help="Do not send the Slack report (for testing)",
    )
    args = parser.parse_args()

    run_counts = load_zfin_gene_reference_tags(input_file=args.input_file)
    report = compose_report_message(run_counts)

    if args.no_slack:
        logger.info("Slack report disabled. Message content:")
        logger.info(report)
    else:
        send_slack_report(report)
