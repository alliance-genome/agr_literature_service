"""
mgi_reference_tag_utils.py
==========================

Shared helpers and constants for the MGI reference-association entity-tag
loader (``load_mgi_allele_reference_tags.py``) (SCRUM-6495).

Same layout as ``zfin_reference_tag_utils.py`` / ``sgd_reference_tag_utils.py``:
the MOD-specific pieces (source row, reference/corpus lookups, the A-team
bulk-load file discovery) live here, while the MOD-agnostic helpers
(over-cap selection, download, id-log writing, report delivery and the shared
tag constants) are imported from ``zfin_reference_tag_utils`` rather than
duplicated.

Unlike ZFIN (which publishes its allele file on zfin.org), MGI's allele data is
taken from the Alliance curation (A-team) bulk-load system: the latest "MGI
Allele Load" file md5 is looked up through the curation API and the file itself
is fetched from the public agr-curation-files S3 bucket (no auth needed for the
download; the API lookup uses the same Okta client-credentials token the
persistent-store client uses).
"""
import logging
from typing import Dict, Optional, Set

import requests
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ModModel, TopicEntityTagSourceModel

logger = logging.getLogger(__name__)

# ATP:0000006 = "allele". Pure entity tags: both topic and entity_type.
ALLELE_ATP = "ATP:0000006"

MUS_MUSCULUS_TAXON = "NCBITaxon:10090"

# Curie prefix shared by MGI reference (J:) and MGI entity identifiers in the
# ABC; reference resolution only consults reference-linked cross_references, so
# the shared prefix is not ambiguous here.
MGI_CURIE_PREFIX = "MGI"

# The shared MGI reference-curation source. ATP:0000036 = assertion by
# professional biocurator (every association in the file is created by
# MGI:curation_staff).
SOURCE_EVIDENCE_ASSERTION = "ATP:0000036"
SOURCE_METHOD = "mgi_reference_curation"
SOURCE_DATA_PROVIDER = "MGI"
SECONDARY_DATA_PROVIDER_ABBR = "MGI"
SOURCE_DESCRIPTION = (
    "Manual association of entities with references by MGI curators, taken from "
    "the Alliance curation (A-team) MGI bulk-load files."
)

# Prod curation API; the bulk-load registry only exists there. The files
# themselves are public S3 objects.
CURATION_API_BASE = "https://curation.alliancegenome.org/api"
CURATION_FILES_S3_BASE = "https://agr-curation-files.s3.amazonaws.com/prod"
MGI_ALLELE_BULK_LOAD_NAME = "MGI Allele Load"


def _query_source(db: Session, mod_id: int) -> Optional[TopicEntityTagSourceModel]:
    return db.query(TopicEntityTagSourceModel).filter_by(
        source_evidence_assertion=SOURCE_EVIDENCE_ASSERTION,
        source_method=SOURCE_METHOD,
        data_provider=SOURCE_DATA_PROVIDER,
        secondary_data_provider_id=mod_id,
    ).one_or_none()


def get_or_create_source(db: Session) -> int:
    """Return the topic_entity_tag_source.id for the shared MGI reference-curation
    source, creating it if absent. On a unique-constraint race (two first runs at
    once) the insert is rolled back and the now-present row is re-read."""
    mod = db.query(ModModel).filter_by(abbreviation=SECONDARY_DATA_PROVIDER_ABBR).one()
    existing = _query_source(db, mod.mod_id)
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
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = _query_source(db, mod.mod_id)
        if existing:
            return existing.topic_entity_tag_source_id
        raise
    db.refresh(source)
    logger.info(f"Created MGI reference-curation TET source id={source.topic_entity_tag_source_id}")
    return source.topic_entity_tag_source_id


def build_pub_to_ref_curie(db: Session) -> Dict[str, str]:
    """Map every non-obsolete reference-linked MGI and PMID cross_reference curie
    to its reference curie, e.g. {"MGI:6414854": "AGRKB:1010...",
    "PMID:17591625": "AGRKB:1010..."}.

    Both prefixes are loaded up front (the PMID map is ~1.5M entries, a few
    hundred MB) so the two passes over the ~800k-allele file resolve references
    with pure dict lookups: MGI lists most papers under BOTH curie forms, and the
    over-cap counting must group by RESOLVED reference, not raw token, or a paper
    split across its two curies could slip under the cap."""
    rows = db.execute(text(
        "SELECT cr.curie, r.curie "
        "FROM   cross_reference cr, reference r "
        "WHERE  cr.reference_id = r.reference_id "
        "AND    cr.curie_prefix IN ('MGI', 'PMID') "
        "AND    cr.is_obsolete IS FALSE"
    )).fetchall()
    return {row[0]: row[1] for row in rows}


def build_mgi_corpus_ref_curies(db: Session) -> Set[str]:
    """Return the set of reference curies currently in the MGI corpus
    (mod_corpus_association.corpus IS TRUE for the MGI mod)."""
    rows = db.execute(text(
        "SELECT r.curie "
        "FROM   mod_corpus_association mca "
        "JOIN   reference r ON mca.reference_id = r.reference_id "
        "JOIN   mod m ON mca.mod_id = m.mod_id "
        "WHERE  m.abbreviation = :abbr "
        "AND    mca.corpus IS TRUE"
    ), {"abbr": SECONDARY_DATA_PROVIDER_ABBR}).fetchall()
    return {row[0] for row in rows}


def _curation_api_headers() -> Dict[str, str]:
    # Same Okta client-credentials token the persistent-store client uses; import
    # deferred so tests and --input-file runs never touch the auth machinery.
    from agr_cognito_py import get_authentication_token
    return {
        "Authorization": f"Bearer {get_authentication_token()}",
        "Content-Type": "application/json",
    }


def find_latest_allele_file_url(timeout: int = 60) -> Optional[str]:  # pragma: no cover
    """Return the public S3 URL of the most recent "MGI Allele Load" bulk-load
    file, or None when the lookup fails (callers treat that as download failure).

    Two curation-API calls: find the load id by name in the bulk-load groups,
    then read its file history for the newest file's md5. The S3 key is the md5
    split into its first four hex chars: prod/6/8/9/3/6893...json.gz."""
    try:
        headers = _curation_api_headers()
        resp = requests.post(
            f"{CURATION_API_BASE}/bulkloadgroup/find?limit=100&page=0",
            json={}, headers=headers, timeout=timeout,
        )
        resp.raise_for_status()
        load_id = None
        for group in resp.json().get("results") or []:
            for load in group.get("loads") or []:
                if load.get("name") == MGI_ALLELE_BULK_LOAD_NAME:
                    load_id = load.get("id")
                    break
        if load_id is None:
            logger.error("Bulk load '%s' not found in the curation API",
                         MGI_ALLELE_BULK_LOAD_NAME)
            return None
        resp = requests.post(
            f"{CURATION_API_BASE}/bulkloadfilehistory/find?limit=200&page=0",
            json={"bulkLoad.id": load_id}, headers=headers, timeout=timeout,
        )
        resp.raise_for_status()
        runs = [r for r in (resp.json().get("results") or []) if r.get("bulkLoadFile")]
        runs.sort(key=lambda r: r.get("loadStarted") or "", reverse=True)
        if not runs:
            logger.error("Bulk load '%s' has no file history", MGI_ALLELE_BULK_LOAD_NAME)
            return None
        md5 = runs[0]["bulkLoadFile"]["md5Sum"]
        logger.info("Latest %s file: md5=%s loadStarted=%s",
                    MGI_ALLELE_BULK_LOAD_NAME, md5, runs[0].get("loadStarted"))
        return (f"{CURATION_FILES_S3_BASE}/{md5[0]}/{md5[1]}/{md5[2]}/{md5[3]}/"
                f"{md5}.json.gz")
    except Exception as e:
        logger.error("Failed to look up the latest MGI allele file: %s", e)
        return None


def format_not_in_corpus_section(not_in_corpus_refs: Dict[str, str],
                                 log_filename: str, report_cap: int) -> str:
    """Render the (capped) "papers not in MGI corpus" report section. The full
    set is always in ``log_filename``; only the first ``report_cap`` are listed
    inline so the email stays usable when the list is large."""
    if not not_in_corpus_refs:
        return ""
    items = sorted(not_in_corpus_refs.items())
    total = len(items)
    section = f"<li>Papers not in MGI corpus ({total}):<br>"
    for reference_curie, mgi_ref_token in items[:report_cap]:
        section += f"{mgi_ref_token} ({reference_curie})<br>"
    if total > report_cap:
        section += (f"...and {total - report_cap} more; "
                    f"full list in {log_filename}<br>")
    return section
