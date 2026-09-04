"""
zfin_reference_tag_utils.py
===========================

Shared helpers and constants for the ZFIN reference-association entity-tag
loaders (``load_zfin_gene_reference_tags.py`` and
``load_zfin_allele_reference_tags.py``).

Both loaders create "pure entity" topic entity tags (topic == entity_type) from
a single shared ZFIN reference-curation source, gated on ZFIN corpus membership.
The pieces that are identical between them — the source, the reference/corpus
lookups, reference resolution, the already-loaded skip set, report/log
formatting, and the download — live here so neither loader imports from the
other.
"""
import logging
from collections import defaultdict
from os import environ
from typing import Dict, List, Optional, Set, Tuple

import requests
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.models import (
    ModModel,
    ReferenceModel,
    TopicEntityTagSourceModel,
)
from agr_literature_service.lit_processing.utils.db_read_utils import (
    get_reference_id_by_pmid,
)
from agr_literature_service.lit_processing.utils.report_utils import send_report

logger = logging.getLogger(__name__)

# Curie prefix shared by ZFIN publication, gene and allele identifiers in the ABC.
ZFIN_CURIE_PREFIX = "ZFIN"

# ATP:0000334 = "existing data"; used for every pure entity tag (topic == entity_type).
EXISTING_DATA_NOVELTY_ATP = "ATP:0000334"

# ATP:0000325 = "experimentally studied data" (SCRUM-5697). ZFIN's gene- and
# allele-publication associations record data the paper actually studied.
EXPERIMENTALLY_STUDIED_DATA_CONTEXT_ATP = "ATP:0000325"
DANIO_RERIO_TAXON = "NCBITaxon:7955"
# entity_id_validation "alliance" resolves ZFIN entity curies to names via the
# Alliance persistent store.
ENTITY_ID_VALIDATION = "alliance"

# The shared ZFIN reference-curation source (SCRUM-6362). Gene and allele tags
# use the same source row (its uniqueness key is identical); they are told apart
# by entity_type. ATP:0000036 = assertion by professional curator.
SOURCE_EVIDENCE_ASSERTION = "ATP:0000036"
SOURCE_METHOD = "zfin_reference_curation"
SOURCE_DATA_PROVIDER = "ZFIN"
SECONDARY_DATA_PROVIDER_ABBR = "ZFIN"
SOURCE_DESCRIPTION = (
    "Manual association of entities with reference by ZFIN curators using the "
    "ZFIN curation interface."
)

# Emit a progress line every this many rows so a long run shows a heartbeat.
PROGRESS_LOG_INTERVAL = 1000

# Skip a paper entirely when it has more than this many entity associations of a
# given type in the ZFIN file. Elasticsearch caps the reference document's
# `topic_entity_tags` nested field at 10000 sub-documents
# (index.mapping.nested_objects.limit); a handful of bulk ZFIN papers carried
# 10k-50k gene/allele tags and halted the search reindex. This per-type cap keeps
# ZFIN's contribution well under that ceiling (SCRUM-6363). Note the ES limit is
# on the reference's *total* nested tags across all sources, so this per-type,
# per-source cap bounds ZFIN's share rather than the document as a whole.
MAX_ASSOCIATIONS_PER_PAPER = 250

# Cap the number of "not in corpus" papers listed inline in the emailed report;
# the full set is always written to the log file.
NOT_IN_CORPUS_REPORT_CAP = 100

log_path = environ.get("LOG_PATH", "")


def select_over_cap_papers(entities_by_paper: Dict[str, Set[str]]) -> Dict[str, int]:
    """Given a mapping of paper token -> set of associated entity curies, return
    the papers whose association count exceeds MAX_ASSOCIATIONS_PER_PAPER, mapped
    to that count. These papers are skipped so they never overflow the
    Elasticsearch nested-object limit on the reference document."""
    return {token: len(entities) for token, entities in entities_by_paper.items()
            if len(entities) > MAX_ASSOCIATIONS_PER_PAPER}


def download_file(url: str, file_with_path: str, timeout: int = 300) -> bool:  # pragma: no cover
    """Stream ``url`` to ``file_with_path``. Returns True on success."""
    logger.info(f"Downloading {url}")
    try:
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()
        with open(file_with_path, "wb") as outfile:
            for chunk in response.iter_content(chunk_size=8192):
                outfile.write(chunk)
        logger.info(f"Downloaded to {file_with_path}")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to download {url}: {e}")
        return False


def _query_source(db: Session, mod_id: int) -> Optional[TopicEntityTagSourceModel]:
    return db.query(TopicEntityTagSourceModel).filter_by(
        source_evidence_assertion=SOURCE_EVIDENCE_ASSERTION,
        source_method=SOURCE_METHOD,
        data_provider=SOURCE_DATA_PROVIDER,
        secondary_data_provider_id=mod_id,
    ).one_or_none()


def find_zfin_source_id(db: Session) -> Optional[int]:
    """Return the topic_entity_tag_source.id for the shared ZFIN reference-curation
    source, or None if it (or the ZFIN mod) does not exist. Read-only counterpart
    to get_or_create_source, for tools that must not create the source."""
    mod = db.query(ModModel).filter_by(abbreviation=SECONDARY_DATA_PROVIDER_ABBR).one_or_none()
    if mod is None:
        return None
    source = _query_source(db, mod.mod_id)
    return source.topic_entity_tag_source_id if source else None


def get_or_create_source(db: Session) -> int:
    """Return the topic_entity_tag_source.id for the shared ZFIN reference-curation
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
    logger.info(f"Created ZFIN reference-curation TET source id={source.topic_entity_tag_source_id}")
    return source.topic_entity_tag_source_id


def build_zfin_pub_to_ref_curie(db: Session) -> Dict[str, str]:
    """Map every non-obsolete ZFIN publication cross_reference curie to its
    reference curie, e.g. {"ZFIN:ZDB-PUB-070425-4": "AGRKB:101000000..."}."""
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


def load_existing_entity_pairs(db: Session, source_id: int,
                               entity_atp: str) -> Set[Tuple[str, str]]:
    """Return the set of (reference_curie, entity) already tagged by this source
    as pure entity tags (topic == entity_type == entity_atp). Used to skip
    already-loaded pairs up front so a weekly re-run does not pay create_tag's
    per-row duplicate-check cost. Both topic and entity_type are constrained so
    the skip set is exactly the rows this loader owns -- a hypothetical mixed tag
    (topic != entity_type) on this source would not suppress a pure entity tag."""
    rows = db.execute(text(
        "SELECT r.curie, tet.entity "
        "FROM   topic_entity_tag tet "
        "JOIN   reference r ON tet.reference_id = r.reference_id "
        "WHERE  tet.topic_entity_tag_source_id = :sid "
        "AND    tet.topic = :atp "
        "AND    tet.entity_type = :atp"
    ), {"sid": source_id, "atp": entity_atp}).fetchall()
    return {(row[0], row[1]) for row in rows}


def resolve_reference_curie(db: Session, ref_token: Optional[str], pmid: Optional[str],
                            pub_to_ref_curie: Dict[str, str],
                            pmid_cache: Dict[str, Optional[str]],
                            unresolved_prefixes: Optional[Dict[str, int]] = None
                            ) -> Optional[str]:
    """Resolve a reference to its ABC reference curie.

    ``ref_token`` is a full curie (e.g. "ZFIN:ZDB-PUB-1" or "PMID:123"); ``pmid``
    is an optional bare PubMed id (the gene file carries it in a separate column).
    Tries the ZFIN publication map first, then a PubMed-id fallback (from ``pmid``
    or a "PMID:" ``ref_token``). Unresolved curie prefixes are tallied in
    ``unresolved_prefixes`` for reporting.
    """
    if ref_token:
        hit = pub_to_ref_curie.get(ref_token)
        if hit:
            return hit
        if not pmid and ref_token.startswith("PMID:"):
            pmid = ref_token.split(":", 1)[1]
    if pmid:
        if pmid not in pmid_cache:
            reference_id = get_reference_id_by_pmid(db, pmid)
            if reference_id is None:
                pmid_cache[pmid] = None
            else:
                reference = db.query(ReferenceModel).filter_by(reference_id=reference_id).one()
                pmid_cache[pmid] = reference.curie
        if pmid_cache[pmid]:
            return pmid_cache[pmid]
    if ref_token and unresolved_prefixes is not None:
        prefix = ref_token.split(":", 1)[0] if ":" in ref_token else ref_token
        unresolved_prefixes[prefix] = unresolved_prefixes.get(prefix, 0) + 1
    return None


def new_unresolved_prefix_counter() -> Dict[str, int]:
    return defaultdict(int)


def write_id_log(filename: str, header: str, lines: List[str]) -> None:  # pragma: no cover
    """Write ``lines`` under ``header`` to LOG_PATH/filename (no-op if either the
    log path or the line list is empty)."""
    if not log_path or not lines:
        return
    with open(log_path + filename, "w") as fw:
        fw.write(f"{header}\n\n")
        for line in lines:
            fw.write(f"{line}\n")


def format_not_in_corpus_section(not_in_corpus_refs: Dict[str, str],
                                 log_filename: str) -> str:
    """Render the (capped) "papers not in ZFIN corpus" report section. The full
    set is always in ``log_filename``; only the first NOT_IN_CORPUS_REPORT_CAP are
    listed inline so the email stays usable when the list is large."""
    if not not_in_corpus_refs:
        return ""
    items = sorted(not_in_corpus_refs.items())
    total = len(items)
    section = f"<li>Papers not in ZFIN corpus ({total}):<br>"
    for reference_curie, zfin_pub_curie in items[:NOT_IN_CORPUS_REPORT_CAP]:
        section += f"{zfin_pub_curie} ({reference_curie})<br>"
    if total > NOT_IN_CORPUS_REPORT_CAP:
        section += (f"...and {total - NOT_IN_CORPUS_REPORT_CAP} more; "
                    f"full list in {log_filename}<br>")
    return section


def deliver_report(subject: str, message: str, no_email: bool) -> None:  # pragma: no cover
    """Email the report via report_utils.send_report, or just log it when
    ``no_email`` is set (send_report emails CRONTAB_EMAIL; there is no Slack)."""
    if no_email:
        logger.info("Report email disabled. Message content:")
        logger.info(message)
    else:
        send_report(subject, message)
