"""
sgd_reference_tag_utils.py
==========================

Shared helpers and constants for the SGD reference-association entity-tag
scripts (``load_sgd_entity_reference_tags.py`` and
``update_sgd_entity_reference_tags.py``) (SCRUM-6404).

Both scripts create "pure entity" topic entity tags (topic == entity_type) for
the gene/allele/complex/pathway entities SGD displays on its reference pages,
from a single shared SGD reference-curation source, gated on SGD corpus
membership. The pieces that are identical between them -- the source, the
reference/corpus lookups, the SGD-curator-to-users.id resolution, the
already-loaded skip set, the create_tag loop, and report/log formatting --
live here so neither script imports from the other (same layout as
zfin_reference_tag_utils.py).
"""
import logging
from collections import defaultdict
from os import environ
from time import sleep
from typing import Dict, Iterable, List, Optional, Set, Tuple

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag
from agr_literature_service.api.models import ModModel, TopicEntityTagSourceModel
from agr_literature_service.api.schemas.topic_entity_tag_schemas import (
    TopicEntityTagSchemaPost,
)
from agr_literature_service.lit_processing.utils.report_utils import send_report

logger = logging.getLogger(__name__)
# Summary/report lines from this module must be visible when the scripts run
# with their default WARNING-level root logger.
logger.setLevel(logging.INFO)

# Curie prefix shared by SGD reference and entity identifiers in the ABC.
SGD_CURIE_PREFIX = "SGD"

SACCHAROMYCES_CEREVISIAE_TAXON = "NCBITaxon:559292"

# ATP:0000334 = "existing data"; used for every pure entity tag (topic == entity_type).
EXISTING_DATA_NOVELTY_ATP = "ATP:0000334"

# entity_id_validation "alliance" resolves SGD entity curies to names via the
# Alliance persistent store (with an SGD-API fallback for SGD: curies the
# store cannot resolve -- see fallback_id_to_name_mapping in
# topic_entity_tag_utils.py, which covers complexes and pathways).
ENTITY_ID_VALIDATION = "alliance"

# The entity types SGD displays on its reference pages, mapped to the ATP
# terms used for both the tag's topic and entity_type (same mapping as
# load_sgd_triage_data.py).
ENTITY_TYPE_TO_ATP = {
    "gene": "ATP:0000005",
    "allele": "ATP:0000006",
    "complex": "ATP:0000128",
    "pathway": "ATP:0000022",
}

# The shared SGD reference-curation source (SCRUM-6404). All four entity types
# use the same source row; they are told apart by entity_type.
# ATP:0000036 = assertion by professional curator.
SOURCE_EVIDENCE_ASSERTION = "ATP:0000036"
SOURCE_METHOD = "sgd_reference_curation"
SOURCE_DATA_PROVIDER = "SGD"
SECONDARY_DATA_PROVIDER_ABBR = "SGD"
SOURCE_DESCRIPTION = (
    "Manual association of entities with reference by SGD curators using the "
    "SGD curation interface."
)

# Emit a progress line every this many rows so a long run shows a heartbeat.
PROGRESS_LOG_INTERVAL = 1000

# Skip a paper's associations of a given entity type when it has more than
# this many in the input. Elasticsearch caps the reference document's
# `topic_entity_tags` nested field at 10000 sub-documents; bulk omics papers
# can carry thousands of gene associations and would halt the search reindex
# (same rationale and cap as the ZFIN loaders, SCRUM-6363).
MAX_ASSOCIATIONS_PER_PAPER = 250

# Abort a run if this many create_tag calls fail in a row (a sign the DB
# connection or session is wedged, rather than a few bad rows). Each
# consecutive error also waits ERROR_BACKOFF_SECONDS * n (capped at
# ERROR_BACKOFF_MAX_SECONDS) before the next attempt, so a transient network
# outage has ~10+ minutes to clear before the abort threshold is reached
# instead of burning through it in seconds.
ABORT_AFTER_CONSECUTIVE_ERRORS = 25
ERROR_BACKOFF_SECONDS = 5
ERROR_BACKOFF_MAX_SECONDS = 60

# Cap the number of "not in corpus" papers listed inline in the emailed report;
# the full set is always written to the log file.
NOT_IN_CORPUS_REPORT_CAP = 100

# The SGD curators who create reference-entity associations all have Stanford
# email addresses; requiring one keeps the loose first/last-name matching below
# from picking up a same-named person from another MOD.
SGD_CURATOR_EMAIL_DOMAIN = "stanford.edu"

# SGD NEX2 created_by values are curator database ids that match the curator's
# first name, last name, or Stanford email local-part -- forms the generic
# user_utils.map_to_user_id resolver does NOT handle (it only matches users.id,
# full email, or full-name/initials+last).
_SGD_CURATOR_MATCH_SQL = text(r"""
    WITH candidates AS (
        SELECT u.id AS users_id,
               regexp_split_to_array(trim(p.display_name), '\s+') AS toks,
               lower(split_part(e.email_address, '@', 1)) AS email_local,
               lower(split_part(e.email_address, '@', 2)) AS email_domain
        FROM   users u
        JOIN   person p ON u.person_id = p.person_id
        JOIN   person_email e ON e.person_id = p.person_id
    )
    SELECT DISTINCT users_id
    FROM   candidates
    WHERE  email_domain = :domain
    AND    (lower(toks[1]) = :sgd_id
            OR lower(toks[array_length(toks, 1)]) = :sgd_id
            OR email_local = :sgd_id)
""")


def resolve_sgd_created_by(db: Session, sgd_created_by: str,
                           cache: Dict[str, str]) -> Optional[str]:
    """Resolve an SGD NEX2 created_by database id to the users.id of the
    matching curator, memoizing per-id results in ``cache``.

    A match is a person with a @stanford.edu email whose first name, last name
    (first/last token of person.display_name), or email local-part equals the
    SGD id, case-insensitively. When there is no match (or several people
    match), the SGD id itself is returned and used verbatim as
    created_by/updated_by -- the audit layer then auto-creates an automation
    users row for it (ensure_user_exists_on_connection), per SCRUM-6404.
    Returns None for an empty id, leaving the tag to the script's global user.
    """
    key = (sgd_created_by or "").strip()
    if not key:
        return None
    if key in cache:
        return cache[key]
    rows = db.execute(_SGD_CURATOR_MATCH_SQL,
                      {"sgd_id": key.lower(), "domain": SGD_CURATOR_EMAIL_DOMAIN}).fetchall()
    users_ids = sorted({row[0] for row in rows})
    if len(users_ids) == 1:
        resolved = users_ids[0]
        logger.info(f"SGD created_by {key} resolved to users.id {resolved}")
    elif users_ids:
        resolved = key
        logger.warning(f"SGD created_by {key} matches multiple users {users_ids}; "
                       f"keeping {key!r} as created_by")
    else:
        resolved = key
        logger.info(f"SGD created_by {key} matches no Stanford person; "
                    "a users row will be auto-created for it")
    cache[key] = resolved
    return resolved


log_path = environ.get("LOG_PATH", "")


def _query_source(db: Session, mod_id: int) -> Optional[TopicEntityTagSourceModel]:
    return db.query(TopicEntityTagSourceModel).filter_by(
        source_evidence_assertion=SOURCE_EVIDENCE_ASSERTION,
        source_method=SOURCE_METHOD,
        data_provider=SOURCE_DATA_PROVIDER,
        secondary_data_provider_id=mod_id,
    ).one_or_none()


def get_or_create_source(db: Session) -> int:
    """Return the topic_entity_tag_source.id for the shared SGD reference-curation
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
    logger.info(f"Created SGD reference-curation TET source id={source.topic_entity_tag_source_id}")
    return source.topic_entity_tag_source_id


def build_sgd_ref_curie_map(db: Session) -> Dict[str, str]:
    """Map every non-obsolete SGD reference cross_reference curie to its
    reference curie, e.g. {"SGD:S000039113": "AGRKB:101000000..."}."""
    rows = db.execute(text(
        "SELECT cr.curie, r.curie "
        "FROM   cross_reference cr, reference r "
        "WHERE  cr.reference_id = r.reference_id "
        "AND    cr.curie_prefix = :prefix "
        "AND    cr.is_obsolete IS FALSE"
    ), {"prefix": SGD_CURIE_PREFIX}).fetchall()
    return {row[0]: row[1] for row in rows}


def build_sgd_corpus_ref_curies(db: Session) -> Set[str]:
    """Return the set of reference curies currently in the SGD corpus
    (mod_corpus_association.corpus IS TRUE for the SGD mod)."""
    rows = db.execute(text(
        "SELECT r.curie "
        "FROM   mod_corpus_association mca "
        "JOIN   reference r ON mca.reference_id = r.reference_id "
        "JOIN   mod m ON mca.mod_id = m.mod_id "
        "WHERE  m.abbreviation = :abbr "
        "AND    mca.corpus IS TRUE"
    ), {"abbr": SECONDARY_DATA_PROVIDER_ABBR}).fetchall()
    return {row[0] for row in rows}


def load_existing_entity_tags(db: Session, source_id: int) -> Set[Tuple[str, str, str]]:
    """Return the set of (reference_curie, entity_type_atp, entity) already tagged
    by this source as pure entity tags (topic == entity_type, one of the four SGD
    entity ATPs). Used to skip already-loaded associations up front so a re-run
    does not pay create_tag's per-row duplicate-check cost."""
    rows = db.execute(text(
        "SELECT r.curie, tet.entity_type, tet.entity "
        "FROM   topic_entity_tag tet "
        "JOIN   reference r ON tet.reference_id = r.reference_id "
        "WHERE  tet.topic_entity_tag_source_id = :sid "
        "AND    tet.topic = tet.entity_type "
        "AND    tet.entity_type = ANY(:atps)"
    ), {"sid": source_id, "atps": list(ENTITY_TYPE_TO_ATP.values())}).fetchall()
    return {(row[0], row[1], row[2]) for row in rows}


def select_over_cap_papers(entities_by_paper: Dict[Tuple[str, str], Set[str]]) -> Dict[Tuple[str, str], int]:
    """Given a mapping of (paper token, entity type) -> set of associated entity
    curies, return the (paper, type) pairs whose association count exceeds
    MAX_ASSOCIATIONS_PER_PAPER, mapped to that count. These associations are
    skipped so they never overflow the Elasticsearch nested-object limit on the
    reference document."""
    return {key: len(entities) for key, entities in entities_by_paper.items()
            if len(entities) > MAX_ASSOCIATIONS_PER_PAPER}


def build_tag_payload(reference_curie: str, entity_type_atp: str, entity_curie: str,
                      source_id: int,
                      created_by: Optional[str] = None) -> TopicEntityTagSchemaPost:
    """Build the pure-entity tag payload (topic == entity_type). ``created_by``
    (a users.id or verbatim SGD curator id from resolve_sgd_created_by) stamps
    both created_by and updated_by; None leaves both to the script's global
    automation user."""
    return TopicEntityTagSchemaPost(
        reference_curie=reference_curie,
        topic=entity_type_atp,
        entity_type=entity_type_atp,
        entity=entity_curie,
        entity_id_validation=ENTITY_ID_VALIDATION,
        species=SACCHAROMYCES_CEREVISIAE_TAXON,
        data_novelty=EXISTING_DATA_NOVELTY_ATP,
        negated=False,
        topic_entity_tag_source_id=source_id,
        created_by=created_by,
        updated_by=created_by,
    )


def create_entity_tags(db: Session,
                       associations: Iterable[Tuple[str, str, str, Optional[str]]],
                       source_id: int,
                       existing_tags: Set[Tuple[str, str, str]],
                       counts: Dict) -> None:
    """Create a pure entity tag for each (reference_curie, entity_type_atp,
    entity_curie, created_by) association, updating ``counts`` in place.
    ``created_by`` (see resolve_sgd_created_by; may be None) stamps the tag's
    created_by/updated_by and plays no part in duplicate detection. Associations
    already present in ``existing_tags`` (or repeated within ``associations``)
    are skipped; a 409 from create_tag also counts as a duplicate. Aborts
    (setting counts["aborted"]) after ABORT_AFTER_CONSECUTIVE_ERRORS
    consecutive errors."""
    seen: Set[Tuple[str, str, str]] = set()
    consecutive_errors = 0
    for reference_curie, entity_type_atp, entity_curie, created_by in associations:
        association = (reference_curie, entity_type_atp, entity_curie)
        if association in existing_tags:
            counts["skipped_duplicate"] += 1
            continue
        if association in seen:
            counts["duplicate_in_input"] += 1
            continue
        seen.add(association)

        try:
            _tag_id, was_upsert = create_tag(
                db, build_tag_payload(reference_curie, entity_type_atp, entity_curie,
                                      source_id, created_by),
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
            return
        if consecutive_errors:
            backoff = min(ERROR_BACKOFF_MAX_SECONDS,
                          ERROR_BACKOFF_SECONDS * consecutive_errors)
            logger.warning("Waiting %ds before next create_tag attempt "
                           "(%d consecutive errors)", backoff, consecutive_errors)
            sleep(backoff)


def new_counts() -> Dict:
    """The run-count dict shared by both SGD entity-tag scripts."""
    return {
        "total_associations": 0,
        "created": 0,
        "skipped_duplicate": 0,
        "duplicate_in_input": 0,
        "unknown_entity_type": 0,
        "missing_reference": 0,
        "not_in_corpus": 0,
        "skipped_over_cap": 0,
        "papers_over_cap": 0,
        "errors": 0,
    }


def new_entities_by_paper() -> Dict[Tuple[str, str], Set[str]]:
    return defaultdict(set)


def format_report_counts(counts: Dict, input_label: str) -> str:
    """Render the common report body (an HTML <ul>) from the run counts."""
    message = "<ul>"
    if counts.get("aborted"):
        message += "<li><b>RUN ABORTED early after consecutive create_tag errors</b>"
    message += f"<li>Total entity-reference associations in {input_label}: {counts['total_associations']}"
    message += f"<li>Entity tags created: {counts['created']}"
    message += f"<li>Already present (skipped): {counts['skipped_duplicate']}"
    message += f"<li>Duplicate associations within input: {counts['duplicate_in_input']}"
    message += f"<li>Unknown entity types skipped: {counts['unknown_entity_type']}"
    message += f"<li>References not found in ABC: {counts['missing_reference']}"
    message += f"<li>Associations skipped (paper not in SGD corpus): {counts['not_in_corpus']}"
    message += (f"<li>Associations skipped (&gt; {MAX_ASSOCIATIONS_PER_PAPER} per entity type per paper): "
                f"{counts['skipped_over_cap']} associations on {counts['papers_over_cap']} papers")
    message += f"<li>Errors: {counts['errors']}"
    not_in_corpus_refs = counts.get("not_in_corpus_refs", {})
    if not_in_corpus_refs:
        items = sorted(not_in_corpus_refs.items())
        message += f"<li>Papers not in SGD corpus ({len(items)}):<br>"
        for reference_curie, sgd_curie in items[:NOT_IN_CORPUS_REPORT_CAP]:
            message += f"{sgd_curie} ({reference_curie})<br>"
        if len(items) > NOT_IN_CORPUS_REPORT_CAP:
            message += f"...and {len(items) - NOT_IN_CORPUS_REPORT_CAP} more; full list in the log file<br>"
    message += "</ul>"
    return message


def log_run_summary(counts: Dict, label: str) -> None:
    logger.info(
        "%s done: total_associations=%d created=%d skipped_duplicate=%d "
        "duplicate_in_input=%d unknown_entity_type=%d missing_reference=%d "
        "not_in_corpus=%d skipped_over_cap=%d papers_over_cap=%d errors=%d",
        label, counts["total_associations"], counts["created"],
        counts["skipped_duplicate"], counts["duplicate_in_input"],
        counts["unknown_entity_type"], counts["missing_reference"],
        counts["not_in_corpus"], counts["skipped_over_cap"],
        counts["papers_over_cap"], counts["errors"],
    )


def write_id_log(filename: str, header: str, lines: List[str]) -> None:  # pragma: no cover
    """Write ``lines`` under ``header`` to LOG_PATH/filename (no-op if either the
    log path or the line list is empty)."""
    if not log_path or not lines:
        return
    with open(log_path + filename, "w") as fw:
        fw.write(f"{header}\n\n")
        for line in lines:
            fw.write(f"{line}\n")


def deliver_report(subject: str, message: str, no_email: bool) -> None:  # pragma: no cover
    """Email the report via report_utils.send_report, or just log it when
    ``no_email`` is set (send_report emails CRONTAB_EMAIL; there is no Slack)."""
    if no_email:
        logger.info("Report email disabled. Message content:")
        logger.info(message)
    else:
        send_report(subject, message)
