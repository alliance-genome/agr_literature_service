"""
sgd_reference_tag_utils.py
==========================

Shared helpers and constants for the SGD reference-association entity-tag
scripts (``load_sgd_entity_reference_tags.py`` and
``update_sgd_entity_reference_tags.py``) (SCRUM-6404).

Both scripts create "pure entity" topic entity tags (topic == entity_type) for
the gene/allele/complex/pathway entities SGD displays on its reference pages,
plus "topic-only" tags (topic == the root topic ATP, no entity) for the
literature annotations SGD curates without an entity -- e.g. review papers and
omics (HTP) papers tagged with just a literature topic -- from a single shared
SGD reference-curation source, gated on SGD corpus membership. The pieces that
are identical between them -- the source, the reference/corpus lookups, the
SGD-curator-to-users.id resolution, the already-loaded skip set, the
create_tag loop, and report/log formatting -- live here so neither script
imports from the other (same layout as zfin_reference_tag_utils.py).
"""
import logging
from collections import defaultdict
from datetime import datetime
from os import environ
from time import sleep
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pytz
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.topic_entity_tag_crud import create_tag
from agr_literature_service.api.crud.topic_entity_tag_utils import (
    root_topic_atp,
    sgd_additional_display_tag,
    sgd_omics_display_tag,
    sgd_primary_display_tag,
    sgd_review_display_tag,
)
from agr_literature_service.api.models import (
    ModModel,
    TopicEntityTagModel,
    TopicEntityTagSourceModel,
)
from agr_literature_service.api.models.audited_model import (
    disable_set_updated_by_onupdate,
    enable_set_updated_by_onupdate,
)
from agr_literature_service.api.user import add_user_if_not_exists
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

# ATP:0000335; what create_tag's SGD branch stores as data_novelty for every
# tag whose topic differs from its entity_type -- including the topic-only
# tags below -- matching the tags SGD curators create in the ABC interface.
NEW_DATA_NOVELTY_ATP = "ATP:0000335"

# ATP:0000002, the root of the topic branch. SGD literature annotations
# without an entity (topic-only rows: e.g. review papers and omics/HTP papers)
# become tags with this topic, no entity/entity_type, and a display_tag mapped
# from the SGD literature topic (see SGD_TOPIC_TO_DISPLAY_TAG) -- the same
# shape the ABC curation interface stores for SGD when a curator picks a topic
# that equals its display tag (check_and_set_sgd_display_tag generalizes the
# topic to this root; e.g. the existing entity-less review tags are
# topic ATP:0000002 + display_tag ATP:0000130).
ROOT_TOPIC_ATP = root_topic_atp

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

# The SGD literature topic each association is annotated under (the section of
# the SGD reference page it appears in), mapped to the ABC display_tag ATP.
# Applied to all four entity types: create_tag's SGD branch
# (check_and_set_sgd_display_tag) keeps an explicitly supplied display_tag on
# pure-entity tags (topic == entity_type), and only when none is supplied
# (e.g. rows from a pre-topic dump) falls back to stamping one from the topic
# ATP (complex -> primary, allele/pathway -> additional, gene none). Also the
# display_tag of the topic-only tags (see ROOT_TOPIC_ATP), where it is what
# identifies the tag.
SGD_TOPIC_TO_DISPLAY_TAG = {
    "Primary Literature": sgd_primary_display_tag,        # ATP:0000147
    "Reviews": sgd_review_display_tag,                    # ATP:0000130
    "Omics": sgd_omics_display_tag,                       # ATP:0000148
    "Additional Literature": sgd_additional_display_tag,  # ATP:0000132
}

# The shared SGD reference-curation source (SCRUM-6404). All four entity types
# use the same source row; they are told apart by entity_type.
# ATP:0000036 = assertion by professional curator.
SOURCE_EVIDENCE_ASSERTION = "ATP:0000036"
SOURCE_METHOD = "sgd_reference_curation"
# source_method of tags curated directly in the ABC curation interface. An
# association a curator already tagged there must not be duplicated under (or
# corrected from) the SGD reference-curation source -- the curator's tag wins
# (see load_abc_entity_tags / create_entity_tags).
ABC_SOURCE_METHOD = "abc_literature_system"
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


# SGD's NEX2 timestamps are the database's local Pacific time (Stanford);
# the ABC stores UTC and its UI converts back to the viewer's timezone, so
# an unconverted date renders a day early for a Pacific viewer.
SGD_TIMEZONE = pytz.timezone("America/Los_Angeles")


def parse_sgd_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an SGD NEX2 date_created string -- 'YYYY-MM-DD HH:MM:SS', or bare
    'YYYY-MM-DD' from an older dump -- as Pacific time (see SGD_TIMEZONE) and
    return it as a naive UTC datetime, comparable to the ABC's stored
    timestamps. None for an empty or unparseable value."""
    text_value = (value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            local = SGD_TIMEZONE.localize(datetime.strptime(text_value, fmt))
        except ValueError:
            continue
        return local.astimezone(pytz.utc).replace(tzinfo=None)
    return None


def sgd_display_tag(sgd_topic: Optional[str]) -> Optional[str]:
    """Return the display_tag ATP for an association annotated under the given
    SGD literature topic, for any entity type. None for an unknown/absent
    topic (e.g. rows from a pre-topic dump) -- create_tag then falls back to
    stamping complex/allele/pathway tags from their topic ATP, and leaves
    gene tags without one (see SGD_TOPIC_TO_DISPLAY_TAG)."""
    return SGD_TOPIC_TO_DISPLAY_TAG.get((sgd_topic or "").strip())


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


# What load_existing_entity_tags records per existing tag, for the in-place
# correction of display_tag/date_created/created_by on a re-run (see
# maybe_update_existing_tag): (topic_entity_tag_id, display_tag, date_created,
# created_by).
ExistingTagRow = Tuple[int, Optional[str], Optional[datetime], Optional[str]]


def load_existing_entity_tags(db: Session, source_id: int,
                              reference_curies: Optional[Iterable[str]] = None
                              ) -> Dict[Tuple[str, str, str], ExistingTagRow]:
    """Return the associations already tagged by this source, mapping each to
    the tag's (topic_entity_tag_id, display_tag, date_created, created_by):
    pure entity tags (topic == entity_type, one of the four SGD entity ATPs)
    under (reference_curie, entity_type_atp, entity), and topic-only tags
    (topic == the root topic ATP, no entity) under (reference_curie,
    ROOT_TOPIC_ATP, display_tag) -- the root topic ATP is not an entity-type
    ATP, so the two key shapes cannot collide. Read once up front so a re-run
    does not pay create_tag's per-row duplicate-check cost, and so
    maybe_update_existing_tag can correct display_tag / date_created /
    created_by in place without a delete-and-reload. ``reference_curies``
    limits the query to those references -- the incremental updater only
    touches the papers in its API window, so it must not pull (and hold in
    memory) the whole historical tag set the way the one-off full load has
    to."""
    sql = ("SELECT r.curie, tet.entity_type, tet.entity, "
           "       tet.topic_entity_tag_id, tet.display_tag, tet.date_created, tet.created_by "
           "FROM   topic_entity_tag tet "
           "JOIN   reference r ON tet.reference_id = r.reference_id "
           "WHERE  tet.topic_entity_tag_source_id = :sid "
           "AND    ((tet.topic = tet.entity_type AND tet.entity_type = ANY(:atps)) "
           "        OR (tet.topic = :root_topic AND tet.entity IS NULL "
           "            AND tet.display_tag IS NOT NULL))")
    params: Dict = {"sid": source_id, "atps": list(ENTITY_TYPE_TO_ATP.values()),
                    "root_topic": ROOT_TOPIC_ATP}
    if reference_curies is not None:
        sql += " AND r.curie = ANY(:ref_curies)"
        params["ref_curies"] = list(reference_curies)
    rows = db.execute(text(sql), params).fetchall()
    return {(row[0], row[1] or ROOT_TOPIC_ATP, row[2] or row[4]):
            (row[3], row[4], row[5], row[6]) for row in rows}


def load_abc_entity_tags(db: Session,
                         reference_curies: Optional[Iterable[str]] = None
                         ) -> Set[Tuple[str, str, str]]:
    """Return the associations already tagged for SGD directly in the ABC
    curation interface -- any tag from any of the SGD mod's
    abc_literature_system source rows whose species is S. cerevisiae -- as
    (reference_curie, entity_type_atp, entity) for tags on one of the four SGD
    entity ATPs, and (reference_curie, ROOT_TOPIC_ATP, display_tag) for
    entity-less tags (matching the key shape of load_existing_entity_tags). An
    entity tag matches on entity_type / species / entity regardless of its
    topic, so a curator's richer annotation of the same entity (e.g. a real
    topic instead of a pure entity tag) also suppresses the load; an
    entity-less tag matches on its display_tag regardless of its topic, so a
    curator's specific topic choice (e.g. non-phenotype HTP for an omics
    paper) suppresses the root-topic tag this load would create for the same
    reference-page section. create_entity_tags skips these associations
    entirely: they are neither duplicated under the SGD reference-curation
    source nor corrected in place (the ABC curator's tag wins over the SGD
    input). ``reference_curies`` limits the query to those references, as in
    load_existing_entity_tags."""
    sql = ("SELECT r.curie, tet.entity_type, tet.entity, tet.display_tag "
           "FROM   topic_entity_tag tet "
           "JOIN   reference r ON tet.reference_id = r.reference_id "
           "JOIN   topic_entity_tag_source tets "
           "       ON tet.topic_entity_tag_source_id = tets.topic_entity_tag_source_id "
           "JOIN   mod m ON tets.secondary_data_provider_id = m.mod_id "
           "WHERE  tets.source_method = :abc_method "
           "AND    m.abbreviation = :abbr "
           "AND    tet.species = :species "
           "AND    ((tet.entity_type = ANY(:atps) AND tet.entity IS NOT NULL) "
           "        OR (tet.entity IS NULL AND tet.display_tag IS NOT NULL))")
    params: Dict = {
        "abc_method": ABC_SOURCE_METHOD,
        "abbr": SECONDARY_DATA_PROVIDER_ABBR,
        "species": SACCHAROMYCES_CEREVISIAE_TAXON,
        "atps": list(ENTITY_TYPE_TO_ATP.values()),
    }
    if reference_curies is not None:
        sql += " AND r.curie = ANY(:ref_curies)"
        params["ref_curies"] = list(reference_curies)
    rows = db.execute(text(sql), params).fetchall()
    return {(row[0], row[1], row[2]) if row[2] is not None
            else (row[0], ROOT_TOPIC_ATP, row[3]) for row in rows}


def select_over_cap_papers(entities_by_paper: Dict[Tuple[str, str], Set[str]]) -> Dict[Tuple[str, str], int]:
    """Given a mapping of (paper token, entity type) -> set of associated entity
    curies, return the (paper, type) pairs whose association count exceeds
    MAX_ASSOCIATIONS_PER_PAPER, mapped to that count. These associations are
    skipped so they never overflow the Elasticsearch nested-object limit on the
    reference document."""
    return {key: len(entities) for key, entities in entities_by_paper.items()
            if len(entities) > MAX_ASSOCIATIONS_PER_PAPER}


def build_tag_payload(reference_curie: str, topic_atp: str, entity_curie: Optional[str],
                      source_id: int,
                      created_by: Optional[str] = None,
                      display_tag: Optional[str] = None,
                      date_created: Optional[datetime] = None) -> TopicEntityTagSchemaPost:
    """Build the tag payload for one association. With an ``entity_curie`` it
    is a pure-entity tag: ``topic_atp`` is one of the four SGD entity ATPs,
    used for both topic and entity_type. Without one (None) it is a topic-only
    tag: ``topic_atp`` is ROOT_TOPIC_ATP and entity_type /
    entity_id_validation stay unset (see ROOT_TOPIC_ATP for the shape).
    ``created_by`` (a users.id or verbatim SGD curator id from
    resolve_sgd_created_by) stamps both created_by and updated_by; None leaves
    both to the script's global automation user. ``display_tag`` is derived
    from the association's SGD literature topic (see sgd_display_tag);
    create_tag's SGD branch (check_and_set_sgd_display_tag) keeps it, and only
    when it is None falls back to stamping complex/allele/pathway tags from
    the topic ATP (ATP:0000147 primary for complex, ATP:0000132 additional for
    allele/pathway; gene none). Topic-only tags always carry one -- it is what
    distinguishes them. ``date_created`` (a UTC datetime from
    parse_sgd_datetime -- when the association was curated in SGD) is
    preserved as the tag's date_created, with the load time as date_updated --
    both must be set explicitly, since AuditedModel's before_insert would
    otherwise copy date_created onto date_updated. Without a date the audit layer stamps both with the load
    time. The same create_tag branch re-derives data_novelty: ATP:0000334 for
    topic == entity_type tags and ATP:0000335 otherwise (topic-only tags),
    matching the values set below."""
    date_updated = datetime.now(tz=pytz.timezone("UTC")) if date_created else None
    topic_only = entity_curie is None
    return TopicEntityTagSchemaPost(
        reference_curie=reference_curie,
        topic=topic_atp,
        entity_type=None if topic_only else topic_atp,
        entity=entity_curie,
        entity_id_validation=None if topic_only else ENTITY_ID_VALIDATION,
        species=SACCHAROMYCES_CEREVISIAE_TAXON,
        display_tag=display_tag,
        data_novelty=NEW_DATA_NOVELTY_ATP if topic_only else EXISTING_DATA_NOVELTY_ATP,
        negated=False,
        topic_entity_tag_source_id=source_id,
        created_by=created_by,
        updated_by=created_by,
        date_created=date_created or None,
        date_updated=date_updated,
    )


def maybe_update_existing_tag(db: Session, existing_row: ExistingTagRow,
                              display_tag: Optional[str],
                              date_created: Optional[datetime],
                              created_by: Optional[str]) -> bool:
    """Correct an already-loaded tag in place when the SGD input disagrees with
    it, so a re-run fixes loaded data without a delete-and-reload. Three fields
    are corrected, each only when the input carries a value (a None desired
    value never clears a stored one): display_tag (see sgd_display_tag),
    date_created (a UTC datetime from parse_sgd_datetime, compared exactly
    against the stored timestamp), and
    created_by (a users.id or verbatim SGD curator id from
    resolve_sgd_created_by; updated_by is corrected along with it, matching
    the load convention updated_by == created_by). Returns True when an update
    was made, False for the no-op case.

    Updates go through the ORM so sqlalchemy-continuum writes a version row.
    The audit auto-stamping is disabled for the flush -- re-setting updated_by
    to an unchanged value does not register as a change with the audit layer,
    whose before_update would then overwrite it with the script user -- so
    date_updated is set to the run time explicitly, and a users row is ensured
    for a corrected created_by (before_update's ensure is skipped too)."""
    tag_id, current_display_tag, current_date_created, current_created_by = existing_row
    new_display_tag = display_tag if display_tag and display_tag != current_display_tag else None
    new_date_created = None
    if date_created is not None and current_date_created != date_created:
        new_date_created = date_created
    new_created_by = created_by if created_by and created_by != current_created_by else None
    if new_display_tag is None and new_date_created is None and new_created_by is None:
        return False
    if new_created_by is not None:
        add_user_if_not_exists(db, new_created_by)
    tag = db.query(TopicEntityTagModel).filter_by(topic_entity_tag_id=tag_id).one()
    disable_set_updated_by_onupdate(tag)
    try:
        if new_display_tag is not None:
            tag.display_tag = new_display_tag
        if new_date_created is not None:
            tag.date_created = new_date_created
        if new_created_by is not None:
            tag.created_by = new_created_by
            tag.updated_by = new_created_by
        tag.date_updated = datetime.now(tz=pytz.timezone("UTC"))
        db.commit()
    finally:
        enable_set_updated_by_onupdate(tag)
    return True


def create_entity_tags(db: Session,
                       associations: Iterable[Tuple[str, str, Optional[str], Optional[str], Optional[str], Optional[datetime]]],
                       source_id: int,
                       existing_tags: Dict[Tuple[str, str, str], ExistingTagRow],
                       abc_tags: Set[Tuple[str, str, str]],
                       counts: Dict) -> None:
    """Create a tag for each (reference_curie, topic_atp, entity_curie,
    created_by, display_tag, date_created) association, updating ``counts`` in
    place. With an ``entity_curie`` the association is a pure entity tag
    (``topic_atp`` one of the four SGD entity ATPs); with None it is a
    topic-only tag (``topic_atp`` == ROOT_TOPIC_ATP), identified by its
    display_tag instead of an entity -- everywhere a key is needed (the
    input-dedupe set, ``existing_tags``, ``abc_tags``) both shapes share
    (reference_curie, topic_atp, entity-or-display_tag). ``created_by`` (see
    resolve_sgd_created_by; may be None) stamps the tag's
    created_by/updated_by; ``display_tag`` (see sgd_display_tag; may be None
    only for entity tags) is the display_tag ATP derived from the SGD
    literature topic; ``date_created`` (a UTC datetime from parse_sgd_datetime;
    may be None) is preserved as the tag's date_created (see
    build_tag_payload). An association already tagged in the ABC
    curation interface (present in ``abc_tags`` -- see load_abc_entity_tags)
    is skipped outright, counted as skipped_in_abc, even when a tag from this
    source also exists: the ABC curator's tag wins, and this script's
    duplicate of it is left untouched. Otherwise an association already
    present in ``existing_tags`` is corrected in place when its display_tag /
    date_created / created_by disagree with the input (counted as
    updated_existing -- see maybe_update_existing_tag; a topic-only tag's
    display_tag is part of its identity, so only date_created / created_by are
    corrected for those) and skipped otherwise;
    a 409 from create_tag
    also counts as a duplicate. Aborts (setting counts["aborted"]) after
    ABORT_AFTER_CONSECUTIVE_ERRORS consecutive errors."""
    seen: Set[Tuple[str, str, str]] = set()
    consecutive_errors = 0
    for reference_curie, topic_atp, entity_curie, created_by, display_tag, date_created in associations:
        association = (reference_curie, topic_atp, entity_curie or display_tag or "")
        if association in seen:
            counts["duplicate_in_input"] += 1
            continue
        seen.add(association)

        if association in abc_tags:
            counts["skipped_in_abc"] += 1
            continue

        association_label = entity_curie or f"topic-only {display_tag}"
        try:
            if association in existing_tags:
                if maybe_update_existing_tag(db, existing_tags[association],
                                             display_tag if entity_curie else None,
                                             date_created, created_by):
                    counts["updated_existing"] += 1
                else:
                    counts["skipped_duplicate"] += 1
            else:
                _tag_id, was_upsert = create_tag(
                    db, build_tag_payload(reference_curie, topic_atp, entity_curie,
                                          source_id, created_by, display_tag, date_created),
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
                    f"TET create/update failed for {reference_curie} / {association_label}: {e.detail}"
                )
        except Exception as e:
            db.rollback()
            counts["errors"] += 1
            consecutive_errors += 1
            logger.warning(
                f"TET create/update failed for {reference_curie} / {association_label}: {e}"
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
        "updated_existing": 0,
        "skipped_duplicate": 0,
        "skipped_in_abc": 0,
        "duplicate_in_input": 0,
        "unknown_entity_type": 0,
        "unknown_topic": 0,
        "missing_entity_id": 0,
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
    message += (f"<li>Existing tags corrected in place (display_tag/date_created/created_by): "
                f"{counts['updated_existing']}")
    message += f"<li>Already present (skipped): {counts['skipped_duplicate']}"
    message += f"<li>Already tagged in the ABC curation interface (skipped): {counts['skipped_in_abc']}"
    message += f"<li>Duplicate associations within input: {counts['duplicate_in_input']}"
    message += f"<li>Unknown entity types skipped: {counts['unknown_entity_type']}"
    message += f"<li>Topic-only annotations with an unknown SGD topic skipped: {counts['unknown_topic']}"
    message += f"<li>Associations without an entity sgdid skipped: {counts['missing_entity_id']}"
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
        "%s done: total_associations=%d created=%d updated_existing=%d "
        "skipped_duplicate=%d skipped_in_abc=%d duplicate_in_input=%d "
        "unknown_entity_type=%d unknown_topic=%d "
        "missing_entity_id=%d missing_reference=%d not_in_corpus=%d "
        "skipped_over_cap=%d papers_over_cap=%d errors=%d",
        label, counts["total_associations"], counts["created"],
        counts["updated_existing"],
        counts["skipped_duplicate"], counts["skipped_in_abc"],
        counts["duplicate_in_input"],
        counts["unknown_entity_type"], counts["unknown_topic"],
        counts["missing_entity_id"],
        counts["missing_reference"], counts["not_in_corpus"],
        counts["skipped_over_cap"], counts["papers_over_cap"], counts["errors"],
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
