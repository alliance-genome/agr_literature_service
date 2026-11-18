import json
from datetime import datetime, timezone
from multiprocessing import Process, Value
from typing import List, Dict, Union, Any, Optional, Tuple

from agr_cognito_py import ModAccess, has_mod_access
from agr_cognito_py.cognito_permissions import ADMIN_GROUPS, GROUP_TO_ACCESS, MOD_ACCESS_ABBR
from fastapi import APIRouter, Body, Depends, Query, Response, Security, status, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import topic_entity_tag_crud, \
    topic_entity_tag_utils
from agr_literature_service.api.schemas import TopicEntityTagSchemaShow, TopicEntityTagSchemaPost
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaRelated, \
    TopicEntityTagSchemaUpdate
from agr_literature_service.api.crud.user_utils import resolve_named_person
from agr_literature_service.api.models import TagSourceModel, TopicEntityTagModel
from agr_literature_service.api.user import acting_as, set_global_user_from_cognito
from agr_literature_service.api.auth import get_authenticated_user, is_ip_bypass_user, \
    is_service_client_token, no_read_auth_bypass
from agr_literature_service.api.observer import service_client_mod
from agr_literature_service.api.util.resource_urls import topic_entity_tag_url

router = APIRouter(
    prefix="/topic_entity_tag",
    tags=['Topic Entity Tag']
)


get_db = database.get_db
db_session: Session = Depends(get_db)

revalidate_all_tags_already_running = Value('b', False)


def require_mod_write_access(user: Optional[Dict[str, Any]], mod_abbreviation: Optional[str]) -> None:
    """403 unless the caller may write tags of this MOD (SCRUM-6853).

    The check delete already made, applied to every tag write: a curator writes
    only the tags of a MOD whose curator group they are in (any of them, not only
    the first one get_mod_access looks at). Admins and developers write every MOD,
    and so do service accounts and the IP-bypass user (has_mod_access), except a
    service client that COGNITO_SERVICE_CLIENT_MODS limits to one MOD. No user at
    all fails closed. A None MOD (an unknown source or tag) is left to the write
    path's own 404.
    """
    if mod_abbreviation is None:
        return
    if user is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Authentication is required to write topic entity tags")
    if not _may_write_mod(user, mod_abbreviation):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail=f"You do not have permission to write {mod_abbreviation} topic entity tags")


def _may_write_mod(user: Dict[str, Any], mod_abbreviation: str) -> bool:
    if user.get("token_type") == "access":
        limited_to = service_client_mod(user)
        if limited_to is not None:
            return limited_to == mod_abbreviation
        return has_mod_access(user, mod_abbreviation)
    for group in user.get("cognito:groups") or []:
        if group in ADMIN_GROUPS or group.endswith("Developer"):
            return True
        access = GROUP_TO_ACCESS.get(group)
        if access is not None and MOD_ACCESS_ABBR.get(access) == mod_abbreviation:
            return True
    return False


def _source_mod(db: Session, tag_source_id: Optional[int]) -> Optional[str]:
    """The MOD a tag source belongs to, or None if there is no such source (the
    write path then answers its own 404)."""
    if tag_source_id is None:
        return None
    source = db.get(TagSourceModel, tag_source_id)
    return source.secondary_data_provider.abbreviation if source is not None else None


def resolve_person(db: Session, identifier: Optional[str], field: str) -> Optional[Tuple[int, str]]:
    """The person a request names in an audit field, as (agent.id, curie); None when
    none is named (SCRUM-6853). Resolved strictly, as updated_by is for indexing
    priority (SCRUM-6819), and only to a person: their AGRKB ID or email. 422 if it
    matches no one, more than one person, or an automation agent (default_user, a
    pipeline) whatever its label looks like, so a typo cannot record a tag under a
    made-up name. A person who has never logged in to ABC gets their agent created."""
    if identifier is None:
        return None
    return resolve_named_person(identifier, db, field)


def _looks_like_person(identifier: str) -> bool:
    return identifier.upper().startswith("AGRKB:") or "@" in identifier


def _naive_utc(value: Optional[datetime]) -> Optional[datetime]:
    """A request timestamp as the naive UTC the DateTime columns store; one with an
    offset is converted rather than having its offset dropped."""
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


@router.post('/',
             status_code=status.HTTP_201_CREATED,
             response_model=TopicEntityTagSchemaShow)
def create_tag(request: TopicEntityTagSchemaPost,
               response: Response,
               user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
               db: Session = db_session):
    set_global_user_from_cognito(db, user)
    require_mod_write_access(user, _source_mod(db, request.tag_source_id))
    # SCRUM-6853: created_by/updated_by naming a person (AGRKB ID or email) must
    # resolve to exactly one person. Any other label keeps the old behaviour, a new
    # automation agent if unknown, which the pipelines and loaders rely on.
    for field in ("created_by", "updated_by"):
        identifier = getattr(request, field)
        if identifier and _looks_like_person(identifier):
            person = resolve_person(db, identifier, field)
            if person is not None:
                setattr(request, field, person[1])
    new_tag_id, was_upsert = topic_entity_tag_crud.create_tag(db, request)
    # 200 when an existing tag absorbed the request (note appended in place),
    # 201 (the default declared above) when a new row was inserted.
    if was_upsert:
        response.status_code = status.HTTP_200_OK
    response.headers["Location"] = topic_entity_tag_url(new_tag_id)
    return topic_entity_tag_crud.show_tag(db, new_tag_id)


class ValidateTopicRequest(BaseModel):
    # Thin curator-validation request for the TET grid's Validation column: just
    # the cell the curator acted on. The server resolves the curator source and
    # all other tag fields (entity=null, data_novelty, etc.) so the UI no longer
    # has to. See topic_entity_tag_crud.validate_topic.
    reference_curie: str
    topic: str
    mod_abbreviation: str
    negated: bool = False
    note: Optional[str] = None
    species: Optional[str] = None
    # SCRUM-6853: the AuditedObject fields, for a service account only. A curator's
    # own login already identifies the curator, and the dates are when it happens.
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    date_created: Optional[datetime] = None
    date_updated: Optional[datetime] = None


AUDIT_FIELDS = ("created_by", "updated_by", "date_created", "date_updated")


@router.post('/validate', status_code=status.HTTP_200_OK)
def validate_topic(request: ValidateTopicRequest,
                   user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                   db: Session = db_session):
    """Create (or upsert) a curator topic-level validation and return the single
    recomputed grid cell ({topic, validation, filter_flags}) plus the tag id, so
    the grid updates that cell without re-fetching/re-aggregating the whole batch.
    The curator source is resolved (get-or-create) server-side from
    mod_abbreviation.

    A service account records the verdict for a curator with the AuditedObject
    fields (SCRUM-6853). Every service-account token is otherwise recorded as
    default_user, so all their verdicts would share one owner and replace each
    other's, so a service account must send at least one of created_by and
    updated_by (422 otherwise). created_by is whose verdict it is (the earlier
    verdict is looked up by it) and updated_by who made this change; one alone
    stands for both, as in the audit model. date_created applies when the verdict
    is created, date_updated when it is created or changed.

    A service account is a real client-credentials token (is_service_client_token).
    A request from a trusted IP with no token, which can already write any tag, may
    also send these fields, but need not: without them it is default_user, as
    before. A curator's own login cannot (403): it already records the curator."""
    set_global_user_from_cognito(db, user)
    require_mod_write_access(user, request.mod_abbreviation)
    service_account = is_service_client_token(user)
    sent = [field for field in AUDIT_FIELDS if getattr(request, field) is not None]
    if sent and not (service_account or is_ip_bypass_user(user)):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail=f"{', '.join(sent)} can only be set by a service account; a curator's "
                                   "own login already records the curator and the time")
    if service_account and request.created_by is None and request.updated_by is None:
        # A service token alone is recorded as default_user, so every such verdict
        # would share one owner and replace the others: refuse rather than store it.
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                            detail="A service account must name the curator with created_by or updated_by")
    creator = resolve_person(db, request.created_by, "created_by")
    updater = resolve_person(db, request.updated_by, "updated_by")
    creator, updater = creator or updater, updater or creator
    with acting_as(updater):
        return topic_entity_tag_crud.validate_topic(
            db,
            reference_curie=request.reference_curie,
            topic=request.topic,
            mod_abbreviation=request.mod_abbreviation,
            negated=request.negated,
            note=request.note,
            species=request.species,
            created_by=creator[1] if creator else None,
            date_created=_naive_utc(request.date_created),
            date_updated=_naive_utc(request.date_updated),
        )


@router.get('/{topic_entity_tag_id}',
            response_model=TopicEntityTagSchemaShow,
            status_code=200)
def show_tag(topic_entity_tag_id: int,
             user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
             db: Session = db_session):
    return topic_entity_tag_crud.show_tag(db, topic_entity_tag_id)


@router.patch('/{topic_entity_tag_id}',
              status_code=status.HTTP_200_OK,
              response_model=TopicEntityTagSchemaShow)
def patch_tag(topic_entity_tag_id: int,
              request: TopicEntityTagSchemaUpdate,
              user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
              db: Session = db_session):
    set_global_user_from_cognito(db, user)
    tag = db.get(TopicEntityTagModel, topic_entity_tag_id)
    if tag is not None:  # a missing tag gets patch_tag's 404
        # the tag's MOD; a PATCH cannot move a tag to another source
        require_mod_write_access(user, tag.tag_source.secondary_data_provider.abbreviation)
    topic_entity_tag_crud.patch_tag(db, topic_entity_tag_id, request)
    return topic_entity_tag_crud.show_tag(db, topic_entity_tag_id)


@router.delete('/{topic_entity_tag_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete_tag(topic_entity_tag_id,
               user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
               db: Session = db_session):
    set_global_user_from_cognito(db, user)
    tag = db.get(TopicEntityTagModel, topic_entity_tag_id)
    if tag is not None:  # a missing tag gets destroy_tag's 404
        require_mod_write_access(user, tag.tag_source.secondary_data_provider.abbreviation)
    # The MOD was checked above, across all of the caller's groups; destroy_tag's own
    # check only looks at the first group, so it would refuse a multi-MOD curator.
    topic_entity_tag_crud.destroy_tag(db, topic_entity_tag_id, ModAccess.ALL_ACCESS)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get('/by_reference/{curie_or_reference_id}',
            status_code=200)
def show_all_reference_tags(
    curie_or_reference_id: str,
    page: int = 1,
    page_size: int = None,
    column_only: str = None,
    column_filter: str = None,
    column_values: str = None,
    count_only: bool = False,
    sort_by: str = None,
    desc_sort: bool = False,
    column_filters: str = None,
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session
) -> Union[List[TopicEntityTagSchemaRelated], List[str], int]:
    # List[str] is the column_only path (distinct values of one column, for
    # the TET table's filter dropdowns): without it in the response union,
    # FastAPI rejected its own response as a 500 ResponseValidationError —
    # latent until the table's server-side filters started calling it
    # (SCRUM-6618).
    # Multi-column grid filters (SCRUM-6618): a JSON object mapping a column to
    # {"values": [...]} | {"contains": "text"} | {"range": [min, max]}, ANDed
    # across columns. Generalizes the single column_filter/column_values pair
    # for the TET table's server-side (infinite) row model.
    parsed_column_filters = None
    if column_filters:
        try:
            parsed_column_filters = json.loads(column_filters)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                                detail="column_filters must be a JSON object")
        if not isinstance(parsed_column_filters, dict):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                                detail="column_filters must be a JSON object")
    result = topic_entity_tag_crud.show_all_reference_tags(
        db, curie_or_reference_id,
        page, page_size,
        count_only, sort_by, desc_sort,
        column_only, column_filter,
        column_values,
        column_filters=parsed_column_filters
    )
    return result


# Cap the batch to bound worst-case latency / avoid gateway timeouts. The grid
# only ever sends one search page (max 100 results, chunked at 50 per request),
# so this is comfortable headroom above any legitimate request.
MAX_BATCH_REFERENCES = 100


class ReferenceTagsBatchFilters(BaseModel):
    """The TET facet criteria from the initial search. All fields optional; an
    omitted/empty field means "no restriction on that dimension", so the grid
    loads only the tags the search asked for.

    ``apply_to_single_tag`` mirrors the search's mode: when true (the default for
    an omitted value), the positive nested-TET facets (topic / confidence-level /
    source-method / source-evidence / data-novelty / data-context) must all be
    satisfied by ONE tag; when false the search matched them across different tags of a reference,
    so they are ORed instead (union of matching tags) to avoid an empty grid row
    for a genuine search hit."""
    topics: Optional[List[str]] = None
    confidence_levels: Optional[List[str]] = None
    negated_confidence_levels: Optional[List[str]] = None
    source_methods: Optional[List[str]] = None
    negated_source_methods: Optional[List[str]] = None
    source_evidence_assertions: Optional[List[str]] = None
    negated_source_evidence_assertions: Optional[List[str]] = None
    data_novelty: Optional[List[str]] = None
    data_context: Optional[List[str]] = None
    negated_data_context: Optional[List[str]] = None
    entity_types: Optional[List[str]] = None
    entities: Optional[List[str]] = None
    confidence_score_min: Optional[float] = None
    confidence_score_max: Optional[float] = None
    apply_to_single_tag: Optional[bool] = None
    # MOD scope from the search's corpus facet(s): restrict tags to those whose
    # source's secondary_data_provider (the owning MOD, per created_by_mod) is one
    # of these abbreviations, so the grid shows only the selected MOD's tags
    # instead of every MOD's tags on a shared reference. Omitted/empty => no MOD
    # restriction (all MODs' tags).
    mods: Optional[List[str]] = None


class ReferenceTagsBatchRequest(BaseModel):
    curies_or_reference_ids: List[str]
    filters: Optional[ReferenceTagsBatchFilters] = None


class ReferenceTagsBatchResponse(BaseModel):
    # tags: input identifier -> its (filtered) TET list.
    tags: Dict[str, List[TopicEntityTagSchemaRelated]]
    # counts: input identifier -> {topic_curie: {topic_only, entity_pos,
    # entity_neg, total, by_source: {label: {...}}}} -- aggregates computed in
    # the API so the grid doesn't recount client-side.
    counts: Dict[str, Any]
    # entries: input identifier -> {topic_curie: [aggregated mini-rows]}.
    # These rows are grouped by source/evidence/kind in the API so the grid
    # renders, filters and sorts from pre-aggregated tag buckets.
    entries: Optional[Dict[str, Any]] = None
    # validation: input identifier -> {topic_curie: {state: positive|negative|
    # conflict, positives, negatives, by_curator: [{name, negated, sources:
    # [{method, label}], species}]}}. Per-cell curator validation aggregated in
    # the API (mirrors ValidationCell) so the grid's Validation column sorts,
    # filters AND renders without deriving it from raw tags. Topics with no
    # curator validation are omitted (client treats absent as 'unvalidated').
    validation: Optional[Dict[str, Any]] = None
    # filter_flags: input identifier -> {topic_curie: {has_any, has_y, has_n,
    # has_note, my_validation_present}}. Per-cell booleans backing the grid's
    # per-topic cell filter (computed over all tags of the cell) so it filters
    # without scanning raw tags. my_validation_present compares each tag's
    # created_by against the authenticated user's users.id server-side (the
    # comparison the client couldn't do -- its uid is an Okta subject id and the
    # serialized created_by is a display name).
    filter_flags: Optional[Dict[str, Any]] = None
    # discovery: batch-global (NOT keyed per reference) {topics: [{curie, name}],
    # sources: [{label, method, secondary_data_provider, data_provider}]}. The
    # distinct topic columns (over all tags) and source labels (over non-curator
    # tags) present in the post-filter batch, so the grid builds its column set
    # and source filter without scanning raw tags. First-seen order.
    discovery: Optional[Dict[str, Any]] = None
    # debug_timing: per-phase timing breakdown, present only when
    # DEBUG_TET_BATCH_TIMING is enabled. Lets the slow-load breakdown be read
    # straight from the browser Network tab. Null in normal operation.
    debug_timing: Optional[Dict[str, Any]] = None


@router.post('/by_references',
             status_code=200)
def show_all_reference_tags_batch(
    request: ReferenceTagsBatchRequest = Body(...),
    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
    db: Session = db_session
) -> ReferenceTagsBatchResponse:
    """Return TETs for many references in one request, keyed by the input
    identifier, restricted to the tags the initial search specified (request.
    filters) and accompanied by per-topic entity-tag counts. Used by the TET
    validation grid to avoid one HTTP round-trip per reference and to avoid
    loading every tag on every reference (see
    show_all_reference_tags_for_references)."""
    if len(request.curies_or_reference_ids) > MAX_BATCH_REFERENCES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Too many references in one batch "
                   f"({len(request.curies_or_reference_ids)} > {MAX_BATCH_REFERENCES}); "
                   f"split into smaller requests."
        )
    filters = request.filters.model_dump(exclude_none=True) if request.filters else None
    return topic_entity_tag_crud.show_all_reference_tags_for_references(
        db, request.curies_or_reference_ids, filters
    )


@router.get('/entity_counts_by_mod/{curie_or_reference_id}',
            response_model=List[Dict[str, Any]],
            status_code=200)
def get_entity_counts_by_mod(curie_or_reference_id: str,
                             user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                             db: Session = db_session):
    """Distinct-entity counts per (owning MOD, entity type) for one reference
    (SCRUM-6620): one row per MOD x entity type with the resolved entity-type
    name. Server-side replacement for the Biblio EntityCountsByMod panel's
    client-side aggregation over the capped tag fetch."""
    return topic_entity_tag_crud.get_entity_counts_by_mod(db, curie_or_reference_id)


@router.get('/by_mod/{mod_abbreviation}',
            status_code=200)
def get_reference_tags(mod_abbreviation: str,
                       days_updated: int = 7,
                       user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                       db: Session = db_session):
    return topic_entity_tag_crud.get_all_topic_entity_tags_by_mod(db, mod_abbreviation, days_updated)


@router.get('/get_curie_to_name_from_all_tets/',
            response_model=Dict[str, str],
            status_code=200)
def get_curie_to_name_from_all_tets(curie_or_reference_id: str,
                                    user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                                    db: Session = db_session):
    return topic_entity_tag_crud.get_curie_to_name_from_all_tets(db, curie_or_reference_id)


def revalidate_tags_process_wrapper(already_running, email: str,
                                    delete_all_validation_values_first: bool,
                                    curie_or_reference_id: str,
                                    validation_values_only: bool):
    try:
        already_running.value = True
        topic_entity_tag_crud.revalidate_all_tags(
            email=email,
            delete_all_first=delete_all_validation_values_first,
            curie_or_reference_id=curie_or_reference_id,
            validation_values_only=validation_values_only
        )
    finally:
        already_running.value = False


@router.get('/revalidate_all_tags/',
            status_code=200)
@no_read_auth_bypass
def revalidate_all_tags(
        email: str = Query(
            default=None,
            description="Email address to notify when done. Revalidation runs in the background "
                        "and can take hours for all tags."
        ),
        delete_all_validation_values_first: bool = Query(
            default=False,
            description="Set True to clear all existing validation relationships before rebuilding. "
                        "Use when validation data is corrupted or after schema changes."
        ),
        curie_or_reference_id: str = Query(
            default=None,
            description="Limit revalidation to a single reference (e.g., 'AGRKB:101000000000001' or "
                        "'12345'). Use after manually editing tags on one paper."
        ),
        validation_values_only: bool = Query(
            default=False,
            description="Set True to only rebuild validation relationships without reprocessing "
                        "tag data. Faster option when tag data is correct but values are stale."
        ),
        user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
        db: Session = db_session):
    # user is guaranteed to be non-None: get_authenticated_user raises 401 on auth failure
    # The null check below is defensive but unreachable in practice
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")
    user_groups = user.get("cognito:groups", [])
    if not user_groups or "SuperAdmin" not in user_groups:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Only users in the 'SuperAdmin' group are allowed to perform this request.")
    set_global_user_from_cognito(db, user)
    global revalidate_all_tags_already_running
    if email is None:
        return {
            "message": "You need to provide an email address to revalidate all tags. You will receive an email at "
                       "the end of the validation process"
        }
    if revalidate_all_tags_already_running.value:
        return {
            "message": "Revalidation in progress, no need to submit the request again."
        }
    else:
        p = Process(target=revalidate_tags_process_wrapper,
                    args=(revalidate_all_tags_already_running, email, delete_all_validation_values_first,
                          curie_or_reference_id, validation_values_only))
        p.start()
        return {
            "message": "Revalidation of all tags started. You will receive an email when done."
        }


@router.delete('/delete_manual_tets/{reference_curie}/{mod_abbreviation}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete_manual_tags(reference_curie,
                       mod_abbreviation,
                       user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                       db: Session = db_session):
    set_global_user_from_cognito(db, user)
    require_mod_write_access(user, mod_abbreviation)
    topic_entity_tag_utils.delete_manual_tets(db, reference_curie, mod_abbreviation)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# Use :path so entity_list can include slashes.


@router.post('/set_no_tet_status/{mod_abbreviation}/{reference_curie}/{uid}',
             status_code=200)
def set_no_tet_status(mod_abbreviation: str,
                      reference_curie: str,
                      uid: str,
                      user: Optional[Dict[str, Any]] = Security(get_authenticated_user),
                      db: Session = db_session):
    set_global_user_from_cognito(db, user)
    require_mod_write_access(user, mod_abbreviation)
    return topic_entity_tag_crud.set_indexing_status_for_no_tet_data(db,
                                                                     mod_abbreviation,
                                                                     reference_curie,
                                                                     uid)
