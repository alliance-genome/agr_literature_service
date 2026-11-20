import logging
import threading

from agr_curation_api.models import OntologyTermResult
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi import HTTPException, status
from typing import Dict, List, Optional, Iterable, Set, Tuple, Union, cast
from sqlalchemy import text, bindparam
from agr_curation_api import AGRCurationAPIClient, AGRAPIError  # type: ignore

logger = logging.getLogger(__name__)

curie_prefix_list = ["FB", "MGI", "RGD", "SGD", "WB", "XenBase", "ZFIN"]
topic_category_atp = "ATP:0000002"

atp_to_name: Dict[str, str] = {}
name_to_atp: Dict[str, str] = {}
atp_to_parent: Dict[str, str] = {}
atp_to_children: Dict[str, List[str]] = {}

# SCRUM-6474: serialises rebuilds of the four maps above. _atp_cache_ready is False for
# the duration of a rebuild, which is what stops a concurrent caller reading a partial
# cache -- "are the maps non-empty?" is true for most of a rebuild and so cannot be used.
#
# _atp_cache_building_thread makes the lock's re-entrancy mean something. No callee of the
# rebuild re-enters _ensure_atp_loaded today, and the RLock alone would NOT make that safe
# if one were added: the same thread re-takes the lock, the readiness re-check still sees
# False, and it starts the rebuild again -- clearing the maps mid-build and recursing until
# RecursionError. Recording the building thread lets a re-entrant call return instead. It
# then reads a partially built cache, which is inherent to asking mid-rebuild, but it is a
# bounded wrong answer rather than an unbounded loop.
_atp_cache_lock = threading.RLock()
_atp_cache_ready = False
_atp_cache_building_thread: Optional[int] = None

# SCRUM-6681: the terms the cache was built down from. Climbing atp_to_parent to one of
# them gives a complete ancestor chain; a climb that stops anywhere else has only reached
# the edge of what is cached, and atp_get_all_ancestors asks the ontology client instead.
_atp_roots: Set[str] = set()
# SCRUM-6681: ancestors resolved through the ontology client, per curie, for the terms the
# cache could not climb to a root. Emptied whenever the cache is rebuilt or replaced.
_atp_ancestor_fallback: Dict[str, List[str]] = {}

_client: Optional[AGRCurationAPIClient] = None


def _get_client() -> AGRCurationAPIClient:
    global _client
    if _client is None:
        _client = AGRCurationAPIClient()
    return _client


def map_entity_to_curie(entity_type: str, entity_list: str, taxon: str) -> JSONResponse:
    """
    Map pipe-delimited entity identifiers to CURIEs using the shared client.
    Returns the same JSONResponse shape as before.
    """
    entity_type_lc = (entity_type or "").lower()
    if not entity_type_lc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Missing entity_type")

    name_list, curie_list = classify_entity_list(entity_list)
    cli = _get_client()

    try:
        data: List[Dict[str, object]] = []
        if curie_list:
            curie_rows = cli.map_entity_curies_to_info(entity_type=entity_type_lc, entity_curies=curie_list)
            data.extend(
                {"entity_curie": r["entity_curie"], "is_obsolete": r["is_obsolete"], "entity": r["entity"]}
                for r in curie_rows
            )
        if name_list:
            name_rows = cli.map_entity_names_to_curies(entity_type=entity_type_lc, entity_names=name_list, taxon=taxon)
            data.extend(
                {"entity_curie": r["entity_curie"], "is_obsolete": r["is_obsolete"], "entity": r["entity"]}
                for r in name_rows
            )
    except AGRAPIError as e:
        if "Unknown entity_type" in str(e):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                                detail=f"Unknown entity_type '{entity_type}'")
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Mapping failed: {e}")
    return JSONResponse(content=jsonable_encoder(data))


def classify_entity_list(entity_list: str) -> Tuple[List[str], List[str]]:
    """Split raw entity_list into separate lists for names and curies."""
    entity_name_list: List[str] = []
    entity_curie_list: List[str] = []
    if not entity_list:
        return entity_name_list, entity_curie_list

    for entity in entity_list.split("|"):
        e = (entity or "").strip()
        if not e:
            continue
        is_mod_curie = any(e.startswith(p + ":") for p in curie_prefix_list)
        if is_mod_curie:
            entity_curie_list.append(e.upper())
        else:
            entity_name_list.append(e.upper())
    return entity_name_list, entity_curie_list


def search_topic(topic: Optional[str] = None, mod_abbr: Optional[str] = None) -> JSONResponse:
    """Search ATP ontology via client (supports MOD subset filter)."""
    cli = _get_client()
    try:
        rows = cli.search_atp_topics(topic=topic, mod_abbr=mod_abbr, limit=10)
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"ATP topic search failed: {e}")

    data = [{"curie": r["curie"], "name": r["name"]} for r in (rows or [])]
    return JSONResponse(content=jsonable_encoder(data))


def search_topic_list(topic: Optional[str] = None, mod_abbr: Optional[str] = None, limit: int = 10) -> List[dict]:
    """Return [{'curie':..., 'name':...}, ...] instead of a JSONResponse (for internal callers)."""
    cli = _get_client()
    try:
        rows = cli.search_atp_topics(topic=topic, mod_abbr=mod_abbr, limit=limit) or []
        return [{"curie": r["curie"], "name": r["name"]} for r in rows]
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"ATP topic search failed: {e}")


def search_atp_descendants(ancestor_curie: str) -> JSONResponse:
    """Return descendants (curie, name) of an ATP node via client."""
    cli = _get_client()
    try:
        rows = cli.get_atp_descendants(ancestor_curie=ancestor_curie)
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"ATP descendants failed: {e}")
    data = [{"curie": r["curie"], "name": r["name"]} for r in (rows or [])]
    return JSONResponse(content=jsonable_encoder(data))


# Upper bound on curies per /ontology/term_details request. The Quick Topic
# Addition UI sends one MOD's topic list (tens of curies); the cap keeps a
# single request from fanning an arbitrary number of lookups to the A-team API.
MAX_TERM_DETAILS_CURIES = 500


def get_topic_term_details(curies: List[str]) -> JSONResponse:
    """Return name/definition/synonyms for a list of ontology term curies.

    Used by the Quick Topic Addition UI to show each topic's definition and
    synonyms alongside its name (SCRUM-6168). Returns a mapping keyed by curie;
    curies with no matching (non-obsolete) term are omitted.
    """
    if not curies:
        return JSONResponse(content={})
    if len(curies) > MAX_TERM_DETAILS_CURIES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"too many curies: {len(curies)} > {MAX_TERM_DETAILS_CURIES}",
        )
    cli = _get_client()
    try:
        terms = cli.get_ontology_terms(curies) or {}
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Ontology term lookup failed: {e}")
    data: Dict[str, dict] = {}
    for curie, term in terms.items():
        if term is None:
            continue
        data[curie] = {
            "curie": term.curie,
            "name": term.name,
            "definition": term.definition,
            "synonyms": term.synonyms or [],
        }
    return JSONResponse(content=jsonable_encoder(data))


def search_species(species: str) -> JSONResponse:
    """Search NCBITaxonTerm via client (by name or CURIE)."""
    cli = _get_client()
    try:
        rows = cli.search_species(species=species, limit=10)
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Species search failed: {e}")
    data = [{"curie": r["curie"], "name": r["name"]} for r in (rows or [])]
    return JSONResponse(content=jsonable_encoder(data))


def get_or_create_species(taxon_id: str) -> JSONResponse:
    """Get or create a species (NCBITaxonTerm) in the A-Team system.

    Returns the existing taxon if found, or auto-imports it from NCBI
    if it doesn't exist yet.

    Args:
        taxon_id: Numeric NCBI Taxon ID (e.g., '6239') or full CURIE
                  (e.g., 'NCBITaxon:6239')
    """
    cli = _get_client()
    try:
        term = cli.get_or_create_species(taxon_id)
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Get or create species failed: {e}")
    data = {"curie": term.curie, "name": term.name}
    return JSONResponse(content=jsonable_encoder(data))


def search_ancestors_or_descendants(ontology_node: str, ancestors_or_descendants: str) -> List[str]:
    """
    Return a list of ancestor or descendant curies for the given ontology node.

    Uses cached ATP ontology data when available (ATP: terms),
    otherwise calls client.search_ontology_ancestors_or_descendants().
    """
    # ATPs are cached locally, so skip client call if applicable
    if ontology_node.startswith("ATP:"):
        if ancestors_or_descendants == "descendants":
            # When include_names=False (default), returns List[str]
            return cast(List[str], atp_get_all_descendants(ontology_node))
        return atp_get_all_ancestors(ontology_node)

    # For non-ATP ontology nodes, use the client API
    cli = _get_client()
    direction = "descendants" if ancestors_or_descendants == "descendants" else "ancestors"
    try:
        return cli.search_ontology_ancestors_or_descendants(
            ontology_node=ontology_node,
            direction=direction
        )
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Ontology traversal failed: {e}")


# ---------- ATP name fetching helpers ----------

def _fetch_atp_names(missing_curies: List[str]) -> None:
    """
    Populate atp_to_name and name_to_atp for any uncached ATP curses by querying the client
    (if possible). Before the client provides a direct method, use this fallback approach.
    """
    if not missing_curies:
        return
    cli = _get_client()
    try:
        # If the client provides a helper, call it; otherwise silently skip.
        curie_to_name = cli.get_ontology_terms(missing_curies)
        ontology_term: OntologyTermResult
        for curie, ontology_term in (curie_to_name or {}).items():
            atp_to_name[curie] = ontology_term.name
            name_to_atp[ontology_term.name] = curie
    except Exception as e:
        logger.debug("ATP name fetching via client helper failed for %s: %s", missing_curies, e)


def _fetch_atp_names_for_curie_list(curies: List[str]) -> None:
    """
    Ensure atp_to_name is filled for given ATP curies by directly querying ontologyterm (ATPTerm).
    """
    missing = [c for c in curies if c and c not in atp_to_name]
    if not missing:
        return
    # Try through the client's DB session (if available)
    try:
        cli = _get_client()
        dbm = cli._get_db_methods()
        session = dbm._create_session()
    except Exception as e:
        logger.debug("ATP fetching: DB session unavailable: %s", e)
        return
    try:
        sql = text("""
            SELECT curie, name
            FROM ontologyterm
            WHERE ontologytermtype = 'ATPTerm'
              AND UPPER(curie) IN :curies
        """).bindparams(bindparam("curies", expanding=True))
        rows = session.execute(sql, {"curies": [c.upper() for c in missing]}).fetchall()
        for curie, name in rows:
            atp_to_name[curie] = name
            name_to_atp[name] = curie
    except Exception as e:
        logger.debug("ATP fetching query failed: %s", e)
    finally:
        session.close()


def map_curies_to_names(category: str, curies: Iterable[str]) -> Dict[str, str]:
    curie_list = [c for c in curies if c]
    if not curie_list:
        return {}
    cat_raw = (category or "").strip()
    cat_norm = cat_raw.replace(" ", "").lower()

    # Treat 'atpterm'/'atp' or a category that is an ATP CURIE as ATP
    if cat_norm in {"atpterm", "atp"} or cat_raw.upper().startswith("ATP:"):
        _ensure_atp_loaded()
        _fetch_atp_names_for_curie_list(curie_list)
        return {c: atp_to_name.get(c, c) for c in curie_list}

    # Non-ATP
    cli = _get_client()
    try:
        result = cli.map_curies_to_names(category=cat_raw, curies=curie_list)
        if result:
            return result
    except AGRAPIError as e:
        logger.debug("DB mapping failed/unavailable for category=%r: %s", cat_raw, e)

    # Fall back to identity mapping for unknown categories.
    return {c: c for c in curie_list}


def create_ateam_db_session():
    """
    provide this so lit-processing programs can still use this function.
    """
    try:
        cli = _get_client()
        dbm = cli._get_db_methods()
        return dbm._create_session()
    except Exception as e:
        raise RuntimeError("DB session unavailable via client") from e


def search_for_entity_curies(
    entity_type: str,
    entity_list: str,
    taxon: Optional[str] = None
) -> List[str]:
    """
    Returns a flat list of CURIE strings resolved from names and/or CURIEs.
    """
    entity_type_lc = (entity_type or "").lower()
    names, curies = classify_entity_list(entity_list)
    cli = _get_client()
    out: List[str] = []
    if curies:
        rows = cli.map_entity_curies_to_info(entity_type=entity_type_lc, entity_curies=curies) or []
        out.extend([r["entity_curie"] for r in rows])
    if names:
        rows = cli.map_entity_names_to_curies(entity_type=entity_type_lc, entity_names=names, taxon=taxon) or []
        out.extend([r["entity_curie"] for r in rows])
    return out


# -----------------------------
# Jobs/subset logic (uses client)
# -----------------------------

def get_workflow_tags_for_mod(name: str, mod_abbreviation: str) -> List[str]:
    """
    Expand a parent ATP into its descendants, filtered by MOD subset.

    - If name is an ATP:curie, treat it as the parent.
    - Else, look for "<name> needed" in ATP names.

    Returns [parent, ...descendants in the MOD's ontology subset...]
    """
    if not atp_to_parent or not atp_to_children:
        load_name_to_atp_and_relationships()

    if name.startswith("ATP:"):
        atp_parent_id = name
    else:
        needed = f"{name} needed"
        if needed not in name_to_atp:
            raise HTTPException(
                status_code=404,
                detail=f"Exception: Could not find '{needed}' in ATP ontology names"
            )
        atp_parent_id = name_to_atp[needed]

    # Compute full candidate set: parent + all descendants
    candidates = {atp_parent_id}
    stack = [atp_parent_id]
    while stack:
        cur = stack.pop()
        for ch in atp_get_children(cur):
            if ch not in candidates:
                candidates.add(ch)
                stack.append(ch)

    # Filter descendants by MOD subset (e.g., FB_tag) in A-Team ontology
    children = candidates - {atp_parent_id}
    cli = _get_client()
    try:
        allowed = set(cli.filter_atp_by_mod_subset(
            list(children), mod_abbreviation
        ))
    except AGRAPIError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to filter subset for {mod_abbreviation}: {e}"
        )

    results = [atp_parent_id]
    results.extend(c for c in children if c in allowed)
    return results


# -----------------------------
# ATP caching and traversal (client-powered)
# -----------------------------

def set_globals(atp_to_name_init, name_to_atp_init, atp_to_children_init, atp_to_parent_init):
    global atp_to_name, name_to_atp, atp_to_children, atp_to_parent, _atp_cache_ready
    global _atp_roots, _atp_ancestor_fallback
    atp_to_name = atp_to_name_init.copy()
    name_to_atp = name_to_atp_init.copy()
    atp_to_children = atp_to_children_init.copy()
    atp_to_parent = atp_to_parent_init.copy()
    # Injected wholesale, so the result is complete by construction, and its roots are
    # the terms that have children but no parent.
    _atp_roots = set(atp_to_children) - set(atp_to_parent)
    _atp_ancestor_fallback = {}
    _atp_cache_ready = True


def _ensure_atp_loaded():
    """Build the ATP cache once, and make concurrent callers wait rather than read it
    half-built.

    SCRUM-6474: the old check was "is any map empty?", which is only true before the
    rebuild's clear() and after it. In between, the maps are non-empty but PARTIAL, and
    every caller arriving in that window sailed past this guard and computed hierarchy
    sets from a half-built parent map -- writing the resulting verdict straight to
    topic_entity_tag_validation. Measured with 8 concurrent cold-start callers against a
    stubbed client: 6 of them started their own full rebuild (1838 A-Team round trips
    instead of ~305) and 2 came back with no ancestors at all.

    _atp_cache_ready is only True once a build has finished, so a caller arriving mid
    rebuild blocks on the lock instead, then re-checks and uses the finished cache. The
    emptiness test is kept as well, so set_globals({}, ...) still forces a reload.
    """
    if _atp_cache_ready and atp_to_name and atp_to_children and atp_to_parent:
        return
    with _atp_cache_lock:
        if _atp_cache_building_thread == threading.get_ident():
            # Re-entered from inside this thread's own rebuild. Returning hands back a
            # partially built cache, which is unavoidable when asking mid-build; starting
            # another rebuild here would clear the maps under the one in progress and
            # recurse without end. No callee does this today -- see the note by the lock.
            logger.warning("ATP cache re-entered during its own rebuild; "
                           "returning the partially built cache")
            return
        if not (_atp_cache_ready and atp_to_name and atp_to_children and atp_to_parent):
            load_name_to_atp_and_relationships()


def load_name_to_atp_and_relationships(start_terms: Optional[List[str]] = None):
    """
    Build ATP maps (name <-> curie, children, parent) by BFS traversal
    from root terms using _get_atp_children for consistent caching.
    """
    if start_terms is None:
        # ATP:0000002 topic/entity_type root (SCRUM-6474), ATP:0000323 data context
        # (SCRUM-5746), ATP:0000177 workflow process, ATP:0000335 data novelty.
        #
        # ATP:0000002 costs ~125 extra round trips to walk (one per node), but leaving it
        # out did not make topic ancestry cheap, it made it WRONG: see below. Validation
        # reads it on every rule, so it has to be here. Its container terms are excluded
        # from matching separately, in topic_hierarchy_with_self.
        #
        # A branch left out here is not cached. Until SCRUM-6681 atp_get_all_ancestors fell
        # back to the ontology client only when atp_to_parent was *entirely* empty, so in
        # any warm worker a missing branch resolved to however much of the chain happened
        # to be cached as a side effect of earlier descendant lookups -- silently, and
        # differently per worker. For data_context, whose terms are never walked as
        # descendants in normal operation, that meant no ancestors at all and a hierarchy
        # check that degraded to exact equality. A climb that does not end on one of these
        # start terms now goes to the client for that curie, which is correct but costs a
        # few round trips the first time each term is asked about (one for its ancestors,
        # one for the children of each ancestor not yet cached), so a branch that
        # validation reads belongs here.
        #
        # Order matters: frontier.pop() is LIFO and atp_to_parent uses setdefault
        # (first write wins), so the LAST entry here is walked FIRST and claims the
        # parent pointer for any term reachable from two roots. New branches go at the
        # front, leaving the pre-existing precedence between 335 and 177 untouched.
        start_terms = ['ATP:0000002', 'ATP:0000323', 'ATP:0000177', 'ATP:0000335']

    global _atp_cache_ready, _atp_cache_building_thread, _atp_roots
    # SCRUM-6474: the whole clear-and-refill runs under the lock, and _atp_cache_ready
    # stays False throughout, so a concurrent caller blocks in _ensure_atp_loaded rather
    # than reading the maps mid-flight. Adding ATP:0000002 took this window from ~180
    # round trips to ~305, which is what turned a theoretical race into a measured one.
    with _atp_cache_lock:
        _atp_cache_ready = False
        _atp_cache_building_thread = threading.get_ident()
        try:
            # Clear and (re)build
            atp_to_name.clear()
            name_to_atp.clear()
            atp_to_children.clear()
            atp_to_parent.clear()
            _atp_roots = set()
            _atp_ancestor_fallback.clear()

            # Fetch root names
            _fetch_atp_names(start_terms)

            # BFS traversal using shared helper
            frontier = list(start_terms)
            seen = set(frontier)

            while frontier:
                parent = frontier.pop()
                for child_curie in _get_atp_children(parent):
                    if child_curie not in seen:
                        seen.add(child_curie)
                        frontier.append(child_curie)
            _atp_roots = set(start_terms)
        except Exception:
            # Leave the maps empty rather than half-built: _ensure_atp_loaded's emptiness
            # test then forces a genuine retry on the next call instead of pinning a
            # partial hierarchy for the life of the worker.
            atp_to_name.clear()
            name_to_atp.clear()
            atp_to_children.clear()
            atp_to_parent.clear()
            raise
        finally:
            _atp_cache_building_thread = None
        _atp_cache_ready = True

    logger.debug("ATP global vars successfully loaded via client traversal")


def atp_get_parent(child_id: str):
    _ensure_atp_loaded()
    return atp_to_parent.get(child_id)


def atp_get_children(parent_id: str) -> List[str]:
    _ensure_atp_loaded()
    return atp_to_children.get(parent_id, [])


def atp_get_children_as_dict(parent_id: str) -> List[Dict[str, str]]:
    return [{"curie": cid, "name": atp_to_name.get(cid, cid)} for cid in atp_get_children(parent_id)]


def atp_to_name_subset(curies: List[str]) -> Dict[str, str]:
    _ensure_atp_loaded()
    missing = [c for c in curies if c not in atp_to_name]
    if missing:
        _fetch_atp_names(missing)
    return {c: atp_to_name.get(c, c) for c in curies}


def atp_get_all_descendants(curie: str, direct_children_only: bool = False, include_self: bool = False,
                            include_names: bool = False) -> Union[List[str], List[Dict[str, str]]]:
    """
    Return all descendant ATP curies for the given curie.
    """
    try:
        _, subset_atp_to_name = get_name_to_atp_for_descendants(curie, direct_children_only)
        if include_self:
            subset_atp_to_name[curie] = atp_get_name(curie)
        if include_names:
            return [{"curie": c, "name": n} for c, n in subset_atp_to_name.items()]
        else:
            return list(subset_atp_to_name.keys())
    except Exception as e:
        logger.warning("Failed to fetch ATP descendants for %s: %s", curie, e)
        result: List[str] = []
        return result


def _climb_cached_parents(curie: str, within: Optional[Set[str]] = None,
                          stop_at: Optional[Set[str]] = None) -> List[str]:
    """Follow atp_to_parent up from curie, nearest ancestor first, stopping at a term with
    no cached parent (or, with `within`, at a parent outside that set). With `stop_at`,
    the climb also ends on the first term in that set, which is included."""
    parent_list: List[str] = []
    seen = {curie}
    node = curie
    while not (stop_at and node in stop_at):
        parent = atp_to_parent.get(node)
        if not parent or parent in seen or (within is not None and parent not in within):
            break
        parent_list.append(parent)
        seen.add(parent)
        node = parent
    return parent_list


def _order_client_ancestors(curie: str, ancestors: Iterable[str]) -> List[str]:
    """Put the client's (unordered) ancestors of curie nearest first, as the cached climb
    does: callers such as workflow_tag_crud._resolve_process_hierarchy read the position.

    Fetching each ancestor's direct children fills the parent pointers along the chain
    (and caches them for later lookups), so the chain can then be climbed in order.
    Ancestors off that single-parent path (a term with two parents) follow, sorted.
    """
    ancestor_set = {a for a in ancestors if a != curie}
    for ancestor in sorted(ancestor_set):
        _get_atp_children(ancestor)
    chain = _climb_cached_parents(curie, within=ancestor_set)
    return chain + sorted(ancestor_set.difference(chain))


def atp_get_all_ancestors(curie: str) -> List[str]:
    """
    Return the ancestors of an ATP curie, nearest first.

    Climbs the cached parent pointers. When the climb does not end on a root the cache was
    built from, the term is outside the cached branches (or only partly cached, as a side
    effect of an earlier descendant lookup), so the ontology client is asked instead and
    its answer is cached for that curie.
    """
    # SCRUM-6474: build the cache first. Without this a cold cache fell through to the
    # client on every call AND never populated the maps, so the "fallback" silently became
    # the permanent path at ~520ms a call. The API hid it because create_tag calls
    # atp_return_invalid_ids (which does _ensure_atp_loaded) before any rule runs, but
    # revalidate_all_tags reaches get_ancestors first -- so the sweep paid it per tag.
    _ensure_atp_loaded()

    # Stop on the first root rather than only checking where the climb ends: a start term
    # has no parent from the build, but _get_atp_children's setdefault gives it one as soon
    # as anything fetches the children of the term above it (_order_client_ancestors does,
    # for every client answer). Without the stop, one fallback for any other child of
    # ATP:0000001 would make every topic climb end on 001, not on ATP:0000002, and send the
    # whole cached topic branch to the client.
    parent_list = _climb_cached_parents(curie, stop_at=_atp_roots)
    top = parent_list[-1] if parent_list else curie
    if top in _atp_roots:
        return parent_list

    # SCRUM-6681: the fallback used to run only when atp_to_parent was entirely empty, so
    # in a warm worker a term outside the cached branches got [] or a truncated chain and
    # every hierarchy check over it degraded to exact equality. It needs
    # agr-curation-api-client >= 0.15.2: earlier versions returned descendants here.
    cached = _atp_ancestor_fallback.get(curie)
    if cached is not None:
        return list(cached)
    try:
        ancestors = _get_client().search_ontology_ancestors_or_descendants(
            ontology_node=curie, direction="ancestors")
    except Exception as e:
        # Not cached, so the next call retries.
        logger.warning("ATP ancestor lookup for %s failed (%s); using the %d cached ancestors",
                       curie, e, len(parent_list))
        return parent_list
    if ancestors is None:
        logger.warning("ATP ancestor lookup for %s returned no payload; not caching", curie)
        return parent_list
    resolved = _order_client_ancestors(curie, ancestors)
    _atp_ancestor_fallback[curie] = resolved
    return list(resolved)


def atp_get_name(atp_id: str) -> Optional[str]:
    if not atp_to_name:
        try:
            load_name_to_atp_and_relationships()
        except HTTPException:
            return None
    if atp_id not in atp_to_name:
        _fetch_atp_names([atp_id])
    return atp_to_name.get(atp_id)


def atp_return_invalid_ids(atp_ids: List[str]) -> List[str]:
    """
    Validate ATP IDs using the ATP cache, fetching any missing ATP IDs first.
    This covers ATP IDs for entity_type == pathway/complex too.
    """
    _ensure_atp_loaded()
    need_fetching = [a for a in (atp_ids or []) if a and a not in atp_to_name]
    if need_fetching:
        _fetch_atp_names_for_curie_list(need_fetching)
    return [a for a in (atp_ids or []) if a and a not in atp_to_name]


def _get_atp_children(parent_curie: str) -> List[str]:
    """Fetch and cache direct children for an ATP term. Returns list of child curies."""
    if parent_curie in atp_to_children:
        return atp_to_children[parent_curie]
    try:
        children = _get_client().get_atp_descendants(
            ancestor_curie=parent_curie, direct_children_only=True
        )
    except AGRAPIError:
        return []

    if children is None:
        # SCRUM-6474: a null payload is a degraded response, not a leaf term, and the two
        # must not be cached alike. Caching it would pin "no children" for the life of the
        # worker -- _ensure_atp_loaded only rebuilds when the cache is empty or unbuilt, so
        # the other roots keep it looking healthy and the miss is never retried. If it hit
        # a branch root during the BFS the entire subtree would be absent, and every
        # hierarchy check over it would silently degrade to exact equality.
        logger.warning("ATP children lookup for %s returned no payload; not caching",
                       parent_curie)
        return []

    child_curies = []
    for child in children:
        curie, name = child["curie"], child["name"]
        atp_to_name[curie] = name
        name_to_atp[name] = curie
        atp_to_parent.setdefault(curie, parent_curie)
        child_curies.append(curie)

    # SCRUM-6474: cache the empty result too. Leaf terms have no children, so the old
    # `if child_curies:` guard never cached them -- and 144 of the 176 ATP terms are
    # leaves. Every get_descendants() on a leaf therefore re-hit the A-Team API, at
    # ~600ms a call, and validate_tags calls get_descendants several times per tag (topic,
    # data_novelty and entity_type, on both the negative and the new-tag rule paths).
    # Reaching here means the client answered, so an empty list really is a leaf and is
    # safe to cache; the AGRAPIError and null-payload branches above both return without
    # caching, so a genuine failure still retries.
    atp_to_children[parent_curie] = child_curies
    return child_curies


def get_name_to_atp_for_descendants(atp_curie: str, direct_children_only: bool = False):
    """
    Return ALL descendants for an ATP term as two dicts:
      subset_name_to_atp:  {<name>: <ATP:curie>, ...}
      subset_atp_to_name:  {<ATP:curie>: <name>, ...}
    This always returns a 2-tuple (possibly both empty dicts).
    """
    _ensure_atp_loaded()

    direct_children = _get_atp_children(atp_curie)
    if not direct_children:
        return {}, {}

    if direct_children_only:
        return (
            {atp_to_name[ch]: ch for ch in direct_children},
            {ch: atp_to_name[ch] for ch in direct_children}
        )

    # BFS traversal to collect all descendants
    subset_atp_to_name: Dict[str, str] = {}
    subset_name_to_atp: Dict[str, str] = {}
    frontier = list(direct_children)
    seen = set(direct_children)

    while frontier:
        current_curie = frontier.pop()
        current_name = atp_to_name.get(current_curie, current_curie)
        subset_atp_to_name[current_curie] = current_name
        subset_name_to_atp[current_name] = current_curie

        for child_curie in _get_atp_children(current_curie):
            if child_curie not in seen:
                seen.add(child_curie)
                frontier.append(child_curie)

    return subset_name_to_atp, subset_atp_to_name
