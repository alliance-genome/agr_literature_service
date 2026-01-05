import logging

from agr_curation_api.models import OntologyTermResult
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi import HTTPException, status
from typing import Dict, List, Optional, Iterable, Tuple, Union, cast
import cachetools.func
from sqlalchemy import text, bindparam
from agr_curation_api import AGRCurationAPIClient, AGRAPIError  # type: ignore
from sqlalchemy.orm import Session

from agr_literature_service.api.models import WorkflowTagTopicModel

logger = logging.getLogger(__name__)

curie_prefix_list = ["FB", "MGI", "RGD", "SGD", "WB", "XenBase", "ZFIN"]
topic_category_atp = "ATP:0000002"

atp_to_name: Dict[str, str] = {}
name_to_atp: Dict[str, str] = {}
atp_to_parent: Dict[str, str] = {}
atp_to_children: Dict[str, List[str]] = {}

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
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Missing entity_type")

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
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
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


def search_species(species: str) -> JSONResponse:
    """Search NCBITaxonTerm via client (by name or CURIE)."""
    cli = _get_client()
    try:
        rows = cli.search_species(species=species, limit=10)
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Species search failed: {e}")
    data = [{"curie": r["curie"], "name": r["name"]} for r in (rows or [])]
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
        curie_to_name = cli.get_ontology_terms(missing_curies)  # type: ignore[attr-defined]
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

def get_jobs_to_run(name: str, mod_abbreviation: str, db: Session) -> List[str]:
    """
    Use ATP children + subset filter from search_atp_topics(mod_abbr) to find jobs.
    - If name is an ATP:curie, treat it as the parent.
    - Else, look for "<name> needed" in ATP names.
    Returns [parent, ...allowed child curies in subset...]
    """
    if not atp_to_parent or not atp_to_children:
        load_name_to_atp_and_relationships()

    if name.startswith("ATP:"):
        atp_parent_id = name
    else:
        needed = f"{name} needed"
        if needed not in name_to_atp:
            raise HTTPException(status_code=404, detail=f"Exception: Could not find '{needed}' in ATP ontology names")
        atp_parent_id = name_to_atp[needed]

    # Compute full candidate set: include parent + all descendants
    candidates = {atp_parent_id}
    stack = [atp_parent_id]
    while stack:
        cur = stack.pop()
        for ch in atp_get_children(cur):
            if ch not in candidates:
                candidates.add(ch)
                stack.append(ch)

    # Filter by MOD subset using client.search_atp_topics(mod_abbr=...)
    cli = _get_client()
    try:
        subset_topics = cli.search_atp_topics(mod_abbr=mod_abbreviation) or []
        allowed_topics = {r["curie"] for r in subset_topics}
        workflow_topic = {row[0]: row[1] for row in db.query(WorkflowTagTopicModel.workflow_tag, WorkflowTagTopicModel.
                                                             topic).all()}
        candidate_topic = {candidate_wf: workflow_topic[candidate_wf] for candidate_wf in candidates if candidate_wf in
                           workflow_topic}
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Failed to load subset topics for {mod_abbreviation}: {e}")

    results = [atp_parent_id]
    results.extend([c for c in candidates if c in candidate_topic and candidate_topic[c] in allowed_topics])
    return results


# -----------------------------
# ATP caching and traversal (client-powered)
# -----------------------------

def set_globals(atp_to_name_init, name_to_atp_init, atp_to_children_init, atp_to_parent_init):
    global atp_to_name, name_to_atp, atp_to_children, atp_to_parent
    atp_to_name = atp_to_name_init.copy()
    name_to_atp = name_to_atp_init.copy()
    atp_to_children = atp_to_children_init.copy()
    atp_to_parent = atp_to_parent_init.copy()


def _ensure_atp_loaded():
    if not atp_to_name or not atp_to_children or not atp_to_parent:
        load_name_to_atp_and_relationships()


def load_name_to_atp_and_relationships(start_terms: Optional[List[str]] = None):
    """
    Build ATP maps (name <-> curie, children, parent) by BFS traversal
    from root terms using _get_atp_children for consistent caching.
    """
    if start_terms is None:
        start_terms = ['ATP:0000177', 'ATP:0000335']

    # Clear and (re)build
    atp_to_name.clear()
    name_to_atp.clear()
    atp_to_children.clear()
    atp_to_parent.clear()

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


def atp_get_all_ancestors(curie: str) -> List[str]:
    """
    Climb parent pointers we built. For strict ancestry, we can fall back to the client.
    """
    if not atp_to_parent:
        try:
            return _get_client().search_ontology_ancestors_or_descendants(ontology_node=curie, direction="ancestors")
        except AGRAPIError:
            pass

    parent_list: List[str] = []
    not_seen: List[str] = [curie]
    while not_seen:
        p = not_seen.pop(0)
        parent = atp_to_parent.get(p)
        if parent:
            parent_list.append(parent)
            not_seen.append(parent)
    return parent_list


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
        ) or []
    except AGRAPIError:
        return []

    child_curies = []
    for child in children:
        curie, name = child["curie"], child["name"]
        atp_to_name[curie] = name
        name_to_atp[name] = curie
        atp_to_parent.setdefault(curie, parent_curie)
        child_curies.append(curie)

    if child_curies:
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
