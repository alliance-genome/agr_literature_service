# ateam_db_helpers.py

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi import HTTPException, status
from typing import Dict, List, Optional, Iterable
import cachetools.func
import logging

# Client library (your refactored multi-source client)
from agr_curation_api import AGRCurationAPIClient, AGRAPIError  # type: ignore


# for testing
from sqlalchemy import text, bindparam
# for testing


logger = logging.getLogger(__name__)

# List of valid prefix identifiers for curies
curie_prefix_list = ["FB", "MGI", "RGD", "SGD", "WB", "XenBase", "ZFIN"]

# Topic tag for ATP ontology (kept for compatibility)
topic_category_atp = "ATP:0000002"

# In-memory ATP caches
atp_to_name: Dict[str, str] = {}
name_to_atp: Dict[str, str] = {}
atp_to_parent: Dict[str, str] = {}
atp_to_children: Dict[str, List[str]] = {}

# Lazily-initialized shared client
_client: Optional[AGRCurationAPIClient] = None


def _get_client() -> AGRCurationAPIClient:
    global _client
    if _client is None:
        _client = AGRCurationAPIClient()
    return _client


# -----------------------------
# Public API used by endpoints
# -----------------------------

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

        # CURIEs → basic info
        if curie_list:
            curie_rows = cli.map_entity_curies_to_info(entity_type=entity_type_lc, entity_curies=curie_list)
            data.extend(
                {"entity_curie": r["entity_curie"], "is_obsolete": r["is_obsolete"], "entity": r["entity"]}
                for r in curie_rows
            )

        # Names → CURIEs (requires taxon for most types)
        if name_list:
            name_rows = cli.map_entity_names_to_curies(entity_type=entity_type_lc, entity_names=name_list, taxon=taxon)
            data.extend(
                {"entity_curie": r["entity_curie"], "is_obsolete": r["is_obsolete"], "entity": r["entity"]}
                for r in name_rows
            )

    except AGRAPIError as e:
        # Keep old behavior: 422 for unknown entity_type; 502 for other client errors
        if "Unknown entity_type" in str(e):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Unknown entity_type '{entity_type}'")
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Mapping failed: {e}")

    return JSONResponse(content=jsonable_encoder(data))


def classify_entity_list(entity_list: str) -> (List[str], List[str]):
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
    Uses client.search_ontology_ancestors_or_descendants.
    """
    cli = _get_client()
    direction = "descendants" if ancestors_or_descendants == "descendants" else "ancestors"
    try:
        return cli.search_ontology_ancestors_or_descendants(ontology_node=ontology_node, direction=direction)
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Ontology traversal failed: {e}")


# ---------- ATP name hydration helper ----------

def _hydrate_atp_names(missing_curies: List[str]) -> None:
    """
    Fill atp_to_name/name_to_atp for any ATP curies not in cache by asking the client.
    If the client doesn't expose get_ontology_names_by_curies, we just skip (graceful no-op).
    """
    if not missing_curies:
        return
    cli = _get_client()
    try:
        # Prefer the dedicated DB lookup if available
        if hasattr(cli, "get_ontology_names_by_curies"):
            curie_to_name = cli.get_ontology_names_by_curies(missing_curies)  # type: ignore[attr-defined]
            for curie, name in (curie_to_name or {}).items():
                atp_to_name[curie] = name
                name_to_atp[name] = curie
    except Exception as e:
        logger.warning("ATP name hydration failed for %s: %s", missing_curies, e)


def _hydrate_atp_names_for_curie_list(curies: list[str]) -> None:
    """
    Ensure atp_to_name is filled for given ATP curies.
    Uses the client's DB session to read ontologyterm (ATPTerm).
    No-ops if DB is unavailable.
    """
    missing = [c for c in curies if c and c not in atp_to_name]
    if not missing:
        return
    try:
        cli = _get_client()
        dbm = cli._get_db_methods()            # uses env DB config
        session = dbm._create_session()
    except Exception as e:
        logger.debug("ATP hydration: DB session unavailable: %s", e)
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
        logger.debug("ATP hydration query failed: %s", e)
    finally:
        session.close()


def map_curies_to_names(category: str, curies: Iterable[str]) -> Dict[str, str]:
    """
    Map CURIEs -> display names. Handles ATP locally; delegates others to client,
    with API fallback when DB mapping fails/returns empty.
    """
    curie_list = [c for c in curies if c]
    if not curie_list:
        return {}
    cat_raw = (category or "").strip()
    cat_norm = cat_raw.replace(" ", "").lower()

    # Treat any 'atpterm' OR category starting with 'ATP:' as ATP
    if cat_norm in {"atpterm", "atp"} or cat_raw.upper().startswith("ATP:"):
        _ensure_atp_loaded()
        _hydrate_atp_names_for_curie_list(curie_list)  # best-effort
        return {c: atp_to_name.get(c, c) for c in curie_list}

    # Non-ATP: try client DB mapping, then API fallback
    cli = _get_client()
    try:
        result = cli.map_curies_to_names(category=cat_raw, curies=curie_list)
        if result:
            return result
    except AGRAPIError as e:
        logger.debug("DB mapping failed/unavailable: %s", e)


# -----------------------------
# Jobs/subset logic (uses client)
# -----------------------------

@cachetools.func.ttl_cache(ttl=12 * 60 * 60)
def get_jobs_to_run(name: str, mod_abbreviation: str) -> List[str]:
    """
    Use ATP children + subset filter from search_atp_topics(mod_abbr) to find jobs.
    - If name is an ATP:curie, treat it as the parent.
    - Else, look for "<name> needed" in ATP names (your existing convention).
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
        allowed = {r["curie"] for r in subset_topics}
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Failed to load subset topics for {mod_abbreviation}: {e}")

    results = [atp_parent_id]
    results.extend([c for c in candidates if c in allowed])
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
    Build ATP maps (name<->curie, children, parent) **without direct SQL**,
    by traversing via client.get_atp_descendants() from a set of roots.

    Note: client.get_atp_descendants returns all descendants (not just immediate children).
    We approximate parent/child by linking the current frontier node as parent of all returned
    descendants to ensure ancestors/descendants queries and name mappings work.
    """
    if start_terms is None:
        # Defaults from your previous module
        start_terms = ['ATP:0000177', 'ATP:0000335']

    cli = _get_client()

    # Clear and (re)build
    atp_to_name.clear()
    name_to_atp.clear()
    atp_to_children.clear()
    atp_to_parent.clear()

    frontier = list(start_terms)
    seen = set(frontier)

    # Optional: hydrate root names (best-effort)
    _hydrate_atp_names(frontier)

    while frontier:
        parent = frontier.pop()
        try:
            descendants = cli.get_atp_descendants(ancestor_curie=parent) or []
        except AGRAPIError as e:
            logger.warning("Failed to load ATP descendants for %s: %s", parent, e)
            continue

        children_curie_list: List[str] = []
        for node in descendants:
            curie = node["curie"]
            nm = node["name"]
            atp_to_name[curie] = nm
            name_to_atp[nm] = curie
            if curie not in atp_to_parent:
                atp_to_parent[curie] = parent
            children_curie_list.append(curie)
            if curie not in seen:
                seen.add(curie)
                frontier.append(curie)

        if children_curie_list:
            # Deduplicate while preserving any prior children recorded
            atp_to_children[parent] = list({*atp_to_children.get(parent, []), *children_curie_list})

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
        _hydrate_atp_names(missing)
    return {c: atp_to_name.get(c, c) for c in curies}


@cachetools.func.ttl_cache(ttl=24 * 60 * 60)
def atp_get_all_descendents(curie: str) -> List[str]:
    """
    Return all descendant ATP curies for the given curie.
    No exceptions; returns [] if none.
    """
    try:
        _, subset_atp_to_name = get_name_to_atp_for_all_children(curie)
        return list(subset_atp_to_name.keys())
    except Exception as e:
        logger.warning("Failed to fetch ATP descendants for %s: %s", curie, e)
        return []


def atp_get_all_ancestors(curie: str) -> List[str]:
    """
    Climb parent pointers we built. For strict ancestry, we can fall back to the client.
    """
    if not atp_to_parent:
        # precise ancestors via client
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
    # If still missing, try a one-off hydration
    if atp_id not in atp_to_name:
        _hydrate_atp_names([atp_id])
    return atp_to_name.get(atp_id)


def atp_return_invalid_ids(atp_ids: List[str]) -> List[str]:
    _ensure_atp_loaded()
    return [a for a in atp_ids if a not in atp_to_name]


def get_name_to_atp_for_all_children(workflow_parent: str):
    """
    Return ALL descendants for an ATP term as two dicts:
      subset_name_to_atp:  {<name>: <ATP:curie>, ...}
      subset_atp_to_name:  {<ATP:curie>: <name>, ...}

    This always returns a 2-tuple (possibly both empty dicts).
    """
    _ensure_atp_loaded()

    subset_name_to_atp: Dict[str, str] = {}
    subset_atp_to_name: Dict[str, str] = {}

    # Start from known children (if any); otherwise return empty tuple
    frontier = list(atp_to_children.get(workflow_parent, []))
    if not frontier:
        # Try a best-effort: if we don't know children yet, attempt hydration via client
        try:
            desc = _get_client().get_atp_descendants(ancestor_curie=workflow_parent) or []
            for node in desc:
                cur = node["curie"]
                nm = node["name"]
                atp_to_name[cur] = nm
                name_to_atp[nm] = cur
                atp_to_parent.setdefault(cur, workflow_parent)
                atp_to_children.setdefault(workflow_parent, []).append(cur)
            frontier = list(atp_to_children.get(workflow_parent, []))
        except AGRAPIError:
            pass

    if not frontier:
        return subset_name_to_atp, subset_atp_to_name

    # BFS over our cached graph
    seen = set(frontier)
    while frontier:
        curie = frontier.pop()
        name = atp_to_name.get(curie, curie)
        subset_atp_to_name[curie] = name
        subset_name_to_atp[name] = curie
        children = atp_to_children.get(curie, [])
        for ch in children:
            if ch not in seen:
                seen.add(ch)
                frontier.append(ch)

    return subset_name_to_atp, subset_atp_to_name
