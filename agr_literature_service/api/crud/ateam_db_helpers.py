"""A-team ontology terms, their hierarchy, MOD subsets and biological entities (SCRUM-6720 Phase 3).

In the merged database the A-team tables (ontologyterm, ontologytermclosure, ontologyterm_subsets,
biologicalentity, slotannotation, ...) are ABC tables too, so every lookup here is SQL on ABC's own
engine. These queries are the ones agr_curation_api's db_methods ran against the separate A-team
database (agr-curation-api-client 0.13.0), so results are unchanged; PERSISTENT_STORE_DB_* is no
longer needed. A failed query still raises AGRAPIError, which callers already turn into a 502.

The only A-team API call left is get_or_create_species: it imports a taxon from NCBI into A-team.
"""
import logging
import threading

from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi import HTTPException, status
from typing import Any, Dict, List, Optional, Iterable, Sequence, Tuple, Union, cast
from sqlalchemy import text, bindparam
from agr_curation_api import AGRCurationAPIClient, AGRAPIError  # type: ignore

from agr_literature_service.api.database.main import SessionLocal, engine

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

# SCRUM-6720 Phase 3: every ATP term's direct children, read in one query at the start of a cache
# build (_sql_atp_children_of_all). The build's BFS walks it in memory instead of asking for each
# term's children (~305 A-team API calls), so the maps come out as before.
_atp_edges: Optional[Dict[str, List[Dict[str, str]]]] = None

_client: Optional[AGRCurationAPIClient] = None


def _get_client() -> AGRCurationAPIClient:
    """The A-team API client, now only for get_or_create_species."""
    global _client
    if _client is None:
        _client = AGRCurationAPIClient()
    return _client


# -----------------------------
# SQL on the merged database
# -----------------------------

def _rows(sql, params: Optional[Dict[str, Any]] = None) -> Sequence[Any]:
    """Run a read query on ABC's engine; a database error becomes AGRAPIError, as from the client."""
    statement = text(sql) if isinstance(sql, str) else sql
    try:
        with engine.connect() as connection:
            return connection.execute(statement, params or {}).fetchall()
    except Exception as e:
        raise AGRAPIError(f"Database query failed: {e}")


def _sql_map_entity_curies_to_info(entity_type: str, entity_curies: List[str]) -> List[Dict[str, Any]]:
    if not entity_curies:
        return []
    entity_type = entity_type.lower()
    if entity_type in ["gene", "allele"]:
        sql = f"""SELECT DISTINCT be.primaryexternalid, be.obsolete, be.primaryexternalid
                  FROM biologicalentity be, {entity_type} ent_tbl
                  WHERE be.id = ent_tbl.id AND UPPER(be.primaryexternalid) IN :curies"""
    elif entity_type == "construct":
        sql = """SELECT DISTINCT r.primaryexternalid, r.obsolete, r.primaryexternalid
                 FROM reagent r, construct c
                 WHERE r.id = c.id AND UPPER(r.primaryexternalid) IN :curies"""
    elif entity_type in ["agm", "agms", "strain", "genotype", "fish"]:
        sql = """SELECT DISTINCT be.primaryexternalid, be.obsolete, be.primaryexternalid
                 FROM biologicalentity be, affectedgenomicmodel agm
                 WHERE be.id = agm.id AND UPPER(be.primaryexternalid) IN :curies"""
    elif "targeting reagent" in entity_type:
        sql = """SELECT DISTINCT be.primaryexternalid, be.obsolete, be.primaryexternalid
                 FROM biologicalentity be, sequencetargetingreagent str
                 WHERE be.id = str.id AND UPPER(be.primaryexternalid) IN :curies"""
    else:
        raise AGRAPIError(f"Unknown entity_type '{entity_type}'")
    rows = _rows(text(sql).bindparams(bindparam("curies", expanding=True)),
                 {"curies": [curie.upper() for curie in entity_curies]})
    return [{"entity_curie": row[0], "is_obsolete": row[1], "entity": row[2]} for row in rows]


_ENTITY_NAME_SLOTS = {
    "gene": ("singlegene_id", ("GeneSymbolSlotAnnotation", "GeneSystematicNameSlotAnnotation",
                               "GeneFullNameSlotAnnotation")),
    "allele": ("singleallele_id", ("AlleleSymbolSlotAnnotation",)),
    "agm": ("singleagm_id", ("AgmFullNameSlotAnnotation", "AgmSecondaryIdSlotAnnotation",
                             "AgmSynonymSlotAnnotation")),
}


def _sql_map_entity_names_to_curies(entity_type: str, entity_names: List[str],
                                    taxon: Optional[str]) -> List[Dict[str, Any]]:
    if not entity_names:
        return []
    entity_type = entity_type.lower()
    params: Dict[str, Any] = {"names": [name.upper() for name in entity_names], "taxon": taxon}
    if entity_type in ["agms", "strain", "genotype", "fish"]:
        entity_type = "agm"
    if entity_type in _ENTITY_NAME_SLOTS:
        slot_column, slot_types = _ENTITY_NAME_SLOTS[entity_type]
        sql = f"""SELECT DISTINCT be.primaryexternalid, sa.obsolete, sa.displaytext
                  FROM biologicalentity be
                  JOIN slotannotation sa ON be.id = sa.{slot_column}
                  JOIN ontologyterm ot ON be.taxon_id = ot.id
                  WHERE sa.slotannotationtype IN :slot_types
                  AND UPPER(sa.displaytext) IN :names
                  AND ot.curie = :taxon"""
        params["slot_types"] = list(slot_types)
        statement = text(sql).bindparams(bindparam("names", expanding=True), bindparam("slot_types", expanding=True))
    elif "targeting reagent" in entity_type:
        statement = text("""SELECT DISTINCT be.primaryexternalid, be.obsolete, str.name
                            FROM biologicalentity be
                            JOIN sequencetargetingreagent str ON be.id = str.id
                            JOIN ontologyterm ot ON be.taxon_id = ot.id
                            WHERE UPPER(str.name) IN :names AND ot.curie = :taxon""").bindparams(
            bindparam("names", expanding=True))
    elif entity_type == "construct":
        statement = text("""SELECT DISTINCT r.primaryexternalid, sa.obsolete, sa.displaytext
                            FROM reagent r
                            JOIN slotannotation sa ON r.id = sa.singleconstruct_id
                            WHERE sa.slotannotationtype IN ('ConstructFullNameSlotAnnotation',
                                                            'ConstructSymbolSlotAnnotation')
                            AND UPPER(sa.displaytext) IN :names""").bindparams(bindparam("names", expanding=True))
    else:
        raise AGRAPIError(f"Unknown entity_type '{entity_type}'")
    rows = _rows(statement, params)
    return [{"entity_curie": row[0], "is_obsolete": row[1], "entity": row[2]} for row in rows]


def _sql_map_curies_to_names(category: str, curies: List[str]) -> Dict[str, str]:
    """Display names of entity or ontology curies (unknown categories map to themselves)."""
    category = category.lower()
    if category == "gene":
        sql = """SELECT be.primaryexternalid, sa.displaytext FROM biologicalentity be
                 JOIN slotannotation sa ON be.id = sa.singlegene_id
                 WHERE be.primaryexternalid IN :curies AND sa.slotannotationtype = 'GeneSymbolSlotAnnotation'"""
    elif "allele" in category:
        sql = """SELECT be.primaryexternalid, sa.displaytext FROM biologicalentity be
                 JOIN slotannotation sa ON be.id = sa.singleallele_id
                 WHERE be.primaryexternalid IN :curies AND sa.slotannotationtype = 'AlleleSymbolSlotAnnotation'"""
    elif category in ["affected genome model", "agm", "strain", "genotype", "fish"]:
        sql = """SELECT DISTINCT be.primaryexternalid, sa.displaytext FROM biologicalentity be
                 JOIN slotannotation sa ON be.id = sa.singleagm_id
                 WHERE be.primaryexternalid IN :curies AND sa.slotannotationtype = 'AgmFullNameSlotAnnotation'"""
    elif "construct" in category:
        sql = """SELECT r.primaryexternalid, sa.displaytext FROM reagent r
                 JOIN slotannotation sa ON r.id = sa.singleconstruct_id
                 WHERE r.primaryexternalid IN :curies AND sa.slotannotationtype = 'ConstructSymbolSlotAnnotation'"""
    elif category in ["species", "ecoterm"]:
        rows = _rows(text("SELECT curie, name FROM ontologyterm WHERE UPPER(curie) IN :curies").bindparams(
            bindparam("curies", expanding=True)), {"curies": [curie.upper() for curie in curies]})
        return {row[0]: row[1] for row in rows}
    else:
        return {curie: curie for curie in curies}
    rows = _rows(text(sql).bindparams(bindparam("curies", expanding=True)), {"curies": list(curies)})
    return {row[0]: row[1] for row in rows}


def _sql_search_atp_topics(topic: Optional[str], mod_abbr: Optional[str], limit: Optional[int]) -> List[dict]:
    """ATP topics (descendants of topic_category_atp) by name substring and/or MOD subset. As in the
    client, a MOD-only search is not limited."""
    if not topic and not mod_abbr:
        return []
    conditions = ["ot.ontologytermtype = 'ATPTerm'", "ancestor.curie = :root"]
    params: Dict[str, Any] = {"root": topic_category_atp}
    subset_join = ""
    if topic:
        conditions += ["UPPER(ot.name) LIKE :name", "ot.obsolete = false"]
        params["name"] = f"%{topic.upper()}%"
    if mod_abbr:
        subset_join = "JOIN ontologyterm_subsets s ON ot.id = s.ontologyterm_id"
        conditions.append("s.subsets = :subset")
        params["subset"] = f"{mod_abbr}_tag"
    limit_clause = ""
    if topic:
        limit_clause = "LIMIT :limit"
        params["limit"] = limit
    rows = _rows(f"""SELECT DISTINCT ot.curie, ot.name
                     FROM ontologyterm ot
                     JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
                     JOIN ontologyterm ancestor ON ancestor.id = otc.closureobject_id
                     {subset_join}
                     WHERE {" AND ".join(conditions)}
                     {limit_clause}""", params)
    return [{"curie": row[0], "name": row[1]} for row in rows]


def _sql_filter_atp_by_mod_subset(curies: List[str], mod_abbr: str) -> List[str]:
    if not curies or not mod_abbr:
        return []
    rows = _rows(text("""SELECT DISTINCT ot.curie FROM ontologyterm ot
                         JOIN ontologyterm_subsets s ON ot.id = s.ontologyterm_id
                         WHERE ot.ontologytermtype = 'ATPTerm' AND UPPER(ot.curie) IN :curies
                         AND s.subsets = :subset""").bindparams(bindparam("curies", expanding=True)),
                 {"curies": [curie.upper() for curie in curies], "subset": f"{mod_abbr}_tag"})
    return [row[0] for row in rows]


def _sql_atp_descendants(ancestor_curie: str, direct_children_only: bool = False) -> List[Dict[str, str]]:
    """(curie, name) of the non-obsolete ATP descendants (or direct children) of ancestor_curie,
    ordered by curie so the cache build is deterministic."""
    distance = "AND otc.distance = 1" if direct_children_only else ""
    rows = _rows(f"""SELECT DISTINCT ot.curie, ot.name
                     FROM ontologyterm ot
                     JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
                     JOIN ontologyterm ancestor ON ancestor.id = otc.closureobject_id
                     WHERE ot.ontologytermtype = 'ATPTerm' AND ot.obsolete = false
                     AND ancestor.curie = :ancestor {distance}
                     ORDER BY ot.curie""", {"ancestor": ancestor_curie})
    return [{"curie": row[0], "name": row[1]} for row in rows]


def _sql_atp_children_of_all() -> Dict[str, List[Dict[str, str]]]:
    """Every ATP parent's non-obsolete direct ATP children, in one query: what _sql_atp_descendants
    (direct_children_only=True) returns for each parent."""
    rows = _rows("""SELECT DISTINCT parent.curie, ot.curie, ot.name
                    FROM ontologyterm ot
                    JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
                    JOIN ontologyterm parent ON parent.id = otc.closureobject_id
                    WHERE ot.ontologytermtype = 'ATPTerm' AND ot.obsolete = false
                    AND parent.ontologytermtype = 'ATPTerm' AND otc.distance = 1
                    ORDER BY parent.curie, ot.curie""")
    children: Dict[str, List[Dict[str, str]]] = {}
    for parent, curie, name in rows:
        children.setdefault(parent, []).append({"curie": curie, "name": name})
    return children


def _sql_ontology_ancestors_or_descendants(ontology_node: str, direction: str) -> List[str]:
    if direction == "descendants":
        sql = """SELECT DISTINCT ot.curie FROM ontologyterm ot
                 JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
                 JOIN ontologyterm node ON node.id = otc.closureobject_id
                 WHERE node.curie = :node AND ot.obsolete = false"""
    else:
        sql = """SELECT DISTINCT ot.curie FROM ontologyterm ot
                 JOIN ontologytermclosure otc ON ot.id = otc.closureobject_id
                 JOIN ontologyterm node ON node.id = otc.closuresubject_id
                 WHERE node.curie = :node AND ot.obsolete = false"""
    return [row[0] for row in _rows(sql, {"node": ontology_node})]


def _sql_ontology_terms(curies: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
    """name, definition and synonyms of the non-obsolete terms among curies (None for the others)."""
    results: Dict[str, Optional[Dict[str, Any]]] = {curie: None for curie in curies}
    if not curies:
        return results
    rows = _rows("""SELECT ot.curie, ot.name, ot.definition,
                           ARRAY_AGG(DISTINCT s.name) FILTER (WHERE s.name IS NOT NULL)
                    FROM ontologyterm ot
                    LEFT JOIN ontologyterm_synonym ots ON ot.id = ots.ontologyterm_id
                    LEFT JOIN synonym s ON ots.synonyms_id = s.id
                    WHERE ot.curie = ANY(:curies) AND ot.obsolete = false
                    GROUP BY ot.curie, ot.name, ot.definition""", {"curies": list(curies)})
    for curie, name, definition, synonyms in rows:
        results[curie] = {"curie": curie, "name": name, "definition": definition, "synonyms": synonyms or []}
    return results


def _sql_atp_names(curies: List[str]) -> Dict[str, str]:
    """Names of the given (non-obsolete) terms, the part of _sql_ontology_terms the ATP cache uses."""
    if not curies:
        return {}
    rows = _rows("SELECT curie, name FROM ontologyterm WHERE curie = ANY(:curies) AND obsolete = false",
                 {"curies": list(curies)})
    return {row[0]: row[1] for row in rows}


def _sql_search_species(species: str, limit: int) -> List[Dict[str, str]]:
    if species.upper().startswith("NCBITAXON"):
        column, pattern = "curie", f"{species.upper()}%"
    else:
        column, pattern = "name", f"%{species.upper()}%"
    rows = _rows(f"""SELECT curie, name FROM ontologyterm
                     WHERE ontologytermtype = 'NCBITaxonTerm' AND UPPER({column}) LIKE :pattern
                     LIMIT :limit""", {"pattern": pattern, "limit": limit})
    return [{"curie": row[0], "name": row[1]} for row in rows]


def map_entity_to_curie(entity_type: str, entity_list: str, taxon: str) -> JSONResponse:
    """
    Map pipe-delimited entity identifiers to CURIEs using the shared client.
    Returns the same JSONResponse shape as before.
    """
    entity_type_lc = (entity_type or "").lower()
    if not entity_type_lc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="Missing entity_type")

    name_list, curie_list = classify_entity_list(entity_list)

    try:
        data: List[Dict[str, object]] = []
        if curie_list:
            curie_rows = _sql_map_entity_curies_to_info(entity_type_lc, curie_list)
            data.extend(
                {"entity_curie": r["entity_curie"], "is_obsolete": r["is_obsolete"], "entity": r["entity"]}
                for r in curie_rows
            )
        if name_list:
            name_rows = _sql_map_entity_names_to_curies(entity_type_lc, name_list, taxon)
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
    """Search ATP topics (supports MOD subset filter)."""
    try:
        rows = _sql_search_atp_topics(topic, mod_abbr, 10)
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"ATP topic search failed: {e}")

    data = [{"curie": r["curie"], "name": r["name"]} for r in (rows or [])]
    return JSONResponse(content=jsonable_encoder(data))


def search_topic_list(topic: Optional[str] = None, mod_abbr: Optional[str] = None, limit: int = 10) -> List[dict]:
    """Return [{'curie':..., 'name':...}, ...] instead of a JSONResponse (for internal callers)."""
    try:
        rows = _sql_search_atp_topics(topic, mod_abbr, limit)
        return [{"curie": r["curie"], "name": r["name"]} for r in rows]
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"ATP topic search failed: {e}")


def search_atp_descendants(ancestor_curie: str) -> JSONResponse:
    """Return descendants (curie, name) of an ATP node."""
    try:
        rows = _sql_atp_descendants(ancestor_curie)
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
    try:
        terms = _sql_ontology_terms(curies)
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Ontology term lookup failed: {e}")
    data = {curie: term for curie, term in terms.items() if term is not None}
    return JSONResponse(content=jsonable_encoder(data))


def search_species(species: str) -> JSONResponse:
    """Search NCBITaxonTerm (by name or CURIE)."""
    try:
        rows = _sql_search_species(species, 10)
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

    Uses cached ATP ontology data when available (ATP: terms), otherwise the closure table.
    """
    # ATPs are cached locally, so skip client call if applicable
    if ontology_node.startswith("ATP:"):
        if ancestors_or_descendants == "descendants":
            # When include_names=False (default), returns List[str]
            return cast(List[str], atp_get_all_descendants(ontology_node))
        return atp_get_all_ancestors(ontology_node)

    direction = "descendants" if ancestors_or_descendants == "descendants" else "ancestors"
    try:
        return _sql_ontology_ancestors_or_descendants(ontology_node, direction)
    except AGRAPIError as e:
        raise HTTPException(status_code=502, detail=f"Ontology traversal failed: {e}")


# ---------- ATP name fetching helpers ----------

def _fetch_atp_names(missing_curies: List[str]) -> None:
    """Populate atp_to_name and name_to_atp for uncached (non-obsolete) terms."""
    if not missing_curies:
        return
    try:
        for curie, name in _sql_atp_names(missing_curies).items():
            atp_to_name[curie] = name
            name_to_atp[name] = curie
    except Exception as e:
        logger.debug("ATP name fetching failed for %s: %s", missing_curies, e)


def _fetch_atp_names_for_curie_list(curies: List[str]) -> None:
    """
    Ensure atp_to_name is filled for given ATP curies by directly querying ontologyterm (ATPTerm).
    """
    missing = [c for c in curies if c and c not in atp_to_name]
    if not missing:
        return
    try:
        rows = _rows(text("""
            SELECT curie, name
            FROM ontologyterm
            WHERE ontologytermtype = 'ATPTerm'
              AND UPPER(curie) IN :curies
        """).bindparams(bindparam("curies", expanding=True)), {"curies": [c.upper() for c in missing]})
        for curie, name in rows:
            atp_to_name[curie] = name
            name_to_atp[name] = curie
    except Exception as e:
        logger.debug("ATP fetching query failed: %s", e)


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
    try:
        result = _sql_map_curies_to_names(cat_raw, curie_list)
        if result:
            return result
    except AGRAPIError as e:
        logger.debug("DB mapping failed/unavailable for category=%r: %s", cat_raw, e)

    # Fall back to identity mapping for unknown categories.
    return {c: c for c in curie_list}


def create_ateam_db_session():
    """
    A session on the database holding the A-team tables: since the merge, ABC's own (kept so
    lit-processing programs can still use this function).
    """
    return SessionLocal()


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
    out: List[str] = []
    if curies:
        rows = _sql_map_entity_curies_to_info(entity_type_lc, curies)
        out.extend([r["entity_curie"] for r in rows])
    if names:
        rows = _sql_map_entity_names_to_curies(entity_type_lc, names, taxon)
        out.extend([r["entity_curie"] for r in rows])
    return out


# -----------------------------
# Jobs/subset logic
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
    try:
        allowed = set(_sql_filter_atp_by_mod_subset(list(children), mod_abbreviation))
    except AGRAPIError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to filter subset for {mod_abbreviation}: {e}"
        )

    results = [atp_parent_id]
    results.extend(c for c in children if c in allowed)
    return results


# -----------------------------
# ATP caching and traversal
# -----------------------------

def set_globals(atp_to_name_init, name_to_atp_init, atp_to_children_init, atp_to_parent_init):
    global atp_to_name, name_to_atp, atp_to_children, atp_to_parent, _atp_cache_ready, _atp_edges
    _atp_edges = None
    atp_to_name = atp_to_name_init.copy()
    name_to_atp = name_to_atp_init.copy()
    atp_to_children = atp_to_children_init.copy()
    atp_to_parent = atp_to_parent_init.copy()
    # Injected wholesale, so the result is complete by construction.
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
        # A branch left out here is not merely uncached. atp_get_all_ancestors falls back
        # to the ontology client only when atp_to_parent is *entirely* empty, so in any
        # warm worker a missing branch resolves to however much of the chain happens to
        # have been cached as a side effect of earlier descendant lookups -- silently, and
        # differently per worker. For data_context, whose terms are never walked as
        # descendants in normal operation, that meant no ancestors at all and a
        # hierarchy check that degraded to exact equality.
        #
        # Order matters: frontier.pop() is LIFO and atp_to_parent uses setdefault
        # (first write wins), so the LAST entry here is walked FIRST and claims the
        # parent pointer for any term reachable from two roots. New branches go at the
        # front, leaving the pre-existing precedence between 335 and 177 untouched.
        start_terms = ['ATP:0000002', 'ATP:0000323', 'ATP:0000177', 'ATP:0000335']

    global _atp_cache_ready, _atp_cache_building_thread, _atp_edges
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
            _atp_edges = _sql_atp_children_of_all()

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
        except Exception:
            # Leave the maps empty rather than half-built: _ensure_atp_loaded's emptiness
            # test then forces a genuine retry on the next call instead of pinning a
            # partial hierarchy for the life of the worker.
            _atp_edges = None
            atp_to_name.clear()
            name_to_atp.clear()
            atp_to_children.clear()
            atp_to_parent.clear()
            raise
        finally:
            _atp_cache_building_thread = None
        _atp_cache_ready = True

    logger.debug("ATP global vars successfully loaded from the ontology tables")


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
    # SCRUM-6474: build the cache first. Without this a cold cache fell through to the
    # client on every call AND never populated the maps, so the "fallback" silently became
    # the permanent path at ~520ms a call. The API hid it because create_tag calls
    # atp_return_invalid_ids (which does _ensure_atp_loaded) before any rule runs, but
    # revalidate_all_tags reaches get_ancestors first -- so the sweep paid it per tag.
    _ensure_atp_loaded()
    if not atp_to_parent:
        try:
            return _sql_ontology_ancestors_or_descendants(curie, "ancestors")
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
    if _atp_edges is not None:
        # every ATP term's children were read at the cache build; a term absent there is a leaf
        children: Optional[List[Dict[str, str]]] = _atp_edges.get(parent_curie, [])
    else:
        try:
            children = _sql_atp_descendants(parent_curie, direct_children_only=True)
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
