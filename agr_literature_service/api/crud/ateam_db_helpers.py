from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from os import environ
from typing import Dict
from sqlalchemy import text
from fastapi import HTTPException, status
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import cachetools.func
import logging

logger = logging.getLogger(__name__)

# List of valid prefix identifiers for curies
curie_prefix_list = ["FB", "MGI", "RGD", "SGD", "WB", "XenBase", "ZFIN"]

# Topic tag for ATP ontology
topic_category_atp = "ATP:0000002"

# Store these to save lookups.
atp_to_name: Dict[str, str] = {}
name_to_atp: Dict[str, str] = {}
atp_to_parent: Dict[str, str] = {}
atp_to_children : Dict[str, list] = {}


def create_ateam_db_session():
    """Create and return a SQLAlchemy session connected to the A-team database."""
    USER = environ.get('PERSISTENT_STORE_DB_USERNAME', 'unknown')
    PASSWORD = environ.get('PERSISTENT_STORE_DB_PASSWORD', 'unknown')
    SERVER = environ.get('PERSISTENT_STORE_DB_HOST', 'localhost')
    PORT = environ.get('PERSISTENT_STORE_DB_PORT', '5432')
    DB = environ.get('PERSISTENT_STORE_DB_NAME', 'unknown')

    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB
    engine = create_engine(engine_var)

    SessionClass = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = SessionClass()
    return session


def map_entity_to_curie(entity_type, entity_list, taxon):
    """Map an entity list (gene, allele, etc.) to their curies, taking into account names and taxon."""
    db = create_ateam_db_session()
    entity_type = entity_type.lower()
    (entity_name_list, entity_curie_list) = classify_entity_list(entity_list)

    entity_curie_rows = search_for_entity_curies(db, entity_type, entity_curie_list)
    entity_name_rows = search_for_entity_names(db, entity_type, entity_name_list, taxon)
    db.close()

    if entity_curie_rows is None or entity_name_rows is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown entity_type '{entity_type}'"
        )

    data = [
        {
            "entity_curie": row[0],
            "is_obsolete": row[1],
            "entity": row[2]
        }
        for row in (entity_curie_rows or []) + (entity_name_rows or [])
    ]
    json_data = jsonable_encoder(data)
    return JSONResponse(content=json_data)


def classify_entity_list(entity_list):
    """Split a raw entity_list string into separate lists for names and curies."""
    entity_name_list = []
    entity_curie_list = []

    # Example: if entity_list is "MGI:1234|ACT1|SGD:S00001"
    # then "MGI:1234" and "SGD:S00001" go into entity_curie_list,
    # while "ACT1" goes into entity_name_list.
    for entity in entity_list.split("|"):
        is_mod_curie = False
        for curie_prefix in curie_prefix_list:
            if entity.startswith(curie_prefix + ":"):
                is_mod_curie = True
                break
        if is_mod_curie:
            entity_curie_list.append(entity.upper())
        else:
            entity_name_list.append(entity.upper())

    return entity_name_list, entity_curie_list


def search_for_entity_names(db: Session, entity_type, entity_name_list, taxon):
    """Look up entities in the DB by name (gene symbol, allele symbol, etc.), restricted by taxon."""
    if len(entity_name_list) == 0:
        return []

    if entity_type == 'gene':
        """
        gene symbol: ACT1
        systematic name: YFL039C
        genomic feature: CEN1
        """
        sql_query = text("""
        SELECT DISTINCT be.primaryexternalid, sa.obsolete, sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singlegene_id
        JOIN ontologyterm ot ON be.taxon_id = ot.id
        WHERE sa.slotannotationtype in (
            'GeneSymbolSlotAnnotation',
            'GeneSystematicNameSlotAnnotation',
            'GeneFullNameSlotAnnotation'
        )
        AND UPPER(sa.displaytext) IN :entity_name_list
        AND ot.curie = :taxon
        """)

    elif entity_type == 'allele':
        # For 'allele' and 'transgenic allele'
        sql_query = text("""
        SELECT DISTINCT be.primaryexternalid, sa.obsolete, sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singleallele_id
        JOIN ontologyterm ot ON be.taxon_id = ot.id
        WHERE sa.slotannotationtype = 'AlleleSymbolSlotAnnotation'
        AND UPPER(sa.displaytext) IN :entity_name_list
        AND ot.curie = :taxon
        """)

    elif entity_type in ['agms', 'strain', 'genotype', 'fish']:
        sql_query = text("""
        SELECT DISTINCT be.primaryexternalid, sa.obsolete, sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singleagm_id
        JOIN ontologyterm ot ON be.taxon_id = ot.id
        WHERE sa.slotannotationtype in (
            'AgmFullNameSlotAnnotation',
            'AgmSecondaryIdSlotAnnotation',
            'AgmSynonymSlotAnnotation'
        )
        AND UPPER(sa.displaytext) IN :entity_name_list
        AND ot.curie = :taxon
        """)
    elif 'targeting reagent' in entity_type:
        sql_query = text("""
        SELECT DISTINCT be.primaryexternalid, be.obsolete, str.name
        FROM biologicalentity be
        JOIN sequencetargetingreagent str ON be.id = str.id
        JOIN ontologyterm ot ON be.taxon_id = ot.id
        WHERE UPPER(str.name) IN :entity_name_list
        AND ot.curie = :taxon
        """)
    elif entity_type == 'construct':
        sql_query = text("""
        SELECT DISTINCT r.primaryexternalid, sa.obsolete, sa.displaytext
        FROM reagent r
        JOIN slotannotation sa ON r.id = sa.singleconstruct_id
        WHERE sa.slotannotationtype in (
            'ConstructFullNameSlotAnnotation',
            'ConstructSymbolSlotAnnotation'
        )
        AND UPPER(sa.displaytext) IN :entity_name_list
        """)
        rows = db.execute(sql_query, {'entity_name_list': tuple(entity_name_list)}).fetchall()
        return rows

    elif entity_type == 'species':
        sql_query = text("""
        SELECT DISTINCT curie, obsolete, name
        FROM ontologyterm
        WHERE name IN :entity_name_list
        OR curie IN :entity_name_list
        """)

    else:
        # Entity type not supported
        return None

    rows = db.execute(sql_query, {'entity_name_list': tuple(entity_name_list), 'taxon': taxon}).fetchall()
    return rows


def search_for_entity_curies(db: Session, entity_type, entity_curie_list):
    """Look up entities in the DB by their curies (MGI:4439460, SGD:S000063664, etc.)."""
    if len(entity_curie_list) == 0:
        return []

    if entity_type in ['gene', 'allele']:
        entity_table_name = entity_type
        sql_query = text(f"""
        SELECT DISTINCT be.primaryexternalid, be.obsolete, be.primaryexternalid
        FROM biologicalentity be, {entity_table_name} ent_tbl
        WHERE be.id = ent_tbl.id
        AND UPPER(be.primaryexternalid) IN :entity_curie_list
        """)

    elif entity_type == 'construct':
        sql_query = text("""
        SELECT DISTINCT r.primaryexternalid, r.obsolete, r.primaryexternalid
        FROM reagent r, construct c
        WHERE r.id = c.id
        AND UPPER(r.primaryexternalid) IN :entity_curie_list
        """)

    elif entity_type in ['agms', 'strain', 'genotype', 'fish']:
        sql_query = text("""
        SELECT DISTINCT be.primaryexternalid, be.obsolete, be.primaryexternalid
        FROM biologicalentity be, affectedgenomicmodel agm
        WHERE be.id = agm.id
        AND UPPER(be.primaryexternalid) IN :entity_curie_list
        """)
    elif 'targeting reagent' in entity_type:
        sql_query = text("""
        SELECT DISTINCT be.primaryexternalid, be.obsolete, be.primaryexternalid
        FROM biologicalentity be, sequencetargetingreagent str
        WHERE be.id = str.id
        AND UPPER(be.primaryexternalid) IN :entity_curie_list
        """)
    else:
        # Entity type not supported
        return None

    rows = db.execute(sql_query, {'entity_curie_list': tuple(entity_curie_list)}).fetchall()
    return rows


def search_topic(topic=None, mod_abbr=None):
    """Search ATP ontology for topics that match the given string."""
    db = create_ateam_db_session()
    if topic is not None:
        search_query = f"%{topic.upper()}%"
    if mod_abbr is not None and topic is not None:
        sql_query = text("""
            SELECT DISTINCT ot.curie, ot.name
            FROM ontologyterm ot
            JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
            JOIN ontologyterm ancestor ON ancestor.id = otc.closureobject_id
            JOIN ontologyterm_subsets s ON ot.id = s.ontologyterm_id
            WHERE ot.ontologytermtype = 'ATPTerm'
            AND UPPER(ot.name) LIKE :search_query
            AND ot.obsolete = false
            AND ancestor.curie = :topic_category_atp
            AND s.subsets = :mod_abbr
            LIMIT 10
            """)
        rows = db.execute(sql_query, {
            'search_query': search_query,
            'topic_category_atp': topic_category_atp,
            'mod_abbr': f'{mod_abbr}_tag'
        }).fetchall()
    elif topic is not None:
        sql_query = text("""
        SELECT DISTINCT ot.curie, ot.name
        FROM ontologyterm ot
        JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
        JOIN ontologyterm ancestor ON ancestor.id = otc.closureobject_id
        WHERE ot.ontologytermtype = 'ATPTerm'
        AND UPPER(ot.name) LIKE :search_query
        AND ot.obsolete = false
        AND ancestor.curie = :topic_category_atp
        LIMIT 10
        """)
        rows = db.execute(sql_query, {
            'search_query': search_query,
            'topic_category_atp': topic_category_atp
        }).fetchall()
    elif mod_abbr is not None:
        sql_query = text("""
            SELECT DISTINCT ot.curie, ot.name
            FROM ontologyterm ot
            JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
            JOIN ontologyterm ancestor ON ancestor.id = otc.closureobject_id
            JOIN ontologyterm_subsets s ON ot.id = s.ontologyterm_id
            WHERE ot.ontologytermtype = 'ATPTerm'
            AND ancestor.curie = :topic_category_atp
            AND s.subsets = :mod_abbr
        """)
        rows = db.execute(sql_query, {
            'topic_category_atp': topic_category_atp,
            'mod_abbr': f'{mod_abbr}_tag'
        }).fetchall()
        return [
            {
                "curie": row[0],
                "name": row[1]
            }
            for row in (rows or [])
        ]
    else:
        return []
    data = [
        {
            "curie": row[0],
            "name": row[1]
        }
        for row in (rows or [])
    ]
    db.close()
    json_data = jsonable_encoder(data)

    return JSONResponse(content=json_data)


def search_atp_descendants(ancestor_curie):
    atp_get_children_as_dict(ancestor_curie)
    db = create_ateam_db_session()
    sql_query = text("""
    SELECT DISTINCT ot.curie, ot.name
    FROM ontologyterm ot
    JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
    JOIN ontologyterm ancestor ON ancestor.id = otc.closureobject_id
    WHERE ot.ontologytermtype = 'ATPTerm'
    AND ot.obsolete = false
    AND ancestor.curie = :ancestor_curie
    """)
    rows = db.execute(sql_query, {
        'ancestor_curie': ancestor_curie
    }).fetchall()
    data = [
        {
            "curie": row[0],
            "name": row[1]
        }
        for row in (rows or [])
    ]
    db.close()
    json_data = jsonable_encoder(data)
    return JSONResponse(content=json_data)


def search_species(species):
    """Search for species in the NCBITaxonTerm ontology, matching either a curie or name."""
    db = create_ateam_db_session()

    if species.upper().startswith("NCBITAXON"):
        search_query = f"{species.upper()}%"
        sql_query = text("""
        SELECT curie, name
        FROM ontologyterm
        WHERE ontologytermtype = 'NCBITaxonTerm'
        AND UPPER(curie) LIKE :search_query
        LIMIT 10
        """)
    else:
        search_query = f"%{species.upper()}%"
        sql_query = text("""
        SELECT curie, name
        FROM ontologyterm
        WHERE ontologytermtype = 'NCBITaxonTerm'
        AND UPPER(name) LIKE :search_query
        LIMIT 10
        """)

    rows = db.execute(sql_query, {'search_query': search_query}).fetchall()
    data = [
        {
            "curie": row[0],
            "name": row[1]
        }
        for row in (rows or [])
    ]
    db.close()
    json_data = jsonable_encoder(data)
    return JSONResponse(content=json_data)


def search_ancestors_or_descendants(ontology_node, ancestors_or_descendants):
    """Return a list of ancestor or descendant curies for the given ontology_node."""
    # ATPs are already cached, so use that if applicable.
    if ontology_node.startswith("ATP:"):
        if ancestors_or_descendants == 'descendants':
            return atp_get_all_descendents(ontology_node)
        return atp_get_all_ancestors(ontology_node)

    db = create_ateam_db_session()
    if ancestors_or_descendants == 'descendants':
        sql_query = text("""
        SELECT DISTINCT ot.curie
        FROM ontologyterm ot
        JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
        JOIN ontologyterm ancestor ON ancestor.id = otc.closureobject_id
        WHERE ancestor.curie = :ontology_node
        AND ot.obsolete = False
        """)
    else:
        sql_query = text("""
        SELECT DISTINCT ot.curie
        FROM ontologyterm ot
        JOIN ontologytermclosure otc ON ot.id = otc.closuresubject_id
        JOIN ontologyterm descendant ON descendant.id = otc.closureobject_id
        WHERE descendant.curie = :ontology_node
        AND ot.obsolete = False
        """)

    rows = db.execute(sql_query, {'ontology_node': ontology_node}).fetchall()
    db.close()
    return [row[0] for row in (rows or [])]


def map_curies_to_names(category, curies):
    """
    Given a category (gene, allele, etc.) and a list of curies,
    return a dictionary mapping each curie to its preferred display name.
    """

    if not curies:
        return {}

    # If category is an ATP:xxxx ID, look up its name first
    if category.startswith('ATP:'):
        category_label = atp_get_name(category)
        if category_label is None:
            # If we can't find a label for the ATP category, just return identity mapping.
            return {curie: curie for curie in curies}
        category = category_label

    category = category.lower()
    if category in 'atpterm':
        return atp_to_name_subset(curies)

    db = create_ateam_db_session()
    if category == 'gene':
        sql_query = text("""
        SELECT be.primaryexternalid, sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singlegene_id
        WHERE be.primaryexternalid IN :curies
        AND sa.slotannotationtype = 'GeneSymbolSlotAnnotation'
        """)

    elif 'allele' in category:
        sql_query = text("""
        SELECT be.primaryexternalid, sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singleallele_id
        WHERE be.primaryexternalid IN :curies
        AND sa.slotannotationtype = 'AlleleSymbolSlotAnnotation'
        """)

    elif category in ['affected genome model', 'strain', 'genotype', 'fish']:
        sql_query = text("""
        SELECT DISTINCT be.primaryexternalid, sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singleagm_id
        WHERE be.primaryexternalid IN :curies
        """)

    elif 'construct' in category:
        sql_query = text("""
        SELECT r.primaryexternalid, sa.displaytext
        FROM reagent r
        JOIN slotannotation sa ON r.id = sa.singleconstruct_id
        WHERE r.primaryexternalid IN :curies
        AND sa.slotannotationtype = 'ConstructSymbolSlotAnnotation'
        """)

    elif category in ['species', 'ecoterm']:
        # Do an uppercase match
        curies = [curie.upper() for curie in curies]
        sql_query = text("""
        SELECT curie, name
        FROM ontologyterm
        WHERE UPPER(curie) IN :curies
        """)

    else:
        # If the category doesn't match a known table/relationship,
        # just map each curie to itself to avoid errors.
        db.close()
        return {curie: curie for curie in curies}

    rows = db.execute(sql_query, {'curies': tuple(curies)}).fetchall()
    curie_to_name_map = {row[0]: row[1] for row in rows}
    db.close()
    return curie_to_name_map


def set_globals(atp_to_name_init, name_to_atp_init, atp_to_children_init, atp_to_parent_init):
    global atp_to_name, name_to_atp, atp_to_children, atp_to_parent

    atp_to_name = atp_to_name_init.copy()
    name_to_atp = name_to_atp_init.copy()
    atp_to_children = atp_to_children_init.copy()
    atp_to_parent = atp_to_parent_init.copy()


@cachetools.func.ttl_cache(ttl=12 * 60 * 60)
def get_jobs_to_run(name: str, mod_abbreviation: str) -> list[str]:
    """
    Use the subsets in ontologyterm_subsets table to find the jobs to run.
    """
    if not atp_to_parent:
        load_name_to_atp_and_relationships()
    if name.startswith('ATP:'):
        atp_parent_id = name
    else:
        needed_string = f"{name} needed"
        if needed_string not in name_to_atp:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Exception: Could not find '{needed_string}' in ATP ontology names")
        atp_parent_id = name_to_atp[needed_string]

    # get list of all possible jobs.
    if name.startswith('ATP:'):
        jobs_list = [atp_parent_id]
    else:
        jobs_list = atp_to_children[atp_parent_id]

    mod_tag = f'{mod_abbreviation}_tag'
    # refine these to ones that are in the subset

    sql_query_str = """
      SELECT o.curie
      FROM ontologyterm o, ontologyterm_subsets s
      WHERE
         o.id = s.ontologyterm_id AND
         s.subsets = :mod_tag AND
         o.curie in :jobs_list
    """
    query_params = {}
    query_params['mod_tag'] = mod_tag
    query_params['jobs_list'] = tuple(jobs_list,)  # type: ignore

    db = create_ateam_db_session()
    rows = db.execute(text(sql_query_str).bindparams(**query_params)).fetchall()
    results = [atp_parent_id]
    for row in rows:
        results.append(row[0])
    return results


def load_name_to_atp_and_relationships(start_term='ATP:0000177'):
    """
    Add data to atp_to_name and name_to_atp dictionaries.
    From the top curie given go down all children and store the data.
    """
    global atp_to_name, name_to_atp, atp_to_children, atp_to_parent

    try:
        db = create_ateam_db_session()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Exception {e} Setting connection of ateam db")
    # Load atp data
    id_to_curie = {}
    sql_query = text("""
    SELECT DISTINCT o.curie as curie, o.name as name, o.obsolete as obsolete, o.id as id, otc.closuresubject_id as child
      FROM ontologyterm o
      LEFT JOIN ontologytermclosure otc ON o.id = otc.closureobject_id
         WHERE o.ontologytermtype = 'ATPTerm'
    """)
    try:
        rows = db.execute(sql_query).fetchall()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Exception {e} Doing query of ateam db")
    db.close()

    # need ALL id_to_curies loaded before we do child/parents
    for row in rows:
        if row.obsolete:
            continue
        id_to_curie[row.id] = row.curie
        name_to_atp[row.name] = row.curie
        atp_to_name[row.curie] = row.name

    # Load the relationships
    # Store all first then get the subset required
    for row in rows:
        if row.obsolete:
            continue
        if row.child:
            child_curie = id_to_curie[row.child]
        else:
            child_curie = None
        parent_curie = id_to_curie[row.id]
        if child_curie:
            if parent_curie in atp_to_children:
                atp_to_children[parent_curie].append(child_curie)
            else:
                atp_to_children[parent_curie] = [child_curie]
    if start_term:
        parent_list = [start_term]
        while parent_list:
            parent = parent_list.pop()
            if parent in atp_to_children:
                for child in atp_to_children[parent]:
                    parent_list.append(child)
                    atp_to_parent[child] = parent

    logger.debug("ATP global vars successfully loaded")
    return


def atp_get_parent(child_id):
    if not atp_to_parent:
        load_name_to_atp_and_relationships()
    if child_id in atp_to_parent:
        return atp_to_parent[child_id]
    else:
        return []


def atp_get_children(parent_id):
    if not atp_to_children:
        load_name_to_atp_and_relationships()
    if parent_id in atp_to_children:
        return atp_to_children[parent_id]
    else:
        return []


def atp_get_children_as_dict(parent_id):
    children = atp_get_children(parent_id)
    result = []
    for atp_id in children:
        result.append({'curie': atp_id, 'name': atp_to_name[atp_id]})
    return result


def atp_to_name_subset(curies: list):
    if not atp_to_name:
        load_name_to_atp_and_relationships()
    subset = {}
    for curie in curies:
        subset[curie] = atp_to_name[curie]
    return subset


@cachetools.func.ttl_cache(ttl=24 * 60 * 60)
def atp_get_all_descendents(curie: str) -> list:
    try:
        return list(get_name_to_atp_for_all_children(curie)[1].keys())
    except IndexError:
        return []


def atp_get_all_ancestors(curie: str):
    parent_list = []
    not_seen = [curie]
    while len(not_seen) > 0:
        p = not_seen.pop(0)
        if p in atp_to_parent:
            parent = atp_to_parent[p]
            parent_list.append(parent)
            not_seen.append(parent)
    return parent_list


def get_name_to_atp_for_all_children(workflow_parent):
    """
    Get ALL descendents for an ATP.
    as a dictionary of name to atp curies and
    curies to names..
    """
    # global atp_to_name, atp_to_children

    if not atp_to_name:
        load_name_to_atp_and_relationships()

    subset_name_to_atp = {}
    subset_atp_to_name = {}
    list_ids = []
    if workflow_parent in atp_to_children:
        # NOTE:
        # list_ids = atp_get_children(workflow_parent)
        # Gives address and not contents!
        list_ids.extend(atp_get_children(workflow_parent))
    else:
        return []

    while len(list_ids) > 0:
        curie = list_ids.pop()

        if not curie:
            continue
        subset_atp_to_name[curie] = atp_to_name[curie]
        subset_name_to_atp[atp_to_name[curie]] = curie
        if curie in atp_to_children:
            list_ids.extend(atp_get_children(curie))
    return subset_name_to_atp, subset_atp_to_name


def atp_get_name(atp_id):
    if not atp_to_name:
        try:
            load_name_to_atp_and_relationships()
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"{e}")
    if atp_id in atp_to_name:
        return atp_to_name[atp_id]
    return None


def atp_return_invalid_ids(atp_ids: list):
    if not atp_to_name:
        load_name_to_atp_and_relationships()
    invalid_atps = []
    for atp_id in atp_ids:
        if atp_id not in atp_to_name:
            invalid_atps.append(atp_id)
    return invalid_atps
