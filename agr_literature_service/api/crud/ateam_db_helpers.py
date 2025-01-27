from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from os import environ
from sqlalchemy import text
from fastapi import HTTPException, status
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# List of valid prefix identifiers for curies
curie_prefix_list = ["FB", "MGI", "RGD", "SGD", "WB", "XenBase", "ZFIN"]

# Topic tag for ATP ontology
topic_category_atp = "ATP:0000002"


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
    for entity in entity_list.replace("+", " ").split("|"):
        is_mod_curie = False
        for curie_prefix in curie_prefix_list:
            if entity.startswith(curie_prefix + ":"):
                is_mod_curie = True
                break
        if is_mod_curie:
            entity_curie_list.append(entity.upper())
        else:
            entity_name_list.append(entity.upper())

    return (entity_name_list, entity_curie_list)


def search_for_entity_names(db: Session, entity_type, entity_name_list, taxon):
    """Look up entities in the DB by name (gene symbol, allele symbol, etc.), restricted by taxon."""
    if len(entity_name_list) == 0:
        return []

    sql_query = None

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
        SELECT DISTINCT be.primaryexternalid, be.obsolete, agm.name
        FROM biologicalentity be
        JOIN affectedgenomicmodel agm ON be.id = agm.id
        JOIN ontologyterm ot ON be.taxon_id = ot.id
        WHERE UPPER(agm.name) IN :entity_name_list
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
        print("HELLO construct name search: rows[0][0]=", rows[0][0], ", rows[0][2]=", rows[0][2])
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

    sql_query = None

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

    else:
        # Entity type not supported
        return None

    rows = db.execute(sql_query, {'entity_curie_list': tuple(entity_curie_list)}).fetchall()
    return rows


def search_topic(topic):
    """Search ATP ontology for topics that match the given string."""
    db = create_ateam_db_session()
    search_query = f"%{topic.upper()}%"
    sql_query = text("""
    SELECT ot.curie, ot.name
    FROM ontologyterm ot
    JOIN ontologyterm_isa_ancestor_descendant oad ON ot.id = oad.isadescendants_id
    JOIN ontologyterm ancestor ON ancestor.id = oad.isaancestors_id
    WHERE ot.ontologytermtype = 'ATPTerm'
    AND UPPER(ot.name) LIKE :search_query
    AND ot.obsolete = false
    AND ancestor.curie = :topic_category_atp
    ORDER BY LENGTH(ot.name)
    LIMIT 10
    """)

    rows = db.execute(sql_query, {
        'search_query': search_query,
        'topic_category_atp': topic_category_atp
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


def search_atp_descendants(ancestor_curie):
    db = create_ateam_db_session()
    sql_query = text("""
    SELECT ot.curie, ot.name
    FROM ontologyterm ot
    JOIN ontologyterm_isa_ancestor_descendant oad ON ot.id = oad.isadescendants_id
    JOIN ontologyterm ancestor ON ancestor.id = oad.isaancestors_id
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
    search_query = None

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


def map_atp_id_to_name(db: Session, atp_id):
    """
    Given an ATPTerm curie (e.g. "ATP:0001234"), return the corresponding name.
    """
    sql_query = text("""
    SELECT name
    FROM ontologyterm
    WHERE ontologytermtype = 'ATPTerm'
    AND curie = :atp_id
    """)
    row = db.execute(sql_query, {'atp_id': atp_id}).fetchone()
    if row:
        return row[0]
    return None


def search_atp_ontology():
    """Return all ATPTerms from the ontologyterm table."""
    db = create_ateam_db_session()
    sql_query = text("""
    SELECT curie, name, obsolete
    FROM ontologyterm
    WHERE ontologytermtype = 'ATPTerm'
    """)
    rows = db.execute(sql_query).fetchall()

    result = [
        {"curie": row.curie, "name": row.name, "obsolete": row.obsolete}
        for row in rows
    ]
    db.close()
    return result


def search_ancestors_or_descendants(ontology_node, ancestors_or_descendants):
    """Return a list of ancestor or descendant curies for the given ontology_node."""
    db = create_ateam_db_session()

    if ancestors_or_descendants == 'descendants':
        sql_query = text("""
        SELECT ot.curie
        FROM ontologyterm ot
        JOIN ontologyterm_isa_ancestor_descendant oad ON ot.id = oad.isadescendants_id
        JOIN ontologyterm ancestor ON ancestor.id = oad.isaancestors_id
        WHERE ancestor.curie = :ontology_node
        AND ot.obsolete = False
        """)
    else:
        sql_query = text("""
        SELECT ot.curie
        FROM ontologyterm ot
        JOIN ontologyterm_isa_ancestor_descendant oad ON ot.id = oad.isaancestors_id
        JOIN ontologyterm descendant ON descendant.id = oad.isadescendants_id
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
    db = create_ateam_db_session()
    if not curies:
        return {}

    # If category is an ATP:xxxx ID, look up its name first
    if category.startswith('ATP:'):
        category_label = map_atp_id_to_name(db, category)
        if category_label is None:
            # If we can't find a label for the ATP category, just return identity mapping.
            return {curie: curie for curie in curies}
        category = category_label

    category = category.lower()
    sql_query = None

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
        SELECT DISTINCT be.primaryexternalid, agm.name
        FROM biologicalentity be
        JOIN affectedgenomicmodel agm ON be.id = agm.id
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

    elif category in ['species', 'atpterm', 'ecoterm']:
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

def get_name_to_atp_and_children(curie):
    """
    Add data to atp_to_name and name_to_atp dictionaries.
    From the top curie given go down all children and store the data.
    """
    # To minimise db calls grab all atp data and then process
    db = create_ateam_db_session()
    print("BOB: get data form a team")
    sql_query = text("""
    SELECT o.curie as curie, o.name as name, o.obsolete as obsolete, o.id as id, opc.isachildren_id as child
      FROM ontologyterm o
      LEFT JOIN ontologyterm_isa_parent_children opc ON o.id = opc.isaparents_id
         WHERE o.ontologytermtype = 'ATPTerm'
    """)
    rows = db.execute(sql_query).fetchall()
    curie_to_id = {}
    id_to_curie = {}
    full_name_to_atp = {}  # list of all atp, which include extras
    full_atp_to_name = {}
    children = {}
    for row in rows:
        if row.obsolete:
            continue
        print(f"BOB: row data: {row}")
        curie_to_id[row.curie] = row.id
        id_to_curie[row.id] = row.curie
        full_name_to_atp[row.name] = row.curie
        full_atp_to_name[row.curie] = row.name
        if row.id in children:
            children[row.id].append(row.child)
        else:
            children[row.id] = [row.child]
    db.close()
    print("BOB: process data form ateam")
    # Now filter down to the ones we want.
    name_to_atp = {}
    atp_to_name = {}
    id_list = [curie_to_id[curie]]
    while len(id_list):
        atp_id = id_list.pop()
        if not atp_id:
            continue
        if atp_id in children and children[atp_id] is not None:
            id_list.extend(children[atp_id])
        # print(f"BOB: id: {atp_id} list: {id_list}")
        atp_curie = id_to_curie[atp_id]
        name_to_atp[full_atp_to_name[atp_curie]] = atp_curie
        atp_to_name[atp_curie] = full_atp_to_name[atp_curie]
    return name_to_atp, atp_to_name