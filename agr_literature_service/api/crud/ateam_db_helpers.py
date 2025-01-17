from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from os import environ
from sqlalchemy import text
from fastapi import HTTPException, status
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

curie_prefix_list = ["FB", "MGI", "RGD", "SGD", "WB", "XenBase", "ZFIN"]
topic_category_atp = "ATP:0000002"  # topic tag


def create_postgres_session():

    USER = environ.get('PERSISTENT_STORE_DB_USERNAME', 'unknown')
    PASSWORD = environ.get('PERSISTENT_STORE_DB_PASSWORD', 'unknown')
    SERVER = environ.get('PERSISTENT_STORE_DB_HOST', 'localhost')
    PORT = environ.get('PERSISTENT_STORE_DB_PORT', '5432')
    DB = environ.get('PERSISTENT_STORE_DB_NAME', 'unknown')
    engine_var = 'postgresql://' + USER + ":" + PASSWORD + '@' + SERVER + ':' + PORT + '/' + DB
    engine = create_engine(engine_var)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = Session()
    return session


def map_entity_to_curie(entity_type, entity_list, taxon):

    db = create_postgres_session()
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

    entity_name_list = []
    entity_curie_list = []
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
        SELECT distinct be.primaryexternalid, sa.obsolete, sa.displaytext
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
        # for 'allele' and 'transgenic allele'
        sql_query = text("""
        SELECT DISTINCT be.primaryexternalid, sa.obsolete, sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singleallele_id
        JOIN ontologyterm ot ON be.taxon_id = ot.id
        WHERE sa.slotannotationtype = 'AlleleSymbolSlotAnnotation'
        AND UPPER(sa.displaytext) IN :entity_name_list
        AND ot.curie = :taxon
        """)
        ## using subquery doesn't help in this case
        ## exection time is similar
        # sql_query = text("""
        # SELECT DISTINCT be.primaryexternalid, sa.obsolete, sa.displaytext
        # FROM biologicalentity be
        # JOIN (
        #    SELECT singleallele_id, obsolete, displaytext
        #    FROM slotannotation
        #    WHERE slotannotationtype = 'AlleleSymbolSlotAnnotation'
        #    AND UPPER(displaytext) IN :exntity_name_list
        # ) AS sa ON be.id = sa.singleallele_id;
        # """)
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
    elif entity_type == 'species':
        sql_query = text("""
        SELECT DISTINCT curie, obsolete, name
        FROM ontologyterm
        WHERE name IN :entity_name_list
        OR curie in :entity_name_list
        """)
    else:
        return None
    rows = db.execute(sql_query, {'entity_name_list': tuple(entity_name_list), 'taxon': taxon}).fetchall()
    return rows


def search_for_entity_curies(db: Session, entity_type, entity_curie_list):
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
        return None
    rows = db.execute(sql_query, {'entity_curie_list': tuple(entity_curie_list)}).fetchall()
    return rows


def search_topic(topic):

    db = create_postgres_session()
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


def search_species(species):

    db = create_postgres_session()
    sql_query = None
    search_query = None
    if species.upper().startswith("NCBITAXON"):
        search_query = f"{species.upper()}%"
        sql_query = text("""
        SELECT curie, name
        FROM ontologyterm
        WHERE ontologytermtype = 'NCBITaxonTerm'
        AND UPPER(curie) like :search_query
        LIMIT 10
        """)
    else:
        search_query = f"%{species.upper()}%"
        sql_query = text("""
        SELECT curie, name
        FROM ontologyterm
        WHERE ontologytermtype = 'NCBITaxonTerm'
        AND UPPER(name) like :search_query
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

    sql_query = text("""
    SELECT name
    FROM ontologyterm
    WHERE ontologytermtype = 'ATPTerm'
    AND curie = :atp_id
    """)
    row = db.execute(sql_query, {'atp_id', atp_id}).fetchone()
    if row:
        return row[0]
    return None


def search_atp_ontology():

    db = create_postgres_session()
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

    db = create_postgres_session()
    sql_query = None
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

    db = create_postgres_session()
    if not curies:
        return {}

    if category.startswith('ATP:'):
        category = map_atp_id_to_name(db, category)
        if category is None:
            return {curie: curie for curie in curies}
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
        curies = [curie.upper() for curie in curies]
        sql_query = text("""
        SELECT curie, name
        FROM ontologyterm
        WHERE UPPER(curie) IN :curies
        """)
    else:
        return {curie: curie for curie in curies}
    rows = db.execute(sql_query, {'curies': tuple(curies)}).fetchall()
    curie_to_name_map = {row[0]: row[1] for row in rows}
    db.close()
    return curie_to_name_map
