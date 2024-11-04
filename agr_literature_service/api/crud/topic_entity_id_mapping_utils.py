from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from fastapi import HTTPException, status

# TODO 1: get this list from database
curie_prefix_list = [
    "FB",
    "MGI",
    "RGD",
    "SGD",
    "WB",
    "XenBase",
    "ZFIN"
]

# TODO 2: adding functions for getting/loading ATP parent/children tree/terms
# TODO 3: adding caching for name to id mapping


def map_entity_to_curie(db: Session, entity_type, entity_list, taxon):

    entity_type = entity_type.lower()
    (entity_name_list, entity_curie_list) = classify_entity_list(entity_list)

    entity_curie_rows = search_for_entity_curies(db, entity_type, entity_curie_list)
    entity_name_rows = search_for_entity_names(db, entity_type, entity_name_list, taxon)

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
        # gene symbol: ACT1
        # systematic name: YFL039C
        # genomic feature: CEN1
        sql_query = text("""
        SELECT distinct be.modentityid, sa.obsolete, sa.displaytext
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
        SELECT DISTINCT be.modentityid, sa.obsolete, sa.displaytext
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
        # SELECT DISTINCT be.modentityid, sa.obsolete, sa.displaytext
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
        SELECT DISTINCT be.modentityid, be.obsolete, agm.name
        FROM biologicalentity be
        JOIN affectedgenomicmodel agm ON be.id = agm.id
        JOIN ontologyterm ot ON be.taxon_id = ot.id
        WHERE UPPER(agm.name) IN :entity_name_list
        AND ot.curie = :taxon
        """)
    elif entity_type == 'construct':
        sql_query = text("""
        SELECT DISTINCT r.modentityid, sa.obsolete, sa.displaytext
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
        SELECT DISTINCT be.modentityid, be.obsolete, be.modentityid
        FROM biologicalentity be, {entity_table_name} ent_tbl
        WHERE be.id = ent_tbl.id
        AND UPPER(be.modentityid) IN :entity_curie_list
        """)
    elif entity_type == 'construct':
        sql_query = text("""
        SELECT DISTINCT r.modentityid, r.obsolete, r.modentityid
        FROM reagent r, construct c
        WHERE r.id = c.id
        AND UPPER(r.modentityid) IN :entity_curie_list
        """)
    elif entity_type in ['agms', 'strain', 'genotype', 'fish']:
        sql_query = text("""
        SELECT DISTINCT be.modentityid, be.obsolete, be.modentityid
        FROM biologicalentity be, affectedgenomicmodel agm
        WHERE be.id = agm.id
        AND UPPER(be.modentityid) IN :entity_curie_list
        """)
    else:
        return None
    rows = db.execute(sql_query, {'entity_curie_list': tuple(entity_curie_list)}).fetchall()
    return rows


def search_topic(db: Session, topic):
    # ATP:0000002 (topic tag)
    search_query = f"%{topic.upper()}%"
    sql_query = text("""
    SELECT ot.curie, ot.name
    FROM ontologyterm ot
    JOIN ontologyterm_isa_ancestor_descendant oad ON ot.id = oad.isadescendants_id
    JOIN ontologyterm ancestor ON ancestor.id = oad.isaancestors_id
    WHERE ot.ontologytermtype = 'ATPTerm'
    AND UPPER(ot.name) LIKE :search_query
    AND ot.obsolete = false
    AND ancestor.curie = 'ATP:0000002'
    ORDER BY LENGTH(ot.name)
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
    json_data = jsonable_encoder(data)
    return JSONResponse(content=json_data)


def search_species(db: Session, species):

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


def map_curies_to_names(db: Session, category, curies):

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
        SELECT be.modentityid, sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singlegene_id
        WHERE be.modentityid IN :curies
        AND sa.slotannotationtype = 'GeneSymbolSlotAnnotation'
        """)
    elif 'allele' in category:
        sql_query = text("""
        SELECT be.modentityid, sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singleallele_id
        WHERE be.modentityid IN :curies
        AND sa.slotannotationtype = 'AlleleSymbolSlotAnnotation'
        """)
    elif category in ['affected genome model', 'strain', 'genotype', 'fish']:
        sql_query = text("""
        SELECT DISTINCT be.modentityid, agm.name
        FROM biologicalentity be
        JOIN affectedgenomicmodel agm ON be.id = agm.id
        WHERE be.modentityid IN :curies
        """)
    elif 'construct' in category:
        sql_query = text("""
        SELECT r.modentityid, sa.displaytext
        FROM reagent r
        JOIN slotannotation sa ON r.id = sa.singleconstruct_id
        WHERE r.modentityid IN :curies
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
    return curie_to_name_map
