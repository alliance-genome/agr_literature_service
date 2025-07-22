import logging
from datetime import date
from sqlalchemy import text
from os import environ, path
from shutil import copy
from collections import defaultdict

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.crud.ateam_db_helpers import create_ateam_db_session, \
    atp_get_name, search_for_entity_curies
from agr_literature_service.api.crud.topic_entity_tag_utils import get_map_entity_curies_to_names
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def check_data():

    db = create_postgres_session(False)
    ateam_db = create_ateam_db_session()

    data_to_report = []
    try:
        for mod_abbreviation in get_mod_abbreviations():
            entity_type_to_mod_entity_ids = get_unique_entity_list(db, mod_abbreviation)
            for entity_type, entity_id_curies_set in entity_type_to_mod_entity_ids.items():
                mod_entity_ids = [eid for eid, _ in entity_id_curies_set]
                agrkbs = {eid: curies for eid, curies in entity_id_curies_set}
                entity_type_name = atp_get_name(entity_type)
                entity_type_name = entity_type_name.replace("transgenic ", "")
                logger.info(f"Checking {mod_abbreviation} obsolete {entity_type_name}:")
                if mod_abbreviation == 'SGD' and (
                        'complex' in entity_type_name or 'pathway' in entity_type_name
                ):
                    deleted_id_set, obsolete_id_set, obsolete_id_to_name = check_sgd_database(
                        db,
                        entity_type_name,
                        list(mod_entity_ids)
                    )
                else:
                    deleted_id_set, obsolete_id_set, obsolete_id_to_name = check_ateam_database(
                        ateam_db,
                        entity_type_name,
                        list(mod_entity_ids)
                    )
                add_missing_name_to_obsolete_ids(
                    ateam_db,
                    entity_type_name,
                    obsolete_id_set,
                    obsolete_id_to_name
                )
                data_to_report.append(
                    (
                        mod_abbreviation,
                        entity_type_name,
                        deleted_id_set,
                        obsolete_id_set,
                        obsolete_id_to_name,
                        agrkbs
                    )
                )
    except Exception as e:
        logger.info(f"An error occurred when getting the data for deleted/obsolete entities. Error={e}")
        db.close()
        ateam_db.close()
        return
    db.close()
    ateam_db.close()
    write_report(data_to_report)


def write_report(data_to_report):

    log_path = environ.get('LOG_PATH', '.')
    log_file = path.join(log_path, "QC/obsolete_entity_report.log")
    datestamp = str(date.today()).replace("-", "")
    log_file_with_datestamp = path.join(log_path, f"QC/obsolete_entity_report_{datestamp}.log")
    with open(log_file, "w") as f:
        f.write(f"#!date-produced: {datestamp}\n")
        for mod_abbreviation, entity_type_name, deleted_id_set, obsolete_id_set, obsolete_id_to_name, agrkbs in data_to_report:
            for curie in deleted_id_set:
                references = agrkbs.get(curie, '')
                f.write(f"{mod_abbreviation}\t{entity_type_name}\tDeleted\t{curie}\t\t{references}\n")
            for curie in obsolete_id_set:
                obsolete_name = obsolete_id_to_name.get(curie, '')
                references = agrkbs.get(curie, '')
                f.write(f"{mod_abbreviation}\t{entity_type_name}\tObsolete\t{curie}\t{obsolete_name}\t{references}\n")
    copy(log_file, log_file_with_datestamp)


def add_missing_name_to_obsolete_ids(ateam_db, entity_type_name, obsolete_id_set, obsolete_id_to_name):

    for obsolete_id in obsolete_id_set:
        if obsolete_id in obsolete_id_to_name:
            continue
        name = get_name_for_curie(obsolete_id)
        if name:
            obsolete_id_to_name[obsolete_id] = name


def get_name_for_curie(ateam_db, entity_type_name, curie):

    if entity_type_name == 'gene':
        query = text("""
        SELECT sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singlegene_id
        WHERE sa.slotannotationtype in (
            'GeneSymbolSlotAnnotation',
            'GeneSystematicNameSlotAnnotation',
            'GeneFullNameSlotAnnotation'
        )
        AND be.primaryexternalid = :curie
        AND sa.obsolete = True
        """)

    elif entity_type_name == 'allele':
        query = text("""
        SELECT sa.displaytext
        FROM biologicalentity be
        JOIN slotannotation sa ON be.id = sa.singleallele_id
        WHERE sa.slotannotationtype = 'AlleleSymbolSlotAnnotation'
        AND be.primaryexternalid = :curie
        AND sa.obsolete = True
        """)

    elif entity_type_name in ['agms', 'strain', 'genotype', 'fish']:
        query = text("""
        SELECT agm.name
        FROM biologicalentity be
        JOIN affectedgenomicmodel agm ON be.id = agm.id
        WHERE be.primaryexternalid = :curie
        AND sa.obsolete = True
        """)

    elif entity_type_name == 'construct':
        query = text("""
        SELECT sa.displaytext
        FROM reagent r
        JOIN slotannotation sa ON r.id = sa.singleconstruct_id
        WHERE sa.slotannotationtype in (
            'ConstructFullNameSlotAnnotation',
            'ConstructSymbolSlotAnnotation'
        )
        AND be.primaryexternalid = :curie
        AND sa.obsolete = True
        """)
    else:
        return None
    rows = ateam_db.execute(query, {'curie': curie}).fetchall()
    if len(rows) > 1:
        return rows[0][0]
    return None


def check_sgd_database(db, entity_type_name, mod_entity_ids, batch_size=100):

    id_to_name_mapping = {}
    for i in range(0, len(mod_entity_ids), batch_size):
        batch = mod_entity_ids[i:i + batch_size]
        batch_mapping = get_map_entity_curies_to_names(db, 'sgd', entity_type_name, batch)
        id_to_name_mapping.update(batch_mapping)
    deleted_ids = {mod_entity_id for mod_entity_id in mod_entity_ids if mod_entity_id not in id_to_name_mapping}
    return deleted_ids, set(), {}


def search_species(ateam_db, species_list):

    query = text("""
        SELECT curie, obsolete, name
        FROM ontologyterm
        WHERE ontologytermtype = 'NCBITaxonTerm'
        AND curie IN :species_list
    """)
    rows = ateam_db.execute(query, {'species_list': tuple(species_list)}).fetchall()
    return rows


def check_ateam_database(ateam_db, entity_type_name, mod_entity_ids, batch_size=100):

    valid_ids = set()
    obsolete_ids = set()
    obsolete_id_to_name = {}

    for i in range(0, len(mod_entity_ids), batch_size):
        batch = mod_entity_ids[i:i + batch_size]
        if entity_type_name == 'species':
            rows = search_species(ateam_db, batch) or []
        else:
            rows = search_for_entity_curies(ateam_db, entity_type_name, batch) or []
        for mod_entity_id, is_obsolete, name in rows:
            if is_obsolete:
                obsolete_ids.add(mod_entity_id)
                if name != mod_entity_id:
                    obsolete_id_to_name[mod_entity_id] = name
            else:
                valid_ids.add(mod_entity_id)

    deleted_ids = {mod_entity_id for mod_entity_id in mod_entity_ids
                   if mod_entity_id not in valid_ids and mod_entity_id not in obsolete_ids}

    return deleted_ids, obsolete_ids, obsolete_id_to_name


def get_unique_entity_list(db, mod_abbreviation):
    query = text("""
        SELECT
            tet.entity_type,
            tet.entity,
            string_agg(ref.curie, ', ') AS reference_curies
        FROM
            topic_entity_tag tet
        JOIN
            topic_entity_tag_source tet_src
            ON tet.topic_entity_tag_source_id = tet_src.topic_entity_tag_source_id
        JOIN
            reference ref
            ON tet.reference_id = ref.reference_id
        WHERE
            tet.entity IS NOT NULL
            AND tet_src.data_provider = :mod_abbreviation
        GROUP BY
            tet.entity_type, tet.entity;
    """)
    params = {"mod_abbreviation": mod_abbreviation}
    rows = db.execute(query, params).fetchall()

    entity_type_to_mod_entity_ids = defaultdict(set)
    for entity_type, entity_mod_id, agrkbs in rows:
        entity_type_to_mod_entity_ids[entity_type].add((entity_mod_id, agrkbs))

    return entity_type_to_mod_entity_ids


if __name__ == "__main__":

    check_data()
