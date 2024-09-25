import argparse
import logging
import json
from os import listdir, path
from typing import List, Optional, Dict, Tuple

from sqlalchemy.exc import SQLAlchemyError
from agr_literature_service.api.crud.mod_reference_type_crud import insert_mod_reference_type_into_db
from agr_literature_service.api.models import (
    CrossReferenceModel,
    ReferenceModel,
    AuthorModel,
    ModCorpusAssociationModel,
    ModModel,
    ReferenceRelationModel,
    MeshDetailModel,
)
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import (
    get_journal_data,
    get_doi_data,
    get_reference_by_pmid,
)
# from agr_literature_service.api.crud.reference_crud import get_citation_from_args
from agr_literature_service.global_utils import get_next_reference_curie
from agr_literature_service.lit_processing.data_ingest.utils.date_utils import parse_date
from agr_literature_service.api.crud.utils.patterns_check import check_pattern

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def post_references(json_path: str, live_change: bool = True) -> List[str]:
    """
    Process JSON files containing reference data and insert them into the database.

    Args:
        json_path (str): Path to the JSON file or directory containing JSON files.
        live_change (bool): If True, commit changes to the database; otherwise, roll back.

    Returns:
        List[str]: List of new reference CURIEs added to the database.
    """
    db_session = create_postgres_session(False)

    files_to_process = []
    if path.isdir(json_path):
        for filename in listdir(json_path):
            if 'REFERENCE_' in filename and '.REFERENCE_' not in filename:
                files_to_process.append(path.join(json_path, filename))
    else:
        files_to_process.append(json_path)

    log.info("Getting journal info from database...")
    journal_to_resource_id = get_journal_data(db_session)

    log.info("Getting DOI info from database...")
    doi_to_reference_id = get_doi_data(db_session)

    log.info("Getting mod info from database...")
    mod_to_mod_id = {x.abbreviation: x.mod_id for x in db_session.query(ModModel).all()}

    log.info("Reading JSON data and loading data into database...")

    new_ref_curies = []
    for json_file in sorted(files_to_process):
        if not path.exists(json_file):
            continue
        with open(json_file, 'r') as f:
            json_data = json.load(f)
        newly_added_curies = read_data_and_load_references(
            db_session,
            json_data,
            journal_to_resource_id,
            doi_to_reference_id,
            mod_to_mod_id,
            live_change,
        )
        if newly_added_curies:
            new_ref_curies.extend(newly_added_curies)
    db_session.commit()
    db_session.close()
    log.info("DONE!\n\n")
    return new_ref_curies


def read_data_and_load_references(
    db_session,
    json_data: List[Dict],
    journal_to_resource_id: Dict[str, Tuple[int, str]],
    doi_to_reference_id: Dict[str, int],
    mod_to_mod_id: Dict[str, int],
    live_change: bool,
) -> List[str]:
    new_ref_curies = []
    for entry in json_data:

        primaryId = set_primaryId(entry)
        crossRef = db_session.query(CrossReferenceModel).filter_by(
            curie=primaryId, is_obsolete=False
        ).one_or_none()
        if crossRef:
            continue

        try:
            if entry.get('crossReferences') is None:
                continue

            reference_id, curie = insert_reference(
                db_session, primaryId, journal_to_resource_id, entry
            )

            if reference_id is None:
                log.info(f"{primaryId}: Error loading reference table")
                continue

            log.info(f"{primaryId}: reference_id = {reference_id}")

            foundXREF = insert_cross_references(
                db_session,
                primaryId,
                reference_id,
                doi_to_reference_id,
                entry['crossReferences'],
            )
            if not foundXREF:
                db_session.rollback()
                continue

            if entry.get('authors'):
                insert_authors(db_session, primaryId, reference_id, entry['authors'])

            if entry.get('meshTerms'):
                insert_mesh_terms(db_session, primaryId, reference_id, entry['meshTerms'])

            if entry.get('commentsCorrections'):
                insert_reference_relations(
                    db_session, primaryId, reference_id, entry['commentsCorrections']
                )

            if entry.get('MODReferenceTypes'):
                insert_mod_reference_types(
                    db_session,
                    primaryId,
                    reference_id,
                    entry['MODReferenceTypes'],
                    entry.get('pubmedType', []),
                )

            if entry.get('modCorpusAssociations'):
                insert_mod_corpus_associations(
                    db_session,
                    primaryId,
                    reference_id,
                    mod_to_mod_id,
                    entry['modCorpusAssociations'],
                )

            log.info(
                f"The new reference for primaryId = {primaryId} has been added into database"
            )
            if live_change:
                db_session.commit()
            else:
                db_session.rollback()
            new_ref_curies.append(curie)
        except (KeyError, ValueError, TypeError) as e:
            log.error(
                f"Data error when processing primaryId = {primaryId}: {e}"
            )
            db_session.rollback()
        except SQLAlchemyError as e:
            log.error(
                f"Database error when adding reference for primaryId = {primaryId}: {e}"
            )
            db_session.rollback()
    return new_ref_curies


def insert_mod_corpus_associations(
    db_session,
    primaryId: str,
    reference_id: int,
    mod_to_mod_id: Dict[str, int],
    mod_corpus_associations_from_json: List[Dict],
):
    for x in mod_corpus_associations_from_json:
        try:
            mod_abbreviation = x.get('modAbbreviation')
            mod_id = mod_to_mod_id.get(mod_abbreviation)
            if mod_id is None:
                log.info(
                    f"The 'modAbbreviation' is missing or invalid in the JSON data for primaryId = {primaryId}"
                )
                continue
            mca = ModCorpusAssociationModel(
                reference_id=reference_id,
                mod_id=mod_id,
                mod_corpus_sort_source=x['modCorpusSortSource'],
                corpus=x['corpus'],
            )
            db_session.add(mca)
            log.info(
                f"{primaryId}: INSERT MOD_CORPUS_ASSOCIATION: for reference_id = {reference_id}, mod_id = {mod_id}, mod_corpus_sort_source = {x['modCorpusSortSource']}"
            )
        except KeyError as e:
            log.error(
                f"{primaryId}: Missing key {e} in mod corpus association data"
            )
        except SQLAlchemyError as e:
            log.error(
                f"{primaryId}: Database error when inserting MOD_CORPUS_ASSOCIATION: {e}"
            )
            db_session.rollback()


def insert_mod_reference_types(
    db_session,
    primaryId: str,
    reference_id: int,
    mod_ref_types_from_json: List[Dict],
    pubmed_types: List[str],
):
    # Check if "Meeting_abstract" is in the referenceType list
    meeting_abstract_present = any(
        x['referenceType'] == 'Meeting_abstract' for x in mod_ref_types_from_json
    )
    found = {}
    for x in mod_ref_types_from_json:
        key = (reference_id, x['source'], x['referenceType'])
        if key in found:
            continue
        found[key] = 1
        # Skip insertion if "Meeting_abstract" is present and referenceType is "Experimental" or "Not_experimental"
        if meeting_abstract_present and x['referenceType'] in [
            "Experimental",
            "Not_experimental",
        ]:
            log.info(
                f"{primaryId}: SKIP MOD_REFERENCE_TYPE: for reference_id = {reference_id}, source = {x['source']}, reference_type = {x['referenceType']} due to presence of 'Meeting_abstract'"
            )
            continue
        try:
            insert_mod_reference_type_into_db(
                db_session, pubmed_types, x['source'], x['referenceType'], reference_id
            )
            log.info(
                f"{primaryId}: INSERT MOD_REFERENCE_TYPE: for reference_id = {reference_id}, source = {x['source']}, reference_type = {x['referenceType']}"
            )
        except KeyError as e:
            log.error(
                f"{primaryId}: Missing key {e} in MOD reference type data"
            )
        except SQLAlchemyError as e:
            log.error(
                f"{primaryId}: Database error when inserting MOD_REFERENCE_TYPE: {e}"
            )
            db_session.rollback()


def insert_reference_relations(
    db_session, primaryId: str, reference_id: int, reference_relations_from_json: Dict
):
    if not reference_relations_from_json:
        return

    type_mapping = {
        'ErratumIn': 'ErratumFor',
        'CommentIn': 'CommentOn',
        'RepublishedIn': 'RepublishedFrom',
        'RetractionIn': 'RetractionOf',
        'ExpressionOfConcernIn': 'ExpressionOfConcernFor',
        'ReprintIn': 'ReprintOf',
        'UpdateIn': 'UpdateOf',
    }

    reference_ids_types = []
    for relation_type in reference_relations_from_json:
        other_pmids = reference_relations_from_json[relation_type]
        other_reference_ids = []
        for this_pmid in other_pmids:
            other_reference_id = get_reference_by_pmid(db_session, this_pmid)
            if other_reference_id is None:
                continue
            other_reference_ids.append(other_reference_id)
        if not other_reference_ids:
            continue
        if relation_type.endswith('For') or relation_type.endswith('From') or relation_type.endswith('Of'):
            reference_id_from = reference_id
            for reference_id_to in other_reference_ids:
                if (
                    reference_id_from,
                    reference_id_to,
                    relation_type,
                ) not in reference_ids_types:
                    if reference_id_from != reference_id_to:
                        reference_ids_types.append(
                            (reference_id_from, reference_id_to, relation_type)
                        )
        else:
            mapped_type = type_mapping.get(relation_type)
            if mapped_type is None:
                continue
            reference_id_to = reference_id
            for reference_id_from in other_reference_ids:
                if (
                    reference_id_from,
                    reference_id_to,
                    mapped_type,
                ) not in reference_ids_types:
                    if reference_id_from != reference_id_to:
                        reference_ids_types.append(
                            (reference_id_from, reference_id_to, mapped_type)
                        )
    for (reference_id_from, reference_id_to, relation_type) in reference_ids_types:
        try:
            x = ReferenceRelationModel(
                reference_id_from=reference_id_from,
                reference_id_to=reference_id_to,
                reference_relation_type=relation_type,
            )
            db_session.add(x)
            log.info(
                f"{primaryId}: INSERT reference_relation: for reference_id_from = {reference_id_from}, reference_id_to = {reference_id_to}, reference_relation_type = {relation_type}"
            )
        except SQLAlchemyError as e:
            log.error(
                f"{primaryId}: Database error when inserting reference_relation: {e}"
            )
            db_session.rollback()


def insert_mesh_terms(
    db_session, primaryId: str, reference_id: int, mesh_terms_from_json: List[Dict]
):
    for m in mesh_terms_from_json:
        try:
            heading_term = m['meshHeadingTerm']
            qualifier_term = m.get('meshQualifierTerm', '')
            mesh = MeshDetailModel(
                reference_id=reference_id,
                heading_term=heading_term,
                qualifier_term=qualifier_term,
            )
            db_session.add(mesh)
            log.info(
                f"{primaryId}: INSERT MESH_DETAIL: for heading_term = '{heading_term}', qualifier_term = '{qualifier_term}'"
            )
        except KeyError as e:
            log.error(
                f"{primaryId}: Missing key {e} in mesh term data"
            )
        except SQLAlchemyError as e:
            log.error(
                f"{primaryId}: Database error when inserting MESH_DETAIL: {e}"
            )
            db_session.rollback()


def insert_cross_references(
    db_session,
    primaryId: str,
    reference_id: int,
    doi_to_reference_id: Dict[str, int],
    cross_refs_from_json: List[Dict],
) -> int:
    found = {}
    foundXREF = 0
    for c in cross_refs_from_json:
        try:
            curie = c['id']
            prefix = curie.split(':')[0]
            status = check_pattern('reference', curie)
            if status is None:
                log.info(f"Unable to find CURIE prefix {prefix} in pattern list for reference")
                continue
            if status is False:
                log.info(f"The CURIE {curie} doesn't match the pattern for reference")
                continue
            if curie.startswith('DOI:'):
                if curie in doi_to_reference_id:
                    log.info(
                        f"{primaryId}: {curie} is already in the database for reference_id = {doi_to_reference_id[curie]}"
                    )
                    continue
            if curie in found:
                continue
            found[curie] = 1

            if c.get('pages'):
                cross_ref = CrossReferenceModel(
                    curie=curie,
                    curie_prefix=prefix,
                    reference_id=reference_id,
                    pages=c['pages'],
                )
            else:
                cross_ref = CrossReferenceModel(
                    curie=curie,
                    curie_prefix=prefix,
                    reference_id=reference_id,
                )
            db_session.add(cross_ref)
            foundXREF += 1
            log.info(f"{primaryId}: INSERT CROSS_REFERENCE: {curie}")
        except KeyError as e:
            log.error(
                f"{primaryId}: Missing key {e} in cross reference data"
            )
        except SQLAlchemyError as e:
            log.error(
                f"{primaryId}: Database error when inserting CROSS_REFERENCE: {e}"
            )
            db_session.rollback()
    return foundXREF


def insert_authors(
    db_session,
    primaryId: str,
    reference_id: int,
    author_list_from_json: List[Dict],
):
    for x in author_list_from_json:
        try:
            orcid = f"ORCID:{x['orcid']}" if x.get('orcid') else ''
            affiliations = x.get('affiliations', [])
            name = x.get('name', '')
            firstname = x.get('firstname', '')
            lastname = x.get('lastname', '')
            firstinit = x.get('firstinit', '')
            rank = x.get('authorRank')
            if rank is None:
                continue
            authorData = {
                "reference_id": reference_id,
                "name": name,
                "first_name": firstname,
                "last_name": lastname,
                "first_initial": firstinit,
                "order": rank,
                "affiliations": affiliations,
                "orcid": orcid if orcid else None,
                "first_author": False,
                "corresponding_author": False,
            }
            authorObj = AuthorModel(**authorData)
            db_session.add(authorObj)
            log.info(
                f"{primaryId}: INSERT AUTHOR: {name} | '{affiliations}'"
            )
        except KeyError as e:
            log.error(
                f"{primaryId}: Missing key {e} in author data"
            )
        except SQLAlchemyError as e:
            log.error(
                f"{primaryId}: Database error when inserting AUTHOR: {e}"
            )
            db_session.rollback()
    db_session.commit()


def insert_reference(
    db_session,
    primaryId: str,
    journal_to_resource_id: Dict[str, Tuple[int, str]],
    entry: Dict,
) -> Tuple[Optional[int], Optional[str]]:
    reference_id = None
    curie = None

    try:
        resource_id = None
        journal_title = None
        if entry.get('journal'):
            journal_info = journal_to_resource_id.get(entry.get('journal'))
            if journal_info:
                resource_id, journal_title = journal_info

        curie = get_next_reference_curie(db_session)

        log.info(f"NEW REFERENCE curie = {curie}")

        date_published_start = entry.get('datePublishedStart')
        date_published_end = entry.get('datePublishedEnd')
        # This is only for unit tests.
        # The DQM loading & PubMed search have already set these two fields
        if date_published_start is None and entry.get('datePublished'):
            date_range, error_message = parse_date(entry['datePublished'], False)
            if date_range:
                date_published_start, date_published_end = date_range

        if date_published_start:
            date_published_start = str(date_published_start)[:10]
        if date_published_end:
            date_published_end = str(date_published_end)[:10]

        refData = {
            "curie": curie,
            "resource_id": resource_id,
            "title": entry.get('title', ''),
            "volume": entry.get('volume', ''),
            "issue_name": entry.get('issueName', ''),
            "page_range": entry.get('pages', ''),
            # "citation": citation,
            "pubmed_types": entry.get('pubMedType', []),
            "keywords": entry.get('keywords', []),
            "category": entry.get('allianceCategory', 'Other').replace(' ', '_'),
            "plain_language_abstract": entry.get('plainLanguageAbstract', ''),
            "pubmed_abstract_languages": entry.get('pubmedAbstractLanguages', []),
            "language": entry.get('language', ''),
            "date_published": entry.get('datePublished', ''),
            "date_published_start": date_published_start,
            "date_published_end": date_published_end,
            "date_arrived_in_pubmed": entry.get('dateArrivedInPubmed', ''),
            "date_last_modified_in_pubmed": entry.get('dateLastModified', ''),
            "publisher": entry.get('publisher', ''),
            "abstract": entry.get('abstract', ''),
        }
        if entry.get('publicationStatus'):
            refData["pubmed_publication_status"] = entry['publicationStatus']

        x = ReferenceModel(**refData)
        db_session.add(x)
        db_session.flush()
        db_session.refresh(x)
        reference_id = x.reference_id
        log.info(f"{primaryId}: INSERT REFERENCE")
        # Remove after testing from here to except.
        db_session.expire(x)
        x = db_session.query(ReferenceModel).filter_by(reference_id=reference_id).one_or_none()

    except KeyError as e:
        log.error(
            f"{primaryId}: Missing key {e} in reference data"
        )
    except ValueError as e:
        log.error(
            f"{primaryId}: Value error in reference data: {e}"
        )
    except SQLAlchemyError as e:
        log.error(
            f"{primaryId}: Database error when inserting REFERENCE: {e}"
        )
        db_session.rollback()

    return reference_id, curie


def set_primaryId(entry: Dict) -> str:
    primaryId = entry.get('primaryId')
    if primaryId and primaryId.startswith('PMID'):
        return primaryId

    if entry.get('pubmed'):
        return f"PMID:{entry['pubmed']}"

    if entry.get('crossReferences'):
        for c in entry['crossReferences']:
            if c['id'].startswith('PMID'):
                return c['id']
            if c.get('pages'):
                primaryId = c['id']
        if primaryId is None:
            primaryId = entry['crossReferences'][0]['id']
    if primaryId:
        return primaryId
    return 'unknown_paper_id'


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p',
        '--json_path',
        action='store',
        type=str,
        help='Path to JSON file or directory containing JSON files',
        required=True,
    )
    parser.add_argument(
        '-c',
        '--live_change',
        action='store_true',
        help="Commit changes to the database if set; otherwise, roll back",
    )

    args = parser.parse_args()
    post_references(args.json_path, args.live_change)
