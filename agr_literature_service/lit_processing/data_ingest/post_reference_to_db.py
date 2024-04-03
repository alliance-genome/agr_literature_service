import argparse
import logging
from os import listdir, path
import json
from typing import List

from agr_literature_service.api.crud.mod_reference_type_crud import insert_mod_reference_type_into_db
from agr_literature_service.api.models import CrossReferenceModel, ReferenceModel, \
    AuthorModel, ModCorpusAssociationModel, ModModel, ReferenceRelationModel, \
    MeshDetailModel, WorkflowTagModel
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.db_read_utils import get_journal_data, \
    get_doi_data, get_reference_by_pmid
from agr_literature_service.api.crud.reference_crud import get_citation_from_args
from agr_literature_service.global_utils import get_next_reference_curie
from agr_literature_service.lit_processing.data_ingest.utils.date_utils import parse_date
from agr_literature_service.api.crud.utils.patterns_check import check_pattern

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def post_references(json_path, live_change=True):  # noqa: C901

    db_session = create_postgres_session(False)

    files_to_process = []
    if path.isdir(json_path):
        for filename in listdir(json_path):
            if 'REFERENCE_' in filename and '.REFERENCE_' not in filename:
                files_to_process.append(json_path + filename)
    else:
        files_to_process.append(json_path)

    log.info("Getting journal info from database...")
    journal_to_resource_id = get_journal_data(db_session)

    log.info("Getting DOI info from database...")
    doi_to_reference_id = get_doi_data(db_session)

    log.info("Getting mod info from database...")
    mod_to_mod_id = dict([(x.abbreviation, x.mod_id) for x in db_session.query(ModModel).all()])

    log.info("Reading json data and loading data into database...")

    new_ref_curies = []
    for json_file in sorted(files_to_process):
        if not path.exists(json_file):
            continue
        f = open(json_file)
        json_data = json.load(f)
        newly_added_curies = read_data_and_load_references(db_session, json_data,
                                                           journal_to_resource_id,
                                                           doi_to_reference_id,
                                                           mod_to_mod_id, live_change)
        if newly_added_curies:
            new_ref_curies.extend(newly_added_curies)
    db_session.commit()
    db_session.close()
    log.info("DONE!\n\n")
    return new_ref_curies


def read_data_and_load_references(db_session, json_data, journal_to_resource_id, doi_to_reference_id, mod_to_mod_id, live_change):

    new_ref_curies = []
    for entry in json_data:

        primaryId = set_primaryId(entry)
        crossRef = db_session.query(CrossReferenceModel).filter_by(
            curie=primaryId, is_obsolete=False).one_or_none()
        if crossRef:
            continue

        try:
            if entry.get('crossReferences') is None:
                continue

            reference_id, curie = insert_reference(db_session, primaryId, journal_to_resource_id, entry)
            # new_ref_curies.append(curie)

            if reference_id is None:
                log.info(primaryId + ": Error loading reference table")
                return

            log.info(primaryId + ": reference_id = " + str(reference_id))

            foundXREF = insert_cross_references(db_session, primaryId, reference_id,
                                                doi_to_reference_id, entry['crossReferences'])
            if not foundXREF:
                db_session.rollback()
                continue

            if entry.get('authors'):

                insert_authors(db_session, primaryId, reference_id, entry['authors'])

            if entry.get('meshTerms'):

                insert_mesh_terms(db_session, primaryId, reference_id,
                                  entry['meshTerms'])

            if entry.get('commentsCorrections'):

                insert_reference_relations(db_session, primaryId, reference_id,
                                           entry['commentsCorrections'])

            if entry.get('MODReferenceTypes'):

                insert_mod_reference_types(db_session, primaryId, reference_id, entry['MODReferenceTypes'],
                                           entry.get('pubmedType', []))

            mod_id = None
            if entry.get('modCorpusAssociations'):

                mod_id = insert_mod_corpus_associations(db_session, primaryId, reference_id,
                                                        mod_to_mod_id,
                                                        entry['modCorpusAssociations'])

            if entry.get('workflowTags') and mod_id:

                insert_workflow_tags(db_session, primaryId, reference_id, mod_id,
                                     entry['workflowTags'])

            log.info("The new reference for for primaryId = " + primaryId + " has been added into database")
            if live_change:
                db_session.commit()
            else:
                db_session.rollback()
            new_ref_curies.append(curie)
        except Exception as e:
            log.info("An error occurred when adding the new reference into database for primaryId = " + primaryId + " " + str(e))
            db_session.rollback()
    return new_ref_curies


def insert_workflow_tags(db_session, primaryId, reference_id, mod_id, workflowTags):

    for atp in workflowTags:
        try:
            x = WorkflowTagModel(reference_id=reference_id,
                                 mod_id=mod_id,
                                 workflow_tag_id=atp)
            db_session.add(x)
            log.info(primaryId + ": INSERT WORKFLOW_TAG: for reference_id = " + str(reference_id) + ", mod_id = " + str(mod_id) + ", workflog_tag_id = " + atp)
        except Exception as e:
            log.info(primaryId + ": INSERT WORKFLOW_TAG: for reference_id = " + str(reference_id) + ", mod_id = " + str(mod_id) + ", workflog_tag_id = " + atp + " " + str(e))


def insert_mod_corpus_associations(db_session, primaryId, reference_id, mod_to_mod_id, mod_corpus_associations_from_json):

    mod_id = None
    for x in mod_corpus_associations_from_json:
        try:
            mod_id = mod_to_mod_id.get(x.get('modAbbreviation'))
            if mod_id is None:
                log.info("The 'modAbbreviation' is missing in the json data for primaryId = " + primaryId)
                continue
            mca = ModCorpusAssociationModel(reference_id=reference_id,
                                            mod_id=mod_id,
                                            mod_corpus_sort_source=x['modCorpusSortSource'],
                                            corpus=x['corpus'])
            db_session.add(mca)
            log.info(primaryId + ": INSERT MOD_CORPUS_ASSOCIATION: for reference_id = " + str(reference_id) + ", mod_id = " + str(mod_id) + ", mod_corpus_sort_source = " + x['modCorpusSortSource'])
        except Exception as e:
            log.info(primaryId + ": INSERT MOD_CORPUS_ASSOCIATION: for reference_id = " + str(reference_id) + ", mod_id = " + str(mod_id) + ", mod_corpus_sort_source = " + x['modCorpusSortSource'] + " " + str(e))
    return mod_id


def insert_mod_reference_types(db_session, primaryId, reference_id, mod_ref_types_from_json, pubmed_types: List[str]):

    found = {}
    for x in mod_ref_types_from_json:
        if (reference_id, x['source'], x['referenceType']) in found:
            continue
        found[(reference_id, x['source'], x['referenceType'])] = 1
        try:
            insert_mod_reference_type_into_db(db_session, pubmed_types, x['source'], x['referenceType'], reference_id)
            log.info(primaryId + ": INSERT MOD_REFERENCE_TYPE: for reference_id = " + str(reference_id) + ", source = " + x['source'] + ", reference_type = " + x['referenceType'])
        except Exception as e:
            log.info(primaryId + ": INSERT MOD_REFERENCE_TYPE: for reference_id = " + str(reference_id) + ", source = " + x['source'] + ", reference_type = " + x['referenceType'] + " " + str(e))


def insert_reference_relations(db_session, primaryId, reference_id, reference_relations_from_json):

    if str(reference_relations_from_json) == '{}':
        return

    type_mapping = {'ErratumIn': 'ErratumFor',
                    'CommentIn': 'CommentOn',
                    'RepublishedIn': 'RepublishedFrom',
                    'RetractionIn': 'RetractionOf',
                    'ExpressionOfConcernIn': 'ExpressionOfConcernFor',
                    'ReprintIn': 'ReprintOf',
                    'UpdateIn': 'UpdateOf'}

    reference_ids_types = []
    for type in reference_relations_from_json:
        other_pmids = reference_relations_from_json[type]
        other_reference_ids = []
        for this_pmid in other_pmids:
            other_reference_id = get_reference_by_pmid(db_session, this_pmid)
            if other_reference_id is None:
                continue
            other_reference_ids.append(other_reference_id)
        if len(other_reference_ids) == 0:
            continue
        if type.endswith('For') or type.endswith('From') or type.endswith('Of'):
            reference_id_from = reference_id
            for reference_id_to in other_reference_ids:
                if (reference_id_from, reference_id_to, type) not in reference_ids_types:
                    if reference_id_from != reference_id_to:
                        reference_ids_types.append((reference_id_from, reference_id_to, type))
        else:
            type = type_mapping.get(type)
            if type is None:
                continue
            reference_id_to = reference_id
            for reference_id_from in other_reference_ids:
                if (reference_id_from, reference_id_to, type) not in reference_ids_types:
                    if reference_id_from != reference_id_to:
                        reference_ids_types.append((reference_id_from, reference_id_to, type))
        for (reference_id_from, reference_id_to, type) in reference_ids_types:
            try:
                x = ReferenceRelationModel(reference_id_from=reference_id_from,
                                           reference_id_to=reference_id_to,
                                           reference_relation_type=type)
                db_session.add(x)
                log.info(primaryId + ": INSERT reference_relation: for reference_id_from = " + str(reference_id_from) + ", reference_id_to = " + str(reference_id_to) + ", reference_relation_type = " + type)
            except Exception as e:
                log.info(primaryId + ": INSERT reference_relation: for reference_id_from = " + str(reference_id_from) + ", reference_id_to = " + str(reference_id_to) + ", reference_relation_type = " + type + " " + str(e))


def insert_mesh_terms(db_session, primaryId, reference_id, mesh_terms_from_json):

    for m in mesh_terms_from_json:
        heading_term = m['meshHeadingTerm']
        qualifier_term = m.get('meshQualifierTerm', '')
        try:
            mesh = MeshDetailModel(reference_id=reference_id, heading_term=heading_term, qualifier_term=qualifier_term)
            db_session.add(mesh)
            log.info(primaryId + ": INSERT MESH_DETAIL: for heading_term = '" + heading_term + "', qualifier_term = '" + qualifier_term + "'")
        except Exception as e:
            log.info(primaryId + ": INSERT MESH_DETAIL: for heading_term = '" + heading_term + "', qualifier_term = '" + qualifier_term + "' failed " + str(e))


def insert_cross_references(db_session, primaryId, reference_id, doi_to_reference_id, cross_refs_from_json):

    found = {}
    foundXREF = 0
    for c in cross_refs_from_json:
        curie = c['id']
        prefix = curie.split(':')[0]
        status = check_pattern('reference', curie)
        if status is None:
            log.info(f"Unable to find curie prefix {prefix} in pattern list for reference")
            continue
        if status is False:
            log.info(f"The curie {curie} doesn't match the pattern for reference")
            continue
        if curie.startswith('DOI:'):
            if curie in doi_to_reference_id:
                log.info(primaryId + ": " + curie + " is already in the database for reference_id = " + str(doi_to_reference_id[curie]))
                continue
        if curie in found:
            continue
        found[curie] = 1

        try:
            cross_ref = None
            if c.get('pages'):
                cross_ref = CrossReferenceModel(curie=curie,
                                                curie_prefix=curie.split(":")[0],
                                                reference_id=reference_id,
                                                pages=c['pages'])
            else:
                cross_ref = CrossReferenceModel(curie=curie,
                                                curie_prefix=curie.split(":")[0],
                                                reference_id=reference_id)
            db_session.add(cross_ref)
            foundXREF += 1
            log.info(primaryId + ": INSERT CROSS_REFERENCE: " + curie)
        except Exception as e:
            log.info(primaryId + ": INSERT CROSS_REFERENCE: " + curie + " failed: " + str(e))
    return foundXREF


def insert_authors(db_session, primaryId, reference_id, author_list_from_json):

    for x in author_list_from_json:
        orcid = 'ORCID:' + x['orcid'] if x.get('orcid') else ''
        affiliations = x['affiliations'] if x.get('affiliations') else []
        name = x.get('name', '')
        firstname = x.get('firstname', '')
        lastname = x.get('lastname', '')
        firstinit = x.get('firstinit', '')
        rank = x.get('authorRank')
        if rank is None:
            continue
        authorData = {"reference_id": reference_id,
                      "name": name,
                      "first_name": firstname,
                      "last_name": lastname,
                      "first_initial": firstinit,
                      "order": rank,
                      "affiliations": affiliations,
                      "orcid": orcid if orcid else None,
                      "first_author": False,
                      "corresponding_author": False}
        try:
            authorObj = AuthorModel(**authorData)
            db_session.add(authorObj)
            log.info(primaryId + ": INSERT AUTHOR: " + name + " | '" + str(affiliations) + "'")
        except Exception as e:
            log.info(primaryId + ": INSERT AUTHOR: " + name + " failed: " + str(e))
    db_session.commit()


def insert_reference(db_session, primaryId, journal_to_resource_id, entry):

    reference_id = None
    curie = None

    try:
        resource_id = None
        journal_title = None
        if entry.get('journal'):
            if entry.get('journal') in journal_to_resource_id:
                (resource_id, journal_title) = journal_to_resource_id[entry.get('journal')]

        # citation = generate_citation(entry, journal_title)

        curie = get_next_reference_curie(db_session)

        log.info("NEW REFERENCE curie = " + str(curie))

        date_published_start = entry.get('datePublishedStart')
        date_published_end = entry.get('datePublishedEnd')
        ## this is only for unit tests.
        ## The dqm loading & PubMed search have already set these two fields
        if date_published_start is None and entry.get('datePublished'):
            date_range, error_message = parse_date(entry['datePublished'], False)
            if date_range is not False:
                (date_published_start, date_published_end) = date_range

        date_published_start = str(date_published_start)[0:10]
        date_published_end = str(date_published_end)[0:10]

        refData = {"curie": curie,
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
                   "abstract": entry.get('abstract', '')}
        if entry.get('publicationStatus'):
            refData["pubmed_publication_status"] = entry['publicationStatus']

        x = ReferenceModel(**refData)
        db_session.add(x)
        # db_session.commit()
        db_session.flush()
        db_session.refresh(x)
        reference_id = x.reference_id
        log.info(primaryId + ": INSERT REFERENCE")
        # remove after testing from here to except.
        db_session.expire(x)
        x = db_session.query(ReferenceModel).filter_by(reference_id=x.reference_id).one_or_none()

    except Exception as e:
        log.info(primaryId + ": INSERT REFERENCE failed " + str(e))

    return reference_id, curie


def generate_citation(entry, journal_title):

    authorNames = ''
    if 'authors' in entry:
        author_names_order = []
        for x in entry['authors']:
            author_names_order.append((x['name'], x['authorRank']))
        authorNames = "; ".join([x[0] for x in sorted(author_names_order, key=lambda x: x[1])])
        if authorNames.endswith("; "):
            authorNames = authorNames[:-2]  # remove last '; '
    date_published = str(entry.get('datePublished', ''))
    title = entry.get('title', '')
    volume = entry.get('volume', '')
    issue = entry.get('issueName', '')
    page_range = entry.get('pages', '')

    citation = get_citation_from_args(authorNames, date_published, title,
                                      journal_title, volume, issue, page_range)

    return citation


def set_primaryId(entry):

    primaryId = entry.get('primaryId')
    if primaryId and primaryId.startswith('PMID'):
        return primaryId

    if entry.get('pubmed'):
        return 'PMID:' + entry['pubmed']

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
    parser.add_argument('-p', '--json_path', action='store', type=str, help='json_file_or_json_file_path', required=True)
    parser.add_argument('-c', '--live_change', action='store_true', help="need_to_check_file")

    args = vars(parser.parse_args())
    post_references(args['json_path'], args['live_change'])
