import json

import requests
import re
from os import environ, makedirs, path
import logging
import logging.config

from agr_literature_service.api.database.setup import setup_database
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.parse_dqm_json_reference import generate_pmid_data, \
    aggregate_dqm_with_pubmed
from agr_literature_service.lit_processing.data_ingest.dqm_ingest.sort_dqm_json_reference_updates import \
    sort_dqm_references
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import post_references
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from agr_literature_service.lit_processing.tests.parse_pubmed_json_reference import parse_pubmed_json_reference
from agr_literature_service.lit_processing.tests.process_many_pmids_to_json import process_many_pmids_to_json
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import sqlalchemy_load_ref_xref
from agr_literature_service.lit_processing.utils.generic_utils import split_identifier

from agr_literature_service.lit_processing.tests.generate_dqm_json_test_set import load_sample_json
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import CrossReferenceModel, AuthorModel
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.process_single_pmid import process_pmid
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_single_mod import \
    update_data
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

from dotenv import load_dotenv

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()
init_tmp_dir()

logging.basicConfig(level=logging.INFO,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

base_path = environ.get("XML_PATH", "")

# connect to docker
#   docker run --rm --network=agr_literature_service_agr-literature -p 5432:5432 -v ${PWD}:/workdir -t -i 100225593120.dkr.ecr.us-east-1.amazonaws.com/agr_literature_dev:latest /bin/bash
# python3 test_functional.py
# 3 minutes on 34 references from 'inputs/sample_dqm_load.json'
#
# to run outside docker set PYTHONPATH to src/xml_processing/ directory and XML_PATH to src/xml_processing/tests/ directory
# pipenv run python functional_tests.py
#
# once pytest can work with this script rename to test_functional.py and
# pipenv run python test_functional.py


def resolve_dqm_to_agr(entry, xref_ref):
    """
    Take a dqm entry and the database mappings of cross_references to reference curies, and return the agr curie for the dqm entry, and whether it was found in xref file.

    :param entry:
    :param xref_ref:
    :return:
    """
    agr_found = False
    agr = ''
    if 'pmid' in entry:
        prefix, identifier, separator = split_identifier(entry['pmid'])
        if prefix in xref_ref:
            if identifier in xref_ref[prefix]:
                agr_found = True
                agr = xref_ref[prefix][identifier]
    if not agr_found and 'modId' in entry:
        for mod_id in entry['modId']:
            if agr_found:
                break
            prefix, identifier, separator = split_identifier(mod_id)
            if prefix in xref_ref:
                if identifier in xref_ref[prefix]:
                    agr_found = True
                    agr = xref_ref[prefix][identifier]
    return agr, agr_found


def test_update_references():
    """
    Load cross_references from database, and sample_dqm_update.json mapping them to the agr reference curie and the types of checks each should have.  Query each reference from the database, and run the appropriate test.

    """

    # xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref_api_flatfile('reference')
    xref_ref, ref_xref_valid, ref_xref_obsolete = sqlalchemy_load_ref_xref('reference')
    input_file = 'inputs/sample_dqm_update.json'
    sample_json = load_sample_json(input_file)
    if not sample_json:
        return
    agr_wanted = dict()
    for entry in sample_json['data']:
        agr, agr_found = resolve_dqm_to_agr(entry, xref_ref)
        if not agr_found:
            assert 'doi_conflict' in entry['update_check']
            continue
        if agr not in agr_wanted:
            agr_wanted[agr] = dict()
        if 'update_check' in entry:
            for check in entry['update_check']:
                if logger.getEffectiveLevel() <= logging.DEBUG:
                    json_data = json.dumps(entry['update_check'], indent=4, sort_keys=True)
                    logger.debug(json_data)
                logger.debug("check %s", check)
                agr_wanted[agr][check] = entry['update_check'][check]
    api_port = environ.get('API_PORT')
    api_server = environ.get('API_SERVER', 'localhost')
    for agr in sorted(agr_wanted):
        db_entry = dict()
        if agr_wanted[agr]:
            url = 'http://' + api_server + ':' + api_port + '/reference/' + agr
            logger.debug("get AGR reference info from database %s", url)
            get_return = requests.get(url)
            db_entry = json.loads(get_return.text)
            logger.debug(db_entry)
        for check in agr_wanted[agr]:
            test_result = check_test(db_entry, check, agr_wanted[agr][check])
            logger.info("agr %s check %s result %s", agr, check, test_result)


def test_load_references():
    """
    Load cross_references from database, and sample_dqm_load.json mapping them to the agr reference curie and the types of checks each should have.  Query each reference from the database, and run the appropriate test.

    """

    # xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref_api_flatfile('reference')
    xref_ref, ref_xref_valid, ref_xref_obsolete = sqlalchemy_load_ref_xref('reference')
    input_file = 'inputs/sample_dqm_load.json'
    sample_json = load_sample_json(input_file)
    if not sample_json:
        return
    agr_wanted = dict()
    for entry in sample_json['data']:
        agr, agr_found = resolve_dqm_to_agr(entry, xref_ref)
        if not agr_found:
            logger.debug("entry is {}".format(entry))
            logger.debug(entry['load_check'])
            logger.debug(agr)
            assert 'doi_conflict' in entry['load_check']
            continue
        if agr not in agr_wanted:
            agr_wanted[agr] = dict()
        if 'load_check' in entry:
            for check in entry['load_check']:
                if logger.getEffectiveLevel() <= logging.DEBUG:
                    json_data = json.dumps(entry['load_check'], indent=4, sort_keys=True)
                    logger.debug(json_data)
                agr_wanted[agr][check] = entry['load_check'][check]
    api_port = environ.get('API_PORT')
    api_server = environ.get('API_SERVER', 'localhost')
    # counter = 0
    for agr in sorted(agr_wanted):
        # for debugging
        # for check in agr_wanted[agr]:
        #     logger.info("agr %s check %s", agr, check)
        # if agr != 'AGR:AGR-Reference-0000000019':
        #     continue
        # counter = counter + 1
        # if counter > 3:
        #     break
        url = 'http://' + api_server + ':' + api_port + '/reference/' + agr
        logger.info("get AGR reference info from database %s", url)
        get_return = requests.get(url)
        logger.debug(get_return.text)
        db_entry = json.loads(get_return.text)
        logger.debug(db_entry)
        for check in agr_wanted[agr]:
            test_result = check_test(db_entry, check, agr_wanted[agr][check])
            logger.debug("agr %s check %s result %s", agr, check, test_result)


def erratum_check(agr_data, value):
    """
    future: check a database reference has reference_relations connection to another reference

    :param agr_data:
    :param value:
    :return:
    """

    # when reference_relations loaded, check that an xref is made to value e.g. PMID:2 to PMID:8
    return 'Success: Errata references created, but connections not created yet, add to sample_reference_populate_load.sh later'


def category_book_check(agr_data, value):
    """
    check a database reference has a category of 'book' from PubMed XML <BookDocument>

    :param agr_data:
    :param value:
    :return:
    """

    if 'category' in agr_data:
        assert agr_data['category'] == value
        if agr_data['category'] == value:
            return 'Success'
    return 'Failure'


def title_check(agr_data, value):
    """
    check a database reference has a title

    :param agr_data:
    :param value:
    :return:
    """

    if 'title' in agr_data:
        assert agr_data['title'] == value
        if agr_data['title'] == value:
            return 'Success'
    return 'Failure'


def mod_corpus_association_check(agr_data, values):
    """
    check a database reference has explicit mod corpus association

    :param agr_data:
    :param value:
    :return:
    """

    failure_string = ''
    db_values = set()
    if 'mod_corpus_associations' in agr_data and agr_data['mod_corpus_associations'] is not None:
        for mca_db in agr_data['mod_corpus_associations']:
            db_string = ''
            if 'mod_abbreviation' in mca_db:
                db_string = db_string + mca_db['mod_abbreviation']
            if 'mod_corpus_sort_source' in mca_db:
                db_string = db_string + mca_db['mod_corpus_sort_source']
            if 'corpus' in mca_db:
                db_string = db_string + str(mca_db['corpus'])
            db_values.add(db_string)
    for mca_dqm in values:
        dqm_string = ''
        if 'mod_abbreviation' in mca_dqm:
            dqm_string = dqm_string + mca_dqm['mod_abbreviation']
        if 'mod_corpus_sort_source' in mca_dqm:
            dqm_string = dqm_string + mca_dqm['mod_corpus_sort_source']
        if 'corpus' in mca_dqm:
            dqm_string = dqm_string + str(mca_dqm['corpus'])
        if dqm_string not in db_values:
            mca_string = json.dumps(mca_dqm, indent=4, sort_keys=True)
            failure_string = failure_string + mca_string + " not in database. "
    result = 'Failure'
    if failure_string != '':
        failure_string = 'Failure: ' + failure_string
        result = failure_string
    else:
        result = 'Success'
    assert result == 'Success'
    return result


def mod_reference_types_check(agr_data, values):
    """
    check a database reference has explicit mod reference types

    :param agr_data:
    :param value:
    :return:
    """

    failure_string = ''
    db_values = set()
    if 'mod_reference_types' in agr_data and agr_data['mod_reference_types']:
        for mrt_db in agr_data['mod_reference_types']:
            db_string = ''
            if 'reference_type' in mrt_db:
                db_string = db_string + mrt_db['reference_type']
            if 'source' in mrt_db:
                db_string = db_string + mrt_db['source']
            db_values.add(db_string)
    for mrt_dqm in values:
        dqm_string = ''
        if 'reference_type' in mrt_dqm:
            dqm_string = dqm_string + mrt_dqm['reference_type']
        if 'source' in mrt_dqm:
            dqm_string = dqm_string + mrt_dqm['source']
        if dqm_string not in db_values:
            mrt_string = json.dumps(mrt_dqm, indent=4, sort_keys=True)
            failure_string = failure_string + mrt_string + " not in database. "
    result = 'Failure'
    if failure_string != '':
        failure_string = 'Failure: ' + failure_string
        result = failure_string
    else:
        result = 'Success'
    assert result == 'Success'
    return result


def authors_exact_check(agr_data, values):
    """
    check a database reference has exact author info for explicit fields from dqm file

    :param agr_data:
    :param value:
    :return:
    """

    if 'authors' not in agr_data:
        return 'Failure: No authors found in database'
    failure_string = ''
    db_values = set()
    if 'authors' in agr_data:
        for aut_db in agr_data['authors']:
            db_string = ''
            if 'order' in aut_db and aut_db['order'] is not None:
                db_string = db_string + str(aut_db['order'])
            if 'name' in aut_db and aut_db['name'] is not None:
                db_string = db_string + aut_db['name']
            if 'first_name' in aut_db and aut_db['first_name'] is not None:
                db_string = db_string + aut_db['first_name']
            if 'last_name' in aut_db and aut_db['last_name'] is not None:
                db_string = db_string + aut_db['last_name']
            db_values.add(db_string)
    for aut_dqm in values:
        dqm_string = ''
        if 'order' in aut_dqm:
            dqm_string = dqm_string + str(aut_dqm['order'])
        if 'name' in aut_dqm:
            dqm_string = dqm_string + aut_dqm['name']
        if 'first_name' in aut_dqm:
            dqm_string = dqm_string + aut_dqm['first_name']
        if 'last_name' in aut_dqm:
            dqm_string = dqm_string + aut_dqm['last_name']
        if dqm_string not in db_values:
            aut_string = json.dumps(aut_dqm, indent=4, sort_keys=True)
            failure_string = failure_string + aut_string + " not in database. "
    result = 'Failure'
    if failure_string != '':
        failure_string = 'Failure: ' + failure_string
        result = failure_string
    else:
        result = 'Success'
    assert result == 'Success'
    return result


def author_name_check(agr_data, value):
    """
    check a database reference has all authors with names, because <CollectiveName> in PubMed XML is not standard author pattern

    :param agr_data:
    :param value:
    :return:
    """

    if 'authors' not in agr_data:
        return 'Failure: No authors found in database'
    result = 'Success'
    has_specific_value = False
    for author in agr_data['authors']:
        has_name = False
        if 'name' in author:
            if author['name'] != '':
                has_name = True
            if author['name'] == value:
                has_specific_value = True
        if not has_name:
            result = 'Failure'
    assert has_specific_value is True
    if not has_specific_value:
        result = 'Failure'
    return result


def author_affiliation_check(agr_data, value):
    """
    check a database reference has an author with an affiliation

    :param agr_data:
    :param value:
    :return:
    """

    result = 'Failure'
    if 'authors' not in agr_data:
        return 'Failure: No authors found in database'
    for author in agr_data['authors']:
        if 'affiliations' in author and author['affiliations'] is not None:
            for affiliation in author['affiliations']:
                if affiliation == value:
                    result = 'Success'
    assert result == 'Success'
    return result


def author_orcid_check(agr_data, value):
    """
    check a database reference has an author with an orcid

    :param agr_data:
    :param value:
    :return:
    """

    result = 'Failure'
    if 'authors' not in agr_data:
        result = 'Failure: No authors found in database'
    for author in agr_data['authors']:
        if 'orcid' in author and author['orcid'] is not None:
            if 'curie' in author['orcid']:
                if author['orcid']['curie'] == value:
                    result = 'Success'
    assert result == 'Success'
    return result


def xref_mods_check(agr_data, mods):
    """
    check a database reference has cross_references to six base mods

    :param agr_data:
    :param value:
    :return:
    """

    result = 'Failure'
    prefixes = set()
    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                prefix, identifier, separator = split_identifier(xref['curie'])
                prefixes.add(prefix)
    all_mods_found = True
    for mod in mods:
        if mod not in prefixes:
            all_mods_found = False
    if all_mods_found:
        result = 'Success'
    else:
        prefixes_found = ', '.join(sorted(prefixes))
        result = 'Failure: only found ' + prefixes_found
    assert result == 'Success'
    return result


def html_abstract_check(agr_data, value):
    """
    future: check a database reference does not have html in the abstract

    :param agr_data:
    :param value:
    :return:
    """

    if 'abstract' in agr_data:
        assert agr_data['abstract'] == value
        if agr_data['abstract'] == value:
            return 'Success: Expected abstract value.  Abstracts do have html, have not decided whether they should not'
    return 'Success: Abstracts do have html, have not decided whether they should not'


def html_doi_check(agr_data, value):
    """
    check a database reference does not have html in the doi

    :param agr_data:
    :param value:
    :return:
    """

    result = 'Failure'
    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                prefix, identifier, separator = split_identifier(xref['curie'])
                if xref['curie'] == value:
                    result = 'Success'
                if prefix == 'DOI':
                    if re.search('&', xref['curie']):
                        result = 'Failure: ' + xref['curie'] + ' has &, could be html in doi'
    assert result == 'Success'
    return result


def keywords_check(agr_data, value):
    """
    check a database reference does have a keyword

    :param agr_data:
    :param value:
    :return:
    """

    result = 'Failure'
    if 'keywords' in agr_data:
        for keyword in agr_data['keywords']:
            if keyword == value:
                result = 'Success'
    assert result == 'Success'
    return result


def doi_conflict_check(agr_data, value):
    """
    check a database reference has a conflict from DOI.  always pass this here, it gets checked during cross_reference resolution

    :param agr_data:
    :param value:
    :return:
    """

    return 'Success'


def has_doi_check(agr_data, value):
    """
    check a database reference does have a doi

    :param agr_data:
    :param value:
    :return:
    """

    result = 'Failure'
    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                if xref['curie'] == value:
                    result = 'Success'
    assert result != 'Failure'
    return result


def test_first_corresponding_author_flag():

    pmid = "26051182"
    process_pmid(pmid, '', '')

    db_session = create_postgres_session(False)

    result = 'Failure'
    c = db_session.query(CrossReferenceModel).filter_by(curie='PMID:' + pmid).one_or_none()
    if c is None:
        return result

    a = db_session.query(AuthorModel).filter_by(reference_id=c.reference_id, order=1).one_or_none()
    a.first_author = True
    a.corresponding_author = True
    a.name = 'TEST full name'
    db_session.add(a)
    db_session.commit()

    ## in the case of updating pubmed papers for one or more pmids
    ## the updating script will not check md5sum - which means it
    ## will always go to pubmed to grab a new xml and update the database
    ## accordingly
    update_data(None, pmid)

    x = db_session.query(AuthorModel).filter_by(name='TEST full name', reference_id=c.reference_id).one_or_none()
    if x is None:
        return result
    if x.first_author is True and x.corresponding_author is True:
        result = "Success"
    assert result != 'Failure'
    return result


def test_pubmed_types_to_category_mapping(base_dir=base_path):

    json_path = base_path + "pubmed_json/"
    if not path.exists(json_path):
        makedirs(json_path)

    pmids = ['26051182', '19678847', '7567443']
    # 26051182 ['Journal Article', 'Research Support, N.I.H., Extramural', "Research Support, Non-U.S. Gov't"]
    # 19678847 ['Comparative Study', 'Journal Article', "Research Support, Non-U.S. Gov't", 'Review']
    # 7567443  ['Journal Article', "Research Support, Non-U.S. Gov't", 'Corrected and Republished Article']
    # Note: Retraction category is only mapped from 'Retraction Notice' pubmed_type
    pmid_to_category = {'26051182': 'Research_Article',
                        '19678847': 'Review_Article',
                        '7567443': 'Correction'}

    generate_json(pmids, [], base_dir=base_dir)

    result = 'Failure'
    good_check_check_count = 0
    for pmid in pmids:
        json_file = json_path + pmid + ".json"
        f = open(json_file)
        json_data = json.load(f)
        f.close()
        if json_data.get('allianceCategory') and json_data['allianceCategory'] == pmid_to_category[pmid]:
            good_check_check_count = good_check_check_count + 1
    if good_check_check_count == 3:
        result = "Success"

    assert result != 'Failure'
    return result


def has_pmid_check(agr_data, value):
    """
    check a database reference does have a pmid

    :param agr_data:
    :param value:
    :return:
    """

    result = 'Failure'
    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                if xref['curie'] == value:
                    result = 'Success: PMID found, PubMed XML might not have it'
    assert result != 'Failure'
    return result


def no_pmid_check(agr_data, value):
    """
    check a database reference does not have a pmid

    :param agr_data:
    :param value:
    :return:
    """

    result = 'Success'
    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                prefix, identifier, separator = split_identifier(xref['curie'])
                if prefix == 'PMID':
                    result = 'Failure: PMID found, this reference should not have one'
    assert result == 'Success'
    return result


def check_test(agr_data, check, value):
    """
    case switch for different types of tests

    :param agr_data:
    :param check:
    :param value:
    :return:
    """

    options = {
        'VernacularTitle': title_check,
        'BookTitle': title_check,
        'ArticleTitle': title_check,
        'title': title_check,
        'CollectiveName': author_name_check,
        'AggregateMods': xref_mods_check,
        'html_doi': html_doi_check,
        'html_abstract': html_abstract_check,
        'BookDocument': category_book_check,
        'doi_conflict': doi_conflict_check,
        'DOI': has_doi_check,
        'PMID': has_pmid_check,
        'no_pmid': no_pmid_check,
        'ORCID': author_orcid_check,
        'AffiliationInfo': author_affiliation_check,
        'Keywords': keywords_check,
        'has_erratum': erratum_check,
        'MODReferenceTypes': mod_reference_types_check,
        'ModCorpusAssociation': mod_corpus_association_check,
        'authors': authors_exact_check,
        'FAIL': title_check
    }
    if check in options:
        test_result = options[check](agr_data, value)
        return test_result
    else:
        return 'test not found'


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("starting functional_tests.py")

    # run this once after data is loaded
    # generate_cross_references_file('reference')

    # TODO:
    # re-write the following two tests, maybe add more tests here

    setup_database()
    populate_test_mods()

    # load the data
    local_file_path = path.dirname(path.abspath(__file__)) + "/"
    generate_pmid_data(base_input_dir=local_file_path, input_path="dqm_load_sample",
                       output_directory="./", input_mod="all")
    process_many_pmids_to_json(skip_download=True, load_pmids_from_file_path="inputs/alliance_pmids",
                               base_dir=local_file_path)
    aggregate_dqm_with_pubmed(base_dir=local_file_path, input_path="dqm_load_sample", input_mod="all",
                              output_directory="./")
    parse_pubmed_json_reference(load_pmids_from_file_path="inputs/pubmed_only_pmids")
    json_filepath = base_path + 'sanitized_reference_json/'
    post_references(json_path=json_filepath)

    # load the update
    sort_dqm_references(base_dir=local_file_path, input_path="/dqm_update_sample", input_mod="WB")

    test_load_references()
    test_update_references()
    test_pubmed_types_to_category_mapping(base_dir=local_file_path)
    test_first_corresponding_author_flag()

    logger.info("ending sort_dqm_json_reference_updates.py")
