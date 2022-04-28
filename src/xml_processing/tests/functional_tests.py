import json
import requests

import re

from os import environ
import logging
import logging.config

from helper_file_processing import load_ref_xref, split_identifier, generate_cross_references_file

from generate_dqm_json_test_set import load_sample_json

from dotenv import load_dotenv

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

load_dotenv()

logging.basicConfig(level=logging.INFO,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

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

    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('reference')
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
                # for debugging
                # json_data = json.dumps(entry['update_check'], indent=4, sort_keys=True)
                # logger.info(json_data)
                logger.info("check %s", check)
                agr_wanted[agr][check] = entry['update_check'][check]
    api_port = environ.get('API_PORT')
    api_server = environ.get('API_SERVER', 'localhost')
    for agr in sorted(agr_wanted):
        db_entry = dict()
        if agr_wanted[agr]:
            url = 'http://' + api_server + ':' + api_port + '/reference/' + agr
            logger.info("get AGR reference info from database %s", url)
            get_return = requests.get(url)
            db_entry = json.loads(get_return.text)
            # logger.info(db_entry)
        for check in agr_wanted[agr]:
            test_result = check_test(db_entry, check, agr_wanted[agr][check])
            logger.info("agr %s check %s result %s", agr, check, test_result)


def test_load_references():
    """
    Load cross_references from database, and sample_dqm_load.json mapping them to the agr reference curie and the types of checks each should have.  Query each reference from the database, and run the appropriate test.

    """

    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('reference')
    input_file = 'inputs/sample_dqm_load.json'
    sample_json = load_sample_json(input_file)
    if not sample_json:
        return
    agr_wanted = dict()
    for entry in sample_json['data']:
        agr, agr_found = resolve_dqm_to_agr(entry, xref_ref)
        if not agr_found:
            print("entry is {}".format(entry))
            print(entry['load_check'])
            print(agr)
            assert 'doi_conflict' in entry['load_check']
            continue
        if agr not in agr_wanted:
            agr_wanted[agr] = dict()
        if 'load_check' in entry:
            for check in entry['load_check']:
                # for debugging
                # json_data = json.dumps(entry['load_check'], indent=4, sort_keys=True)
                # logger.info(json_data)
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
        db_entry = json.loads(get_return.text)
        # logger.info(db_entry)
        for check in agr_wanted[agr]:
            test_result = check_test(db_entry, check, agr_wanted[agr][check])
            logger.info("agr %s check %s result %s", agr, check, test_result)


def erratum_check(agr_data, value):
    """
    future: check a database reference has comment_and_corrections connection to another reference

    :param agr_data:
    :param value:
    :return:
    """

    # when comments and corrections loaded, check that an xref is made to value e.g. PMID:2 to PMID:8
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
    if 'mod_reference_types' in agr_data:
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
    generate_cross_references_file('reference')
    test_load_references()
    test_update_references()

    logger.info("ending sort_dqm_json_reference_updates.py")
