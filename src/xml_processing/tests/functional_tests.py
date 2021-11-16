import json
import requests

import re

# import pytest    # don't know how this works, pytest not installed on dev server

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
# based off of   pipenv run python functional_tests.py


def test_load_references():
    """
    Load cross_references to database, and sample.json mapping them to the agr reference curie and the types of checks each should have.  Query each reference from the database, and run the appropriate test.

    """

    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref('reference')
    input_file = 'inputs/sample_dqm_load.json'
    sample_json = load_sample_json(input_file)
    if not sample_json:
        return
    agr_wanted = dict()
    for entry in sample_json['data']:
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
        if not agr_found:
            continue
        if agr not in agr_wanted:
            agr_wanted[agr] = set()
        if 'check' in entry:
            for check in entry['check']:
                agr_wanted[agr].add(check)
    api_port = environ.get('API_PORT')
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
        url = 'http://localhost:' + api_port + '/reference/' + agr
        logger.info("get AGR reference info from database %s", url)
        # TODO
        # put this back when database works
        get_return = requests.get(url)
        db_entry = json.loads(get_return.text)
        # logger.info(db_entry)
        for check in agr_wanted[agr]:
            test_result = check_test(db_entry, check)
            logger.info("agr %s check %s result %s", agr, check, test_result)


def erratum_check(agr_data):
    """
    future: check a database reference has comment_and_corrections connection to another reference

    :param agr_data:
    :return:
    """

    return 'Success: Errata references created, but connections not created yet, add to sample_reference_populate_load.sh later'


def category_book_check(agr_data):
    """
    check a database reference has a category of 'book' from PubMed XML <BookDocument>

    :param agr_data:
    :return:
    """

    if 'category' in agr_data:
        if agr_data['category'] == 'book':
            return 'Success'
    return 'Failure'


def title_check(agr_data):
    """
    check a database reference has a title

    :param agr_data:
    :return:
    """

    if 'title' in agr_data:
        if agr_data['title'] != '':
            return 'Success'
    return 'Failure'


def author_name_check(agr_data):
    """
    check a database reference has all authors with names, because <CollectiveName> in PubMed XML is not standard author pattern

    :param agr_data:
    :return:
    """

    if 'authors' not in agr_data:
        return 'Failure: No authors found'
    result = 'Success'
    for author in agr_data['authors']:
        has_name = False
        if 'name' in author:
            if author['name'] != '':
                has_name = True
        if not has_name:
            result = 'Failure'
    return result


def author_affiliation_check(agr_data):
    """
    check a database reference has an author with an affiliation

    :param agr_data:
    :return:
    """

    if 'authors' not in agr_data:
        return 'Failure: No authors found'
    for author in agr_data['authors']:
        if 'affiliation' in author:
            for affiliation in author['affiliation']:
                if affiliation != '':
                    return 'Success'
    return 'Failure'


def author_orcid_check(agr_data):
    """
    check a database reference has an author with an orcid

    :param agr_data:
    :return:
    """

    if 'authors' not in agr_data:
        return 'Failure: No authors found'
    for author in agr_data['authors']:
        if 'orcid' in author:
            if 'curie' in author['orcid']:
                if author['orcid']['curie'] != '':
                    return 'Success'
    return 'Failure'


def xref_mods_check(agr_data):
    """
    check a database reference has cross_references to six base mods

    :param agr_data:
    :return:
    """

    prefixes = set()
    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                prefix, identifier, separator = split_identifier(xref['curie'])
                prefixes.add(prefix)
        if 'WB' in prefixes and 'MGI' in prefixes and 'FB' in prefixes and 'ZFIN' in prefixes and 'RGD' in prefixes and 'MGI' in prefixes:
            return 'Success'
    prefixes_found = ', '.join(sorted(prefixes))
    return 'Failure: only found ' + prefixes_found


def html_abstract_check(agr_data):
    """
    future: check a database reference does not have html in the abstract

    :param agr_data:
    :return:
    """

    return 'Success: Abstracts do have html, have not decided whether they should not'


def html_doi_check(agr_data):
    """
    check a database reference does not have html in the doi

    :param agr_data:
    :return:
    """

    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                prefix, identifier, separator = split_identifier(xref['curie'])
                if prefix == 'DOI':
                    if re.search('&', xref['curie']):
                        return 'Failure: ' + xref['curie'] + ' has &, could be html in doi'
    return 'Success'


def keywords_check(agr_data):
    """
    check a database reference does have a keyword

    :param agr_data:
    :return:
    """

    if 'keywords' in agr_data:
        for keyword in agr_data['keywords']:
            if keyword != '':
                return 'Success'
    return 'Failure'


def has_doi_check(agr_data):
    """
    check a database reference does have a doi

    :param agr_data:
    :return:
    """

    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                prefix, identifier, separator = split_identifier(xref['curie'])
                if prefix == 'DOI':
                    return 'Success'
    return 'Failure'


def has_pmid_check(agr_data):
    """
    check a database reference does have a pmid

    :param agr_data:
    :return:
    """

    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                prefix, identifier, separator = split_identifier(xref['curie'])
                if prefix == 'PMID':
                    return 'Success: PMID found, PubMed XML might not have it'
    return 'Failure'


def no_pmid_check(agr_data):
    """
    check a database reference does not have a pmid

    :param agr_data:
    :return:
    """

    if 'cross_references' in agr_data:
        for xref in agr_data['cross_references']:
            if 'curie' in xref:
                prefix, identifier, separator = split_identifier(xref['curie'])
                if prefix == 'PMID':
                    return 'Failure: PMID found, this reference should not have one'
    return 'Success'


def check_test(agr_data, check):
    """
    case switch for different types of tests

    :param agr_data:
    :param check:
    :return:
    """

    options = {
        'VernacularTitle': title_check,
        'BookTitle': title_check,
        'ArticleTitle': title_check,
        'CollectiveName': author_name_check,
        'AggregateMods': xref_mods_check,
        'html_doi': html_doi_check,
        'html_abstract': html_abstract_check,
        'BookDocument': category_book_check,
        'DOI': has_doi_check,
        'PMID': has_pmid_check,
        'no_pmid': no_pmid_check,
        'ORCID': author_orcid_check,
        'AffiliationInfo': author_affiliation_check,
        'Keywords': keywords_check,
        'has_erratum': erratum_check,
        'FAIL': title_check
    }
    if check in options:
        test_result = options[check](agr_data)
        return test_result
    else:
        return 'test not found'


if __name__ == "__main__":
    """
    call main start function
    """

    logger.info("starting functional_tests.py")

    assert 1 == 1

    # run this once after data is loaded
    generate_cross_references_file('reference')
    test_load_references()

    logger.info("ending sort_dqm_json_reference_updates.py")
