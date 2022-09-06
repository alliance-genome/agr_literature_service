import argparse
import sys
import json
import logging
import logging.config
from os import environ

from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_session
from agr_literature_service.api.models import ReferenceModel, CrossReferenceModel,\
    ReferenceCommentAndCorrectionModel

# pipenv run python post_comments_corrections_to_api.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/all_pmids > log_post_comments_corrections_to_api
# enter a file of pmids as an argument, sanitize, post to api
# 1 hour 19 minutes for 669998 pmids and 6268 rows created


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format= '%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def get_pmid_to_reference(db_session, pmids):

    query = db_session.query(
        CrossReferenceModel.curie,
        ReferenceModel.curie
    ).join(
        ReferenceModel.cross_reference
    ).filter(
        CrossReferenceModel.curie.in_(pmids)
    )

    results = query.all()

    pmid_curie_dict = {}

    for result in results:
        if result[0] not in pmid_curie_dict or pmid_curie_dict[result[0]] is None:
            pmid_curie_dict[result[0]] = result[1]
    # json_object = json.dumps(pmid_curie_dict, indent=4)
    # print(json_object)

    return pmid_curie_dict


def post_comments_corrections(pmids_wanted):      # noqa: C901
    """

    :param pmids_wanted:
    :return:
    """

    logger.info(pmids_wanted)

    allowed_com_cor_types = ['CommentOn', 'ErratumFor', 'ExpressionOfConcernFor', 'ReprintOf',
                             'RepublishedFrom', 'RetractionOf', 'UpdateOf']
    remap_com_cor_types = dict()
    remap_com_cor_types['CommentIn'] = 'CommentOn'
    remap_com_cor_types['ErratumIn'] = 'ErratumFor'
    remap_com_cor_types['ExpressionOfConcernIn'] = 'ExpressionOfConcernFor'
    remap_com_cor_types['ReprintIn'] = 'ReprintOf'
    remap_com_cor_types['RepublishedIn'] = 'RepublishedFrom'
    remap_com_cor_types['RetractionIn'] = 'RetractionOf'
    remap_com_cor_types['UpdateIn'] = 'UpdateOf'

    mappings_set = set()
    pmids_in_xml = set()
    for pmid in pmids_wanted:
        pubmed_json_filepath = base_path + 'pubmed_json/' + pmid + '.json'
        try:
            pubmed_data = dict()
            with open(pubmed_json_filepath, 'r') as f:
                pubmed_data = json.load(f)
                f.close()
            if 'commentsCorrections' in pubmed_data:
                for com_cor_type in pubmed_data['commentsCorrections']:
                    reverse = False
                    for other_pmid in pubmed_data['commentsCorrections'][com_cor_type]:
                        if com_cor_type in remap_com_cor_types:
                            reverse = True
                            com_cor_type = remap_com_cor_types[com_cor_type]
                        if com_cor_type in allowed_com_cor_types:
                            primary = 'PMID:' + pmid
                            secondary = 'PMID:' + other_pmid
                            pmids_in_xml.add(primary)
                            pmids_in_xml.add(secondary)
                            if reverse is True:
                                primary = 'PMID:' + other_pmid
                                secondary = 'PMID:' + pmid
                            mappings_set.add(primary + '\t' + secondary + '\t' + com_cor_type)
        except IOError:
            print(f"{pubmed_json_filepath} not found in filesystem")

    reference_to_curie = dict()

    # generating only needed pmid mappings of xref to reference curie through sqlalchemy

    db_session = create_postgres_session(False)

    reference_to_curie = get_pmid_to_reference(db_session, list(pmids_in_xml))

    mappings = sorted(mappings_set)
    # counter = 0
    for mapping in mappings:
        map_data = mapping.split("\t")
        primary_pmid = map_data[0]
        secondary_pmid = map_data[1]
        com_cor_type = map_data[2]
        primary_curie = ''
        secondary_curie = ''
        if primary_pmid in reference_to_curie:
            primary_curie = reference_to_curie[primary_pmid]
        if secondary_pmid in reference_to_curie:
            secondary_curie = reference_to_curie[secondary_pmid]
        if primary_curie == '':
            logger.info(f"ERROR {mapping} : {primary_pmid} does not map to an AGR Reference curie")
        if secondary_curie == '':
            logger.info(f"ERROR {secondary_pmid} does not map to an AGR Reference curie")
        if primary_curie != '' and secondary_curie != '':
            new_entry = dict()
            new_entry['reference_curie_from'] = primary_curie
            new_entry['reference_curie_to'] = secondary_curie
            new_entry['reference_comment_and_correction_type'] = com_cor_type

            # debug: output what is sent to API after converting file data
            # json_object = json.dumps(new_entry, indent=4)
            # print(json_object)

            try:
                x = ReferenceCommentAndCorrectionModel(**new_entry)
                db_session.add(x)
                logger.info("The comment/correction row has been added into database for PMID " + str(primary_pmid))
            except Exception as e:
                logger.info("An error occurred when adding a comment/correction row into database for PMID " + str(primary_pmid) + " " + str(e))

    db_session.commit()
    db_session.close()


if __name__ == "__main__":
    """
    call main start function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--generate-pmid-data', action='store_true', help='generate pmid outputs')
    parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
    parser.add_argument('-m', '--mod', action='store', help='which mod, use all or leave blank for all')
    parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')

    args = vars(parser.parse_args())

    pmids_wanted = []

    # python post_comments_corrections_to_api.py -c 1234 4576 1828
    if args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids_wanted.append(pmid)

    elif args['file']:
        logger.info("Processing file input from %s", args['file'])
        base_path = environ.get('XML_PATH')
        filename = base_path + args['file']
        try:
            with open(filename, 'r') as fp:
                pmid = fp.readline()
                while pmid:
                    pmids_wanted.append(pmid.rstrip())
                    pmid = fp.readline()
                fp.close()
        except IOError:
            logger.info("No input file at %s", filename)

    else:
        logger.info("Must enter a PMID through command line")

    post_comments_corrections(pmids_wanted)

    logger.info("Done Processing")
