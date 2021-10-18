from os import path
from os import environ
import json
import requests
import argparse
import logging
import logging.config

from helper_post_to_api import generate_headers, update_token

# from sanitize_pubmed_json import sanitize_pubmed_json_list
# from post_reference_to_api import post_references

# pipenv run python post_comments_corrections_to_api.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/all_pmids > log_post_comments_corrections_to_api
# enter a file of pmids as an argument, sanitize, post to api
# 1 hour 19 minutes for 669998 pmids and 6268 rows created


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('post_comments_corrections_to_api')

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--generate-pmid-data', action='store_true', help='generate pmid outputs')
parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
parser.add_argument('-m', '--mod', action='store', help='which mod, use all or leave blank for all')
parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')

args = vars(parser.parse_args())



def post_comments_corrections(pmids_wanted):      # noqa: C901
    """

    :param pmids_wanted:
    :return:
    """

    logger.info(pmids_wanted)

    api_port = environ.get('API_PORT')
    # base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
    base_path = environ.get('XML_PATH')

    okta_file = base_path + 'okta_token'
    token = ''
    if path.isfile(okta_file):
        with open(okta_file, 'r') as okta_fh:
            token = okta_fh.read().replace("\n", "")
            okta_fh.close
    else:
        token = update_token()
    headers = generate_headers(token)

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

    reference_to_curie = dict()
    reference_primary_id_to_curie_file = base_path + 'reference_primary_id_to_curie'
    if path.isfile(reference_primary_id_to_curie_file):
        with open(reference_primary_id_to_curie_file, 'r') as read_fh:
            for line in read_fh:
                line_data = line.split("\t")
                if line_data[0]:
                    reference_to_curie[line_data[0]] = line_data[1].rstrip()
            read_fh.close

    mappings_set = set()
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
                            primary = pmid
                            secondary = other_pmid
                            if reverse is True:
                                primary = other_pmid
                                secondary = pmid
                            mappings_set.add(primary + '\t' + secondary + '\t' + com_cor_type)
        except IOError:
            print(pubmed_json_filepath + ' not found in filesystem')

    url = 'http://localhost:' + api_port + '/reference_comment_and_correction/'
    mappings = sorted(mappings_set)
    for mapping in mappings:
        # print(mapping)
        map_data = mapping.split("\t")
        primary_pmid = 'PMID:' + map_data[0]
        secondary_pmid = 'PMID:' + map_data[1]
        com_cor_type = map_data[2]
        primary_curie = ''
        secondary_curie = ''
        if primary_pmid in reference_to_curie:
            primary_curie = reference_to_curie[primary_pmid]
        if secondary_pmid in reference_to_curie:
            secondary_curie = reference_to_curie[secondary_pmid]
        if primary_curie == '':
            # print('ERROR ' + primary_pmid + ' does not map to an AGR Reference curie')
            logger.info("ERROR %s : %s does not map to an AGR Reference curie", mapping, primary_pmid)
        if secondary_curie == '':
            # print('ERROR ' + secondary_pmid + ' does not map to an AGR Reference curie')
            logger.info("ERROR %s does not map to an AGR Reference curie", secondary_pmid)
        if primary_curie != '' and secondary_curie != '':
            # print(primary_curie + '\t' + secondary_curie + '\t' + com_cor_type)
            # print('primary ' + primary_pmid + ' maps to ' + primary_curie)
            # print('secondary ' + secondary_pmid + ' maps to ' + secondary_curie)
            # print('com_cor_type ' + com_cor_type)
            new_entry = dict()
            new_entry['reference_curie_from'] = primary_curie
            new_entry['reference_curie_to'] = secondary_curie
            new_entry['reference_comment_and_correction_type'] = com_cor_type

# uncomment to test
            post_return = requests.post(url, headers=headers, json=new_entry)
            # response_dict = json.loads(post_return.text)
            # print(primary_curie + "\t" + secondary_curie + "\ttext " + str(post_return.text))
            # print(primary_curie + "\t" + secondary_curie + "\tstatus_code " + str(post_return.status_code))
            logger.info("%s\t%s\t%s\t%s\t%s\ttext %s\tstatus_code %s", primary_pmid, primary_curie, secondary_pmid, secondary_curie, com_cor_type, str(post_return.text), str(post_return.status_code))

# delete later
#                 if (post_return.status_code == 201):
#                     response_dict = response_dict.replace('"', '')
#                     for identifier in identifiers:
#                         logger.info("I %s\t%s", identifier, response_dict)
#                         mapping_fh.write("%s\t%s\n" % (identifier, response_dict))
#                 # if making multiple runs on data that has already gone into api
#                 # elif (post_return.status_code == 409):
#                 #     continue
#                 else:
#                     logger.info("ERROR %s primaryId %s message %s", post_return.status_code, primary_id, response_dict['detail'])
#                     error_fh.write("ERROR %s primaryId %s message %s\n" % (post_return.status_code, primary_id, response_dict['detail']))


if __name__ == "__main__":
    """
    call main start function
    """

    pmids_wanted = []

#    python post_comments_corrections_to_api.py -c 1234 4576 1828
    if args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids_wanted.append(pmid)

    elif args['file']:
        logger.info("Processing file input from %s", args['file'])
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids_wanted.append(pmid.rstrip())
                pmid = fp.readline()

    else:
        logger.info("Must enter a PMID through command line")

    post_comments_corrections(pmids_wanted)

    logger.info("Done Processing")
