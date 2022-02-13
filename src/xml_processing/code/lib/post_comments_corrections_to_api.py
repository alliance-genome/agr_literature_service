"""
pipenv run python post_comments_corrections_to_api.py -f /home/azurebrd/git/agr_
literature_service_demo/src/xml_processing/inputs/all_pmids > log_post_comments_corrections_to_api
enter a file of pmids as an argument, sanitize, post to api
1 hour 19 minutes for 669998 pmids and 6268 rows created
"""


import argparse
import json
import logging
import logging.config
from os import environ, path

from helper_file_processing import generate_cross_references_file, load_ref_xref
from helper_post_to_api import (
    generate_headers,
    get_authentication_token,
    process_api_request,
)

log_file_path = path.join(path.dirname(path.abspath(__file__)), "../logging.conf")
logging.config.fileConfig(log_file_path)
logger = logging.getLogger("post_comments_corrections_to_api")

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--generate-pmid-data", action="store_true", help="generate pmid outputs")
parser.add_argument("-f", "--file", action="store", help="take input from REFERENCE files in full path")
parser.add_argument("-m", "--mod", action="store", help="which mod, use all or leave blank for all")
parser.add_argument("-c", "--commandline", nargs="*", action="store", help="take input from command line flag",)

args = vars(parser.parse_args())


def post_comments_corrections(pmids_wanted):  # noqa: C901
    """

    :param pmids_wanted:
    :return:
    """

    logger.info(pmids_wanted)

    api_port = environ.get("API_PORT")
    base_path = environ.get("XML_PATH")

    token = get_authentication_token()
    headers = generate_headers(token)

    allowed_com_cor_types = ["CommentOn", "ErratumFor", "ExpressionOfConcernFor", "ReprintOf",
                             "RepublishedFrom", "RetractionOf", "UpdateOf"]
    remap_com_cor_types = {"CommentIn": "CommentOn", "ErratumIn": "ErratumFor",
                           "ExpressionOfConcernIn": "ExpressionOfConcernFor",
                           "ReprintIn": "ReprintOf", "RepublishedIn": "RepublishedFrom",
                           "RetractionIn": "RetractionOf", "UpdateIn": "UpdateOf"}

    reference_to_curie = {}
    # previously loading from reference_primary_id_to_curie from past run of this script
    # reference_primary_id_to_curie_file = base_path + 'reference_primary_id_to_curie'
    # if path.isfile(reference_primary_id_to_curie_file):
    #     with open(reference_primary_id_to_curie_file, 'r') as read_fh:
    #         for line in read_fh:
    #             line_data = line.split("\t")
    #             if line_data[0]:
    #                 reference_to_curie[line_data[0]] = line_data[1].rstrip()
    #         read_fh.close

    # this updates from references in the database, and takes 88 seconds. if updating this script,
    # comment it out after running it once
    generate_cross_references_file("reference")
    xref_ref, ref_xref_valid, ref_xref_obsolete = load_ref_xref("reference")
    reference_to_curie = {}
    for prefix in xref_ref:
        for identifier in xref_ref[prefix]:
            xref_curie = prefix + ":" + identifier
            reference_to_curie[xref_curie] = xref_ref[prefix][identifier]

    mappings_set = set()
    for pmid in pmids_wanted:
        pubmed_json_filepath = base_path + "pubmed_json/" + pmid + ".json"
        try:
            pubmed_data = {}
            with open(pubmed_json_filepath) as f:
                pubmed_data = json.load(f)
            if "commentsCorrections" in pubmed_data:
                for com_cor_type in pubmed_data["commentsCorrections"]:
                    reverse = False
                    for other_pmid in pubmed_data["commentsCorrections"][com_cor_type]:
                        if com_cor_type in remap_com_cor_types:
                            reverse = True
                            com_cor_type = remap_com_cor_types[com_cor_type]
                        if com_cor_type in allowed_com_cor_types:
                            primary = pmid
                            secondary = other_pmid
                            if reverse is True:
                                primary = other_pmid
                                secondary = pmid
                            mappings_set.add(primary + "\t" + secondary + "\t" + com_cor_type)
        except IOError:
            print(pubmed_json_filepath + " not found in filesystem")

    api_server = environ.get("API_SERVER", "localhost")
    url = "http://" + api_server + ":" + api_port + "/reference_comment_and_correction/"
    mappings = sorted(mappings_set)
    # counter = 0
    for mapping in mappings:
        # print(mapping)
        # only take a couple of samples for testing
        # counter += 1
        # if counter > 2:
        #     break

        map_data = mapping.split("\t")
        primary_pmid = "PMID:" + map_data[0]
        secondary_pmid = "PMID:" + map_data[1]
        com_cor_type = map_data[2]
        primary_curie = ""
        secondary_curie = ""
        if primary_pmid in reference_to_curie:
            primary_curie = reference_to_curie[primary_pmid]
        if secondary_pmid in reference_to_curie:
            secondary_curie = reference_to_curie[secondary_pmid]
        if primary_curie == "":
            # print('ERROR ' + primary_pmid + ' does not map to an AGR Reference curie')
            logger.info("ERROR %s : %s does not map to an AGR Reference curie", mapping, primary_pmid)
        if secondary_curie == "":
            # print('ERROR ' + secondary_pmid + ' does not map to an AGR Reference curie')
            logger.info(
                "ERROR %s does not map to an AGR Reference curie", secondary_pmid
            )
        if primary_curie != "" and secondary_curie != "":
            # print(primary_curie + '\t' + secondary_curie + '\t' + com_cor_type)
            # print('primary ' + primary_pmid + ' maps to ' + primary_curie)
            # print('secondary ' + secondary_pmid + ' maps to ' + secondary_curie)
            # print('com_cor_type ' + com_cor_type)
            new_entry = {"reference_curie_from": primary_curie, "reference_curie_to": secondary_curie,
                         "reference_comment_and_correction_type": com_cor_type}

            # output what is sent to API after converting file data
            # json_object = json.dumps(new_entry, indent=4)
            # print(json_object)

            api_response_tuple = process_api_request("POST", url, headers, new_entry, primary_pmid, None, None)
            headers = api_response_tuple[0]
            response_text = api_response_tuple[1]
            response_status_code = api_response_tuple[2]
            log_info = api_response_tuple[3]
            response_dict = json.loads(response_text)

            if log_info:
                logger.info(log_info)

            if response_status_code == 201:
                logger.info("%s\t%s\t%s\t%s\t%s\ttext %s\tstatus_code %s",
                            primary_pmid, primary_curie, secondary_pmid,
                            secondary_curie, com_cor_type, response_text,
                            response_status_code)
            else:
                logger.info("api error %s primary pmid %s message %s",
                             str(response_status_code), primary_pmid, response_dict["detail"])


if __name__ == "__main__":
    """
    call main start function
    """

    pmids_wanted = []

    # python post_comments_corrections_to_api.py -c 1234 4576 1828
    if args["commandline"]:
        logger.info("Processing commandline input")
        for pmid in args["commandline"]:
            pmids_wanted.append(pmid)
    elif args["file"]:
        logger.info("Processing file input from %s", args["file"])
        base_path = environ.get("XML_PATH")
        filename = base_path + args["file"]
        try:
            pmids_wanted = open(filename).read().splitlines()
        except IOError:
            logger.info("No input file at %s", filename)

    else:
        logger.info("Must enter a PMID through command line")

    post_comments_corrections(pmids_wanted)

    logger.info("Done processing")
