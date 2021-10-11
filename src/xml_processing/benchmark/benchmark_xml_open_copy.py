import json
import urllib


# python3 xml_to_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
#
# benchmark using python to open xml files, copy to temp_benchmark_copy/


import argparse
import re

from os import environ, path
import logging
import logging.config

from dotenv import load_dotenv

load_dotenv()

pmids = []


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')

args = vars(parser.parse_args())

# todo: save this in an env variable
# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH')


def generate_json():
    # open xml files, copy to temp_benchmark_copy/
    for pmid in pmids:
        storage_path = base_path + 'pubmed_xml/'
        # storage_path = base_path + 'pubmed_xml_20210205/'
        filename = storage_path + pmid + '.xml'
        if not path.exists(filename):
            continue
        with open(filename) as xml_file:

            xml = xml_file.read()
            # print (xml)

            # xmltodict is treating html markup like <i>text</i> as xml, which is creating mistaken structure in the conversion.
            # may be better to parse full xml instead.
            # data_dict = xmltodict.parse(xml_file.read())
            xml_file.close()

            # print (pmid)
            data_dict = dict()

            # Write the json data to output json file
# UNCOMMENT TO write to json directory
            # json_storage_path = base_path + 'pubmed_json/'
            json_storage_path = base_path + 'temp_benchmark_copy/'
            json_filename = json_storage_path + pmid + '.json'
            with open(json_filename, "w") as json_file:
                # json_file.write(json_data)
                json_file.write(xml)
                json_file.close()


if __name__ == "__main__":
    """ call main start function """

#     python xml_to_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
    if args['file']:
        logger.info("Processing file input from %s", args['file'])
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids.append(pmid.rstrip())
                pmid = fp.readline()

    else:
        logger.info("Processing database entries")

    generate_json()
    logger.info("Done opening and copying files")
