import json 
import xmltodict 

#  python generate_pubmed_nlm_resource

# https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt

# todo, download J_Medline file dynamically

import re
import urllib

from os import path
import logging
import logging.config

import argparse




# pmids = []


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-l', '--input-localfile', action='store_true', help='take input from local file')
parser.add_argument('-u', '--input-url', action='store_true', help='take input from url')
args = vars(parser.parse_args())




# todo: save this in an env variable
storage_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/pubmed_resource_json/'



def populate_nlm_info(file_data):
    nlm_info = []
    logger.info("Generating NLM data from file")
#     counter = 0

#     filename = 'J_Medline.txt'
# 
#     with open(filename) as txt_file: 
#         if not path.exists(filename):
#             return "journal info file not found"
#         file_data = txt_file.read()
#         txt_file.close() 

    entries = file_data.split('\n--------------------------------------------------------\n')

    for entry in entries:
#         counter = counter + 1
#         if counter > 5:
#             continue
        nlm = ''
        if re.search("NlmId: (.+)", entry):
            nlm_group = re.search("NlmId: (.+)", entry)
            nlm = nlm_group.group(1)
        if not nlm:
#             print "skip"
            continue
        data_dict = {}
        data_dict['primaryId'] = nlm
        if re.search("JournalTitle: (.+)", entry):
            title_group = re.search("JournalTitle: (.+)", entry)
            title = title_group.group(1)
            data_dict['title'] = title
        if re.search("IsoAbbr: (.+)", entry):
            iso_abbreviation_group = re.search("IsoAbbr: (.+)", entry)
            iso_abbreviation = iso_abbreviation_group.group(1)
            data_dict['isoAbbreviation'] = iso_abbreviation
        if re.search("MedAbbr: (.+)", entry):
            medline_abbreviation_group = re.search("MedAbbr: (.+)", entry)
            medline_abbreviation = medline_abbreviation_group.group(1)
            data_dict['medlineAbbreviation'] = medline_abbreviation
        if re.search("ISSN \(Print\): (.+)", entry):
            print_issn_group = re.search("ISSN \(Print\): (.+)", entry)
            print_issn = print_issn_group.group(1)
            data_dict['printISSN'] = print_issn
        if re.search("ISSN \(Online\): (.+)", entry):
            online_issn_group = re.search("ISSN \(Online\): (.+)", entry)
            online_issn = online_issn_group.group(1)
            data_dict['onlineISSN'] = online_issn

#             print nlm
#             data_dict['nlm'] = nlm
        nlm_info.append(data_dict)
#         print entry
    return nlm_info


  
def generate_json(nlm_info):
    logger.info("Generating JSON from NLM data and saving to outfile")
    json_data = json.dumps(nlm_info, indent=4, sort_keys=True) 

            # Write the json data to output json file 
# UNCOMMENT TO write to json directory
    output_json_file = storage_path + 'resource_pubmed_all.json'
    with open(output_json_file, "w") as json_file: 
        json_file.write(json_data) 
        json_file.close() 


def populate_from_url():
    url = "https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt"
    print url
    f = urllib.urlopen(url)
    file_data = f.read()
    return file_data

def populate_from_local_file():
    filename = 'J_Medline.txt'
    with open(filename) as txt_file: 
        if not path.exists(filename):
            return "journal info file not found"
        file_data = txt_file.read()
        txt_file.close() 
        return file_data




if __name__ == "__main__":
    """ call main start function """

    file_data = ''
    if args['input_url']:
        file_data = populate_from_url()
        logger.info("Processing input from url")
    elif args['input_localfile']:
        file_data = populate_from_local_file()
        logger.info("Processing input from local file")
    else:
        file_data = populate_from_url()
        logger.info("Processing input from url")

    nlm_info = populate_nlm_info(file_data)
    generate_json(nlm_info)

