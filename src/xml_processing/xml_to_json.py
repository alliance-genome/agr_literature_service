import json 
import xmltodict 

#  python get_pubmed_xml.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set



import argparse

from os import path
import logging
import logging.config



# Need to set up a queue that queries postgres to get a list of pubmed id that don't have a pubmed final flag
# Need to set up an S3 bucket to store xml
# Need to set up flags to take in pmids from postgres queue, file in filesystem, file in URL, list from command line

# to get set of pmids with search term 'elegans'
# https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=elegans&retmax=100000000

pmids = []


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


parser = argparse.ArgumentParser()
parser.add_argument('-c', '--commandline', nargs='*', action='store', help='take input from command line flag')
parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
parser.add_argument('-f', '--file', action='store', help='take input from entries in file with full path')
parser.add_argument('-u', '--url', action='store', help='take input from entries in file at url')
parser.add_argument('-s', '--sample', action='store_true', help='test sample input from hardcoded entries')

args = vars(parser.parse_args())

# todo: save this in an env variable
storage_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/pubmed_xml/'

# def download_pubmed_xml():
#   for pmid in pmids:
# #    add some validation here
#     url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=" + pmid + "&retmode=xml"
#     filename = storage_path + pmid + '.xml'
# #     print url
# #     print filename
#     logger.info("Downloading %s into %s", url, filename)
#     urllib.urlretrieve(url, filename)
#     time.sleep( 5 )

  
  
def print_titles():
    # open input xml file and read data in form of python dictionary using xmltodict module 
    for pmid in pmids:
        filename = storage_path + pmid + '.xml'
        with open(filename) as xml_file: 
              
            data_dict = xmltodict.parse(xml_file.read()) 
            xml_file.close() 
        
            print (pmid)
            print (data_dict["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]["Article"]["ArticleTitle"])
        #     if (data_dict["PubmedArticleSet"]["PubmedArticle"]["MedlineCitation"]["Article"]["ArticleTitle"])
            
        # {
        #     "PubmedArticleSet": {
        #         "PubmedArticle": {
        #             "MedlineCitation": {
        #                 "@Owner": "PIP",
        #                 "@Status": "MEDLINE",
        #                 "Article": {
        #                     "@PubModel": "Print",
        #                     "ArticleTitle"
              
            # generate the object using json.dumps()  
            # corresponding to json data 
              
            # minified
            # json_data = json.dumps(data_dict) 
        
            # pretty-print
            json_data = json.dumps(data_dict, indent=4, sort_keys=True) 
        
            # Write the json data to output  
            # json file 
        json_storage_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/pubmed_json/'
        json_filename = json_storage_path + pmid + '.json'
        with open(json_filename, "w") as json_file: 
            json_file.write(json_data) 
            json_file.close() 


if __name__ == "__main__":
    """ call main start function """

#    python xml_to_json.py -d
    if args['database']:
        logger.info("Processing database entries")

#     python xml_to_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
    elif args['file']:
        logger.info("Processing file input from %s", args['file'])
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids.append(pmid.rstrip())
                pmid = fp.readline()

#     python xml_to_json.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
    elif args['url']:
        logger.info("Processing url input from %s", args['url'])
        req = urllib.urlopen(args['url'])
        data = req.read()
        lines = data.splitlines()
        for pmid in lines:
            pmids.append(pmid)

#    python xml_to_json.py -c 1234 4576 1828
    elif args['commandline']:
        logger.info("Processing commandline input")
        for pmid in args['commandline']:
            pmids.append(pmid)

#    python xml_to_json.py -s
    elif args['sample']:
        logger.info("Processing hardcoded sample input")
        pmid = '12345678'
        pmids.append(pmid)
        pmid = '12345679'
        pmids.append(pmid)
        pmid = '12345680'
        pmids.append(pmid)

    else:
        logger.info("Processing database entries")

#     download_pubmed_xml()
    print_titles()
