"""
# search all pubmed_xml/*.xml for types of
# <CommentsCorrections RefType="<TO_FIND>">
# <PublicationType.*?><TO_FIND></PublicationType>
# for curators to decide what we want to capture

# pipenv run python find_pubmed_type.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
"""

import argparse
import logging
import logging.config
import re
import urllib.request
from os import environ, makedirs, path
import calendar

# from dotenv import load_dotenv
# load_dotenv()

pmids = []

log_file_path = path.join(path.dirname(path.abspath(__file__)), "../logging.conf")
logging.config.fileConfig(log_file_path)
logger = logging.getLogger("literature logger")


parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--commandline",
    nargs="*",
    action="store",
    help="take input from command line flag",
)
parser.add_argument(
    "-d", "--database", action="store_true", help="take input from database query"
)
parser.add_argument(
    "-f",
    "--file",
    action="store",
    help="take input from entries in file with full path",
)
parser.add_argument("-r", "--restapi", action="store", help="take input from rest api")
parser.add_argument(
    "-s",
    "--sample",
    action="store_true",
    help="test sample input from hardcoded entries",
)
parser.add_argument(
    "-u", "--url", action="store", help="take input from entries in file at url"
)
args = vars(parser.parse_args())

# todo: save this in an env variable
base_path = environ.get("XML_PATH", "")

# def represents_int(s):
#     """
#
#     :param s:
#     :return:
#     """
#
#     try:
#         int(s)
#         return True
#     except ValueError:
#         return False


# def month_name_to_number_string(string):
#     """
#
#     :param string:
#     :return:
#     """
#
#     m = {
#         "jan": "01",
#         "feb": "02",
#         "mar": "03",
#         "apr": "04",
#         "may": "05",
#         "jun": "06",
#         "jul": "07",
#         "aug": "08",
#         "sep": "09",
#         "oct": "10",
#         "nov": "11",
#         "dec": "12",
#     }
#     s = string.strip()[:3].lower()
#
#     try:
#         out = m[s]
#         return out
#     except ValueError:
#         raise ValueError(string + " is not a month")


def get_year_month_day_from_xml_date(pub_date):
    """

    :param pub_date:
    :return:
    """

    date_list = []
    year = ""
    month = "01"
    day = "01"
    year_re_output = re.search("<Year>(.+?)</Year>", pub_date)
    if year_re_output is not None:
        year = year_re_output.group(1)
    month_re_output = re.search("<Month>(.+?)</Month>", pub_date)
    if month_re_output is not None:
        month_text = month_re_output.group(1)
        if type(month_text) == int:
            month = month_text
        else:
            month = list(calendar.month_abbr).index(month_text)
    day_re_output = re.search("<Day>(.+?)</Day>", pub_date)
    if day_re_output is not None:
        day = day_re_output.group(1)
    date_list.append(year)
    date_list.append(month)
    date_list.append(day)
    return date_list


def get_medline_date_from_xml_date(pub_date):
    """

    :param pub_date:
    :return:
    """

    medline_re_output = re.search("<MedlineDate>(.+?)</MedlineDate>", pub_date)
    if medline_re_output is not None:
        return medline_re_output.group(1)


def generate_json():
    """

    open input xml file and read data in form of python dictionary using xmltodict module
    storage_path = base_path + 'pubmed_xml_20210322/'
    json_storage_path = base_path + 'pubmed_json_20210322/'

    :return:
    """

    publication_type_set = set()
    comments_ref_type_set = set()

    storage_path = base_path + "pubmed_xml/"
    json_storage_path = base_path + "pubmed_json/"
    if not path.exists(storage_path):
        makedirs(storage_path)
    if not path.exists(json_storage_path):
        makedirs(json_storage_path)
    for pmid in pmids:
        filename = storage_path + pmid + ".xml"
        # if getting pmids from directories split into multiple sub-subdirectories
        # filename = get_path_from_pmid(pmid, 'xml')
        if not path.exists(filename):
            continue
        # logger.info("processing %s", filename)
        with open(filename) as xml_file:

            xml = xml_file.read()
            # print(xml)

            # xmltodict is treating html markup like <i>text</i> as xml,
            # which is creating mistaken structure in the conversion.
            # may be better to parse full xml instead.
            # data_dict = xmltodict.parse(xml_file.read())
            xml_file.close()

            # print(pmid)

            if re.findall("<PublicationType>(.+?)</PublicationType>", xml):
                types_group = re.findall("<PublicationType>(.+?)</PublicationType>", xml)
                for type in types_group:
                    publication_type_set.add(type)
            elif re.findall('<PublicationType UI=".*?">(.+?)</PublicationType>', xml):
                types_group = re.findall('<PublicationType UI=".*?">(.+?)</PublicationType>', xml)
                for type in types_group:
                    publication_type_set.add(type)
                # publication_type_set.add(types_group)

            if re.findall('<CommentsCorrections RefType="(.+?)">', xml):
                types_group = re.findall('<CommentsCorrections RefType="(.+?)">', xml)
                for type in types_group:
                    comments_ref_type_set.add(type)

    for comments_ref_type in comments_ref_type_set:
        logger.info("comments_ref_type %s", comments_ref_type)

    for publication_type in publication_type_set:
        logger.info("publication_type %s", publication_type)


if __name__ == "__main__":
    """
    call main start function
    """

    #    python find_pubmed_type.py -d
    if args["database"]:
        logger.info("Processing database entries")

    elif args["restapi"]:
        logger.info("Processing rest api entries")

    # python find_pubmed_type.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set
    elif args["file"]:
        logger.info("Processing file input from %s", args["file"])
        pmids = open(args["file"]).read().splitlines()

    # python find_pubmed_type.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
    elif args["url"]:
        logger.info("Processing url input from %s", args["url"])
        req = urllib.request.urlopen(args["url"])
        data = req.read()
        lines = data.splitlines()
        for pmid in lines:
            pmids.append(str(int(pmid)))

    #    python find_pubmed_type.py -c 1234 4576 1828
    elif args["commandline"]:
        logger.info("Processing commandline input")
        for pmid in args["commandline"]:
            pmids.append(pmid)

    #    python find_pubmed_type.py -s
    elif args["sample"]:
        logger.info("Processing hardcoded sample input")
        pmid = "12345678"
        pmids.append(pmid)
        pmid = "12345679"
        pmids.append(pmid)
        pmid = "12345680"
        pmids.append(pmid)

    else:
        logger.info("Processing database entries")

    generate_json()
    logger.info("Done converting XML to JSON")

# capture ISSN / NLM
#         <MedlineJournalInfo>
#             <Country>England</Country>
#             <MedlineTA>J Travel Med</MedlineTA>
#             <NlmUniqueID>9434456</NlmUniqueID>
#             <ISSNLinking>1195-1982</ISSNLinking>
#         </MedlineJournalInfo>
# not from
#             <Journal>
#                 <ISSN IssnType="Electronic">1708-8305</ISSN>
