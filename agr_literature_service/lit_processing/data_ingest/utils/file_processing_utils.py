import json
import logging
import tarfile
import gzip
import shutil
from urllib import request
import datetime
from os import environ, path, listdir, remove
import html
import re

from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

init_tmp_dir()

base_path = environ.get('XML_PATH')

logger = logging.getLogger(__name__)


def load_pubmed_resource_basic():
    """

    :return:
    """

    filename = base_path + 'pubmed_resource_json/resource_pubmed_all.json'
    f = open(filename)
    resource_data = json.load(f)
    pubmed_by_nlm = dict()
    nlm_by_issn = dict()
    for entry in resource_data:
        # primary_id = entry['primaryId']
        nlm = entry['nlm']
        pubmed_by_nlm[nlm] = entry
        if 'printISSN' in entry:
            pissn = entry['printISSN']
            if pissn in nlm_by_issn:
                if nlm not in nlm_by_issn[pissn]:
                    nlm_by_issn[pissn].append(nlm)
            else:
                nlm_by_issn[pissn] = [nlm]
        if 'onlineISSN' in entry:
            oissn = entry['onlineISSN']
            if oissn in nlm_by_issn:
                if nlm not in nlm_by_issn[oissn]:
                    nlm_by_issn[oissn].append(nlm)
            else:
                nlm_by_issn[oissn] = [nlm]
    return pubmed_by_nlm, nlm_by_issn


def save_resource_file(json_storage_path, pubmed_by_nlm, datatype):
    """

    :param json_storage_path:
    :param pubmed_by_nlm:
    :return:
    """

    pubmed_data = dict()
    pubmed_data['data'] = []
    for nlm in pubmed_by_nlm:
        pubmed_data['data'].append(pubmed_by_nlm[nlm])
    json_filename = json_storage_path + 'RESOURCE_' + datatype + '.json'
    write_json(json_filename, pubmed_data)


def write_json(json_filename, dict_to_output):
    """

    :param json_filename:
    :param dict_to_output:
    :return:
    """

    with open(json_filename, "w") as json_file:
        # not sure how to logger from imported function without breaking logger in main function
        # logger.info("Generating JSON for %s", json_filename)
        json_data = json.dumps(dict_to_output, indent=4, sort_keys=True)
        json_file.write(json_data)
        json_file.close()


def chunks(list, size):
    for i in range(0, len(list), size):
        yield list[i:i + size]


def download_file(url, file):

    try:
        logger.info("Downloading " + url)
        req = request.urlopen(url)
        data = req.read()
        with open(file, 'wb') as fh:
            fh.write(data)
    except Exception as e:
        logger.error("Error downloading the file: " + file + ". Error=" + str(e))


def gunzip_file(file_with_path, to_file_dir):

    try:
        file = tarfile.open(file_with_path)
        file.extractall(to_file_dir)
        return True
    except Exception as e:
        logger.error(e)
        return False


def gzip_file(file_with_path):

    gzip_file_with_path = None
    try:
        gzip_file_with_path = file_with_path + ".gz"
        with open(file_with_path, 'rb') as f_in, gzip.open(gzip_file_with_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        logging.error(e)
        gzip_file_with_path = None

    return gzip_file_with_path


def remove_old_files(dir_path, days_old):

    now = datetime.datetime.now()

    # iterate over all files in the directory
    for filename in listdir(dir_path):
        file_path = path.join(dir_path, filename)
        if path.isfile(file_path):
            file_mod_time = datetime.datetime.fromtimestamp(path.getmtime(file_path))
            delta = now - file_mod_time
            if delta.days > days_old:
                remove(file_path)


def classify_pmc_file(file_name, file_extension):

    """
    image_related_file_extensions = [
        'jpg', 'jpeg', 'gif', 'tif', 'tiff', 'png',
        'eps', 'ai', 'bmp', 'svg', 'webp', 'emf'
    ]
    """
    image_related_file_extensions = ['jpg', 'jpeg', 'gif', 'tif', 'tiff', 'png']
    if file_extension.lower() == "nxml":
        return "nXML"
    if "thumb" in file_name.lower() and file_extension.lower() in image_related_file_extensions:
        return "thumbnail"
    # if "fig" in file_name.lower() and file_extension.lower() in image_related_file_extensions:
    if file_extension.lower() in image_related_file_extensions:
        return "figure"
    return "supplement"


def get_pmids_from_exclude_list(mod=None):

    data_path = path.join(path.dirname(path.dirname(path.abspath(__file__))),
                          "pubmed_ingest", "data_for_pubmed_processing")

    exclude_pmid_file = None
    if mod is None:
        exclude_pmid_file = path.join(data_path, "pmids_to_excude.txt")
    else:
        # 'SGD': 'SGD_false_positive_pmids.txt'
        # 'FB': 'FB_false_positive_pmids.txt'
        mod_false_positive_file = {
            'WB': 'WB_false_positive_pmids.txt',
            'XB': 'XB_false_positive_pmids.txt',
            'ZFIN': 'ZFIN_false_positive_pmids.txt'
        }
        # "SGD": "https://sgd-prod-upload.s3.us-west-2.amazonaws.com/latest/SGD_false_positive_pmids.txt"
        # "FB": "https://ftp.flybase.net/flybase/associated_files/alliance/FB_false_positive_pmids.txt"
        mod_to_fp_pmids_url = {
            "WB": "https://caltech-curation.textpressolab.com/files/pub/agr_upload/pap_papers/rejected_pmids",
            "XB": "https://ftp.xenbase.org/pub/DataExchange/AGR/XB_false_positive_pmids.txt"
        }
        if mod in mod_false_positive_file:
            exclude_pmid_file = path.join(data_path, mod_false_positive_file[mod])
            if mod in mod_to_fp_pmids_url:
                fp_url = mod_to_fp_pmids_url[mod]
                download_file(fp_url, exclude_pmid_file)

    exclude_pmids = set()
    if exclude_pmid_file:
        with open(exclude_pmid_file, "r") as infile_fh:
            exclude_pmids = {line.rstrip().replace('PMID:', '') for line in infile_fh if line.strip()}

    return exclude_pmids


def remove_surrogates(text):
    """
    to deal with error like the following:
    'utf-8' codec can't encode characters in position 163848625-163848626:
    surrogates not allowed
    """

    if text:
        return re.sub(r'[\uD800-\uDFFF]', '', text)
    return text


def escape_special_characters(text):

    if text:
        text = remove_surrogates(text)

        # Unescape HTML entities
        text = html.unescape(text)

        ## escape any newline, carriage return, and tab
        if "\n" in text or "\r" in text or "\t" in text:
            text = text.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')

        ## escape double quote
        # text = text.replace('"', '\\"')

    return text
