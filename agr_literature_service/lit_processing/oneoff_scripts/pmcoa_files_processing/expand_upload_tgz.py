
# pipenv run python expand_upload_tgz.py > log_expand_upload_tgz

# take list of tgz files from  chunking_pmids/20210426_01.txt (or other file),
# get tgz files from  chunking_pmids/pubmed_tgz_20210426_01/<pmid>.tar.gz
# expand files to  chunking_pmids/expand_tgz/<pmid>/
# copy tgz to  chunking_pmids/expand_tgz/<pmid>/
# get md5sum of each file into  chunking_pmids/expand_tgz/<pmid>/md5sum
# upload each file to s3 at s3://agr-literature/develop/reference/documents/pubmed/pmid/<pmid>/
# output log of all uploaded files and md5sums to  chunking_pmids/md5sum_20210426_01


import hashlib
import logging.config
import tarfile
from os import environ, listdir, makedirs, path, rename, walk
from shutil import copy2

from agr_literature_service.lit_processing.utils.s3_utils import upload_file_to_s3
from dotenv import load_dotenv
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir


load_dotenv()
init_tmp_dir()

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')
logging.getLogger("s3transfer.utils").setLevel(logging.WARNING)
logging.getLogger("s3transfer.tasks").setLevel(logging.WARNING)
logging.getLogger("s3transfer.futures").setLevel(logging.WARNING)


base_path = environ.get('XML_PATH', "")
process_path = base_path + 'chunking_pmids/'

base_expand_dir = process_path + 'expand_tgz/'
if not path.exists(base_expand_dir):
    makedirs(base_expand_dir)
temp_expand_dir = process_path + 'expand_tgz/temp_expand/'
if not path.exists(temp_expand_dir):
    makedirs(temp_expand_dir)


def expand_tgz(tgz_file, expand_dir):
    """

    :param tgz_file:
    :param expand_dir:
    :return:
    """

    this_tarfile = tarfile.open(tgz_file)
    this_tarfile.extractall(temp_expand_dir)
    this_tarfile.close()
    dir_list = listdir(temp_expand_dir)
    dir_to_move = temp_expand_dir + dir_list[0]
    if len(dir_list) > 1:
        logger.info("WARNING %s has %s directories", tgz_file, len(dir_list))
    rename(dir_to_move, expand_dir)


def generate_md5sum(expand_dir, pmid):
    output_md5file = expand_dir + '/md5sum'
    md5_info = ''
    with open(output_md5file, "w") as output_fh:
        dir_list = listdir(expand_dir)
        for filename in dir_list:
            if filename == 'md5sum':
                continue
            file = expand_dir + '/' + filename
            md5sum = hashlib.md5(open(file, 'rb').read()).hexdigest()
            output_fh.write("%s\t%s\n" % (filename, md5sum))
            md5_info += ("%s/%s\t%s\n" % (pmid, filename, md5sum))
        output_fh.close()

    return md5_info


def upload_s3_dir(expand_dir, pmid):
    """

    :param expand_dir:
    :param pmid:
    :return:
    """

    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    bucketname = 'agr-literature'
    if env_state != 'test':
        for root, _dirs, files in walk(expand_dir):
            for filename in files:
                file = env_state + '/reference/documents/pubmed/pmid/' + pmid + '/' + filename
                upload_file_to_s3(path.join(root, filename), bucketname, file)


def process_tgz():
    """

    :return:
    """

    date_file = '20210426_02'
    list_file = process_path + date_file + '.txt'
    count = 0
    md5_summary = ''
    with open(list_file) as list_fh:
        line = list_fh.readline()
        while line:
            count += 1
            # if count > 3:
            if count > 333333:
                break
            tabs_split = line.split("\t")
            pmid = tabs_split[0]
            tgz_file = process_path + 'pubmed_tgz_' + date_file + '/' + pmid + '.tar.gz'
            expand_dir = process_path + 'expand_tgz/' + pmid
            # if this script has not already run on this pmid, expand the tar gz file into a directory for the pmid
            if not path.exists(expand_dir):
                expand_tgz(tgz_file, expand_dir)
            copy2(tgz_file, expand_dir)
            md5_info = generate_md5sum(expand_dir, pmid)
            md5_summary += md5_info
            upload_s3_dir(expand_dir, pmid)
            logger.info("pmid %s", pmid)
            line = list_fh.readline()
        list_fh.close()
    summary_md5file_filename = date_file + '_md5sum.txt'
    output_md5file = process_path + '/' + summary_md5file_filename
    with open(output_md5file, "w") as output_fh:
        output_fh.write(md5_summary)
        output_fh.close()
    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    s3_file_location = env_state + '/reference/documents/pubmed/tarball_chunks/pubmed_tgz_' + summary_md5file_filename
    if env_state != 'test':
        upload_file_to_s3(output_md5file, 'agr-literature', s3_file_location)


if __name__ == "__main__":
    """
    call main start function
    """

    process_tgz()
