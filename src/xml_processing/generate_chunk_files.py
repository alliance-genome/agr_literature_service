
# takes pmids_not_found from get_pubmed_xml.py, and pmids_by_mods from parse_dqm_json.py, and
# generates a set sorted by MODs of pmids that were not found in pubmed.
#
# python generate_chunk_files.py


import logging
import logging.config
import re
from datetime import datetime
from os import environ, makedirs, path

from dotenv import load_dotenv

load_dotenv()

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

base_path = environ.get('XML_PATH', "")
process_path = base_path + 'chunking_pmids/'


def generate_chunk_files():
    """

    :return:
    """

    main_chunk_file = process_path + 'chunking_set'
    chunk_to_pmid_to_ftp = dict()
    current_chunk = 1
    chunk_to_pmid_to_ftp[current_chunk] = dict()
    with open(main_chunk_file) as main_file:
        main_data = main_file.read()
        main_split = main_data.split("\n")
        count = 0
#         tot_count = 0
        for line in main_split:
            pmid_re_output = re.search(r"INFO - Download (\d+) (ftp.+?tar.gz)", line)
            if pmid_re_output is not None:
                pmid = pmid_re_output.group(1)
                ftp = pmid_re_output.group(2)
                count += 1
#                 tot_count += 1
#                 if tot_count > 10:
#                     continue
#                 if count > 3:
                if count > 10000:
                    current_chunk = current_chunk + 1
                    chunk_to_pmid_to_ftp[current_chunk] = dict()
                    count = 1
                chunk_to_pmid_to_ftp[current_chunk][pmid] = ftp
        main_file.close()

    now = datetime.now()
    date = now.strftime("%Y%m%d")
#     date = '20210426'

    for chunk_number in chunk_to_pmid_to_ftp:
        chunk_count_string = str(chunk_number)
        if chunk_number < 10:
            chunk_count_string = '0' + chunk_count_string
        chunk_dir = process_path + 'pubmed_tgz_' + date + '_' + chunk_count_string
        if not path.exists(chunk_dir):
            makedirs(chunk_dir)

        output_chunk_file = process_path + date + '_' + chunk_count_string + '.txt'
        with open(output_chunk_file, "w") as output_fh:
            for pmid in chunk_to_pmid_to_ftp[chunk_number]:
                ftp = chunk_to_pmid_to_ftp[chunk_number][pmid]
                output_fh.write("%s\t%s\n" % (pmid, ftp))
            output_fh.close()

        move_chunk_file = process_path + date + '_' + chunk_count_string + '.mv'
        with open(move_chunk_file, "w") as move_fh:
            for pmid in chunk_to_pmid_to_ftp[chunk_number]:
                ftp = chunk_to_pmid_to_ftp[chunk_number][pmid]
                move_fh.write("mv %spubmed_tgz/%s.tar.gz %s\n" % (base_path, pmid, chunk_dir))
            move_fh.close()


if __name__ == "__main__":
    """
    call main start function
    """

    generate_chunk_files()
