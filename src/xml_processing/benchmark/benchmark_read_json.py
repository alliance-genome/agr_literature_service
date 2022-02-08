
import argparse
import logging.config
import time
from os import environ, path

from dotenv import load_dotenv

load_dotenv()


# python3 benchmark_read_json.py -f inputs/alliance_pmids
# run this to test reading large number of files from single directory, and test read times.
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-volume-types.html
# seems limited to
# Max IOPS per volume 	40-200
# Max throughput per volume 	40-90 MiB/s


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')

args = vars(parser.parse_args())

# base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
base_path = environ.get('XML_PATH')

pmids = []


def benchmark_read_json():
    """

    :return:
    """

    start = time.time()
    actual_start = time.time()
    logger.info("in benchmark %s", start)
    counter = 0
    last_counter = 0
    size = 0
    last_size = 0
    time_rate = 1
    for pmid in pmids:
        filename = get_path_from_pmid(pmid, 'xml')
        counter = counter + 1
        try:
            with open(filename, 'r') as f:
                line = f.read()
                size += len(line)
                end = time.time()
                if end - start > time_rate:
                    rate = counter - last_counter
                    size_rate = size - last_size
                    avg_file_size = size_rate / rate
                    last_counter = counter
                    last_size = size
                    start = end
                    logger.info("read %s seconds %s entries, %s size, %s size_rate, %s rate, %s avg_file_size", time_rate, counter, size, size_rate, rate, avg_file_size)
                f.close()
        except Exception as e:
            print(str(e))
            pass
    end = time.time()
    diff_time = end - actual_start
    logger.info("read %s seconds %s entries, %s size", diff_time, counter, size)


def get_path_from_pmid(pmid, file_type):
    """

    :param pmid:
    :param file_type:
    :return:
    """

    pmid_list = list(pmid)
    if len(pmid_list) < 4:
        destination_filepath = base_path + 'pubmed_' + file_type + '_split/0/' + pmid + '.' + file_type
    else:
        w = pmid_list.pop(0)
        x = pmid_list.pop(0)
        y = pmid_list.pop(0)
        z = pmid_list.pop(0)
        destination_filepath = base_path + 'pubmed_' + file_type + '_split/' + str(w) + '/' + str(x) + '/' + str(y) + '/' + str(z) + '/' + pmid + '.' + file_type
    # logger.info("pmid %s %s", pmid, destination_filepath)

    return destination_filepath


if __name__ == "__main__":
    """ call main start function """
    logger.info("starting benchmark_read_json.py")

    if args['file']:
        logger.info("Processing file input from %s", args['file'])
        with open(args['file'], 'r') as fp:
            pmid = fp.readline()
            while pmid:
                pmids.append(pmid.rstrip())
                pmid = fp.readline()

    else:
        logger.info("No flag passed in.  Use -h for help.")

    benchmark_read_json()
    logger.info("ending benchmark_read_json.py")
