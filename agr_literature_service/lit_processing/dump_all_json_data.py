import logging
import time

from agr_literature_service.lit_processing.dump_json_data import dump_data

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def dump_all_data():

    for mod in ['WB', 'ZFIN', 'FB', 'SGD', 'RGD', 'MGI']:
        log.info("Dumping json data for " + mod)
        try:
            dump_data(mod, None)
        except Exception as e:
            log.info("Error occurred when dumping json data for " + mod + ": " + str(e))
        time.sleep(5)


if __name__ == "__main__":

    dump_all_data()
