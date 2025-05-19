import logging
import time

from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import \
    dump_data, dump_all_mods_references
from agr_literature_service.lit_processing.utils.db_read_utils import get_mod_abbreviations

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def dump_all_data():

    for mod in get_mod_abbreviations():
        log.info("Dumping json data for " + mod)
        try:
            dump_data(mod, None, None)
        except Exception as e:
            log.info("Error occurred when dumping json data for " + mod + ": " + str(e))
        time.sleep(5)
    dump_all_mods_references(email=None, ondemand=False, ui_root_url=None)


if __name__ == "__main__":

    dump_all_data()
