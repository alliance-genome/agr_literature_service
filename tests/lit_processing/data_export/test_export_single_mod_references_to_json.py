import json
from os import environ, path

from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import \
    dump_data, generate_json_file, concatenate_json_files, get_meta_data
from tests.utils import cleanup_tmp_files
import load_references


class TestExportSingleModReferencesToJson:

    def test_dump_data(self):
        try:
            load_references()
            dump_data('ZFIN', None, None)
            dump_data('SGD', None, None)
            dump_data('WB', None, None)
            json_path = path.join(path.dirname(__file__),
                                  "../../../../agr_literature_service/lit_processing/data_ingest/tmp/json_data/")
            
            json_file = json_path + "reference_ZFIN.json"
            assert os.path.exists(json_file)
            assert os.stat(json_file).st_size > 5000
            json_data = json.load(open(json_file))
            assert len(json_data['data']) == 3
            
            
        finally:
            cleanup_tmp_files()
