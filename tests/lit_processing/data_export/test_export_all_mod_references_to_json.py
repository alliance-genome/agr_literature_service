import json
from os import path, stat

from agr_literature_service.lit_processing.data_export.export_all_mod_references_to_json import \
    dump_all_data
from tests.utils import cleanup_tmp_files
import load_references


class TestExportAllModReferencesToJson:

    def test_dump_all_data(self):
        try:
            load_references()
            dump_all_data()
            json_path = path.join(path.dirname(__file__),
                                  "../../../../agr_literature_service/lit_processing/data_ingest/tmp/json_data/")
            mod_to_count = {'WB': 1, 'XB': 2, 'ZFIN': 3, 'FB': 3, 'SGD': 2, 'RGD': 2, 'MGI': 2}
            for mod in ['WB', 'XB', 'ZFIN', 'FB', 'SGD', 'RGD', 'MGI']:
                json_file = json_path + "reference" + "_" + mod + ".json"
                assert path.exists(json_file)
                assert stat(json_file).st_size > 5000
                json_data = json.load(open(json_file))
                assert len(json_data['data']) == mod_to_count[mod]
                assert 'category' in json_data['data']
                assert 'curie' in json_data['data']
        finally:
            cleanup_tmp_files()
