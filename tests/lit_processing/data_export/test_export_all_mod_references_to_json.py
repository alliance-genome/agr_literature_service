import json
from os import stat, path, environ

from agr_literature_service.lit_processing.data_export.export_all_mod_references_to_json import \
    dump_all_data
from ...fixtures import db, load_sanitized_references, cleanup_tmp_files_when_done, populate_test_mod_reference_types # noqa


class TestExportAllModReferencesToJson:

    def test_dump_all_data(self, db, load_sanitized_references, cleanup_tmp_files_when_done): # noqa
        dump_all_data()
        base_path = environ.get('XML_PATH')
        json_path = path.join(base_path, "json_data/")
        # mod_to_count = {'WB': 3, 'XB': 2, 'ZFIN': 3, 'FB': 3, 'SGD': 2, 'RGD': 2, 'MGI': 2}
        mod_to_count = {'WB': 3, 'ZFIN': 3, 'FB': 3}
        # for mod in ['WB', 'XB', 'ZFIN', 'FB', 'SGD', 'RGD', 'MGI']:
        for mod in ['WB', 'ZFIN', 'FB']:
            json_file = path.join(json_path, "reference_" + mod + ".json")
            assert path.exists(json_file)
            if mod in ['WB', 'FB']:
                assert stat(json_file).st_size > 4000
            else:
                assert stat(json_file).st_size > 10000
            json_data = json.load(open(json_file))
            assert len(json_data['data']) == mod_to_count[mod]
            for reference in json_data["data"]:
                assert 'category' in reference
                assert 'curie' in reference
