# import json
import os
from os import environ

from agr_literature_service.lit_processing.data_ingest.dqm_ingest.parse_dqm_json_reference \
    import generate_pmid_data
#     get_alliance_category_from_pubmed_types, generate_json
# from ....fixtures import cleanup_tmp_files_when_done # noqa


class TestParseDqmJsonReference:
    def test_generate_pmid_data(self):      # noqa: C901
        base_path = environ.get('XML_PATH')
        sample_file_path = os.path.join(
            os.path.dirname(__file__),
            "../../../../agr_literature_service/lit_processing/tests/")
        generate_pmid_data(base_input_dir=sample_file_path, input_path="dqm_load_sample/",
                           output_directory="./", input_mod="all")
        expected_pmids = [2, 10022914, 10206683, 20301347, 21290765, 21413221, 21413225,
                          21873635, 27899353, 28304499, 28308877, 30002370, 30003105,
                          30110134, 30979869, 31188077, 31193955, 33054145, 34530988]
        filename = os.path.join(base_path, "inputs", "alliance_pmids")
        assert os.path.exists(filename)
        assert os.stat(filename).st_size > 0
        for line in open(filename):
            assert int(line.rstrip('\n')) in expected_pmids
        filename = os.path.join(base_path, "pmids_by_mods")
        assert os.path.exists(filename)
        assert os.stat(filename).st_size > 0
        has_multi_mod = False
        for line in open(filename):
            cols = line.split("\t")
            if cols[0] == '21873635':
                if cols[2].rstrip('\n') == "RGD, MGI, SGD, FB, ZFIN, WB":
                    has_multi_mod = True
        assert has_multi_mod is True
