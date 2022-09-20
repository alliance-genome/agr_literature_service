import os
from os import environ
import shutil
import filecmp

from agr_literature_service.lit_processing.data_ingest.dqm_ingest.parse_dqm_json_reference \
    import generate_pmid_data, aggregate_dqm_with_pubmed
from ....fixtures import cleanup_tmp_files_when_done # noqa


class TestParseDqmJsonReference:
    def test_generate_pmid_data(self, cleanup_tmp_files_when_done):      # noqa: C901 F811
        base_path = environ.get('XML_PATH')
        sample_file_path = os.path.join(
            os.path.dirname(__file__),
            "../../../../agr_literature_service/lit_processing/tests/")
        generate_pmid_data(base_input_dir=sample_file_path, input_path="dqm_load_sample/",
                           output_directory="./", input_mod="all")
        expected_pmids_string = '2\n10022914\n10206683\n20301347\n21290765\n21413221\n21413225\n21873635\n27899353\n28304499\n28308877\n30002370\n30003105\n30110134\n30979869\n31188077\n31193955\n33054145\n34530988\n'
        filename = os.path.join(base_path, "inputs", "alliance_pmids")
        assert os.path.exists(filename)
        assert os.stat(filename).st_size > 0
        generated_pmids = open(filename).read()
        assert expected_pmids_string == generated_pmids

        expected_pmids_by_mods_string = '10206683\t1\tRGD\n10022914\t2\tRGD, FB\n2\t2\tRGD, SGD\n20301347\t1\tRGD\n21873635\t6\tRGD, MGI, SGD, FB, ZFIN, WB\n27899353\t1\tRGD\n30979869\t1\tMGI\n30002370\t1\tSGD\n33054145\t1\tSGD\n28308877\t1\tFB\n28304499\t1\tFB\n31188077\t1\tZFIN\n30110134\t1\tZFIN\n31193955\t1\tZFIN\n30003105\t1\tZFIN\n34530988\t1\tZFIN\n21413225\t1\tWB\n21413221\t1\tWB\n21290765\t1\tWB\n'
        filename = os.path.join(base_path, "pmids_by_mods")
        assert os.path.exists(filename)
        assert os.stat(filename).st_size > 0
        generated_pmids_by_mods = open(filename).read()
        assert expected_pmids_by_mods_string == generated_pmids_by_mods

    def test_aggregate_dqm_with_pubmed(self, cleanup_tmp_files_when_done):      # noqa: C901 F811
        sample_file_path = os.path.join(
            os.path.dirname(__file__),
            "../../sample_data/for_aggregate_dqm_with_pubmed/")
        base_path = environ.get('XML_PATH')
        if (os.path.isdir(base_path)):
            shutil.rmtree(base_path)
        shutil.copytree(sample_file_path, base_path)

        aggregate_dqm_with_pubmed(base_dir=sample_file_path, input_path="dqm_load_sample", input_mod="all",
                                  output_directory="./")

        validation_dir = os.path.join(sample_file_path, "validation")
        for dirname in os.listdir(validation_dir):
            subdirname = os.path.join(validation_dir, dirname)
            if os.path.isdir(subdirname):
                for filename in os.listdir(subdirname):
                    file = os.path.join(subdirname, filename)
                    if os.path.isfile(file):
                        if (dirname == 'resource_xml'):
                            # resource_xml/resource_abbreviation_not_matched is getting generated in sample_file_path and needs to be refactored
                            # there is no test resource data, so all resources are in file of not_matched.
                            # once resources are in, see which ones would not get generated and test for those.
                            validation_data = open(file).readlines()
                            generated_data = open(os.path.join(sample_file_path, dirname, filename)).readlines()
                            assert validation_data.sort() == generated_data.sort()
                        else:
                            assert filecmp.cmp(file, os.path.join(base_path, dirname, filename))
