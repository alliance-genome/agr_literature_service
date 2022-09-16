import json
from os import path, stat, rename
from datetime import date

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_engine
from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import \
    dump_data, concatenate_json_files, get_meta_data, generate_json_file, get_reference_col_names,\
    get_reference_data_and_generate_json, get_comment_correction_data, get_journal_data

from tests.utils import cleanup_tmp_files
import load_references


class TestExportSingleModReferencesToJson:

    def test_dump_data(self):
        try:
            load_references()

            ## test dump_data()
            dump_data('ZFIN', None, None)
            dump_data('SGD', None, None)
            dump_data('WB', None, None)
            json_path = path.join(path.dirname(__file__),
                                  "../../../../agr_literature_service/lit_processing/data_ingest/tmp/json_data/")

            json_file = json_path + "reference_ZFIN.json"
            assert path.exists(json_file)
            assert stat(json_file).st_size > 5000
            json_data = json.load(open(json_file))
            assert 'data' in json_data
            assert len(json_data['data']) == 3

            ## test get_meta_data()
            datestamp = str(date.today()).replace("-", "")
            metaData = get_meta_data('SGD', datestamp)
            assert 'dateProvided' in metaData
            assert metaData['dateProvided'] == datestamp
            assert 'dataProvider' in metaData
            assert 'mod' in metaData['dataProvider']
            assert metaData['dataProvider']['mod'] == 'SGD'

            ## test concatenate_json_files()
            rename(json_path + "reference_ZFIN.json", json_path + "reference_ZFIN.json_0")
            rename(json_path + "reference_SGD.json", json_path + "reference_ZFIN.json_1")
            rename(json_path + "reference_WB.json", json_path + "reference_ZFIN.json_2")
            json_file = json_path + "reference_ZFIN.json"
            concatenate_json_files(json_file, 2)
            assert path.exists(json_file)
            assert stat(json_file).st_size > 20000
            json_data = json.load(open(json_file))
            data = json_data['data']
            metaData = json_data['metaData']
            assert len(data) == 6

            ## test generate_json_file()
            ## just to make sure there is only one copy of metaData in the concatenated json_file
            new_json_file = json_file + "_new"
            generate_json_file(metaData, data, new_json_file)
            assert new_json_file == json_file

            ## test get_reference_col_names()
            ref_col_names = get_reference_col_names()
            assert 'title' in ref_col_names
            assert 'curie' in ref_col_names

            engine = create_postgres_engine(False)
            db_connection = engine.connect()

            ## test get_comment_correction_data() and get_journal_data()
            reference_id_to_comment_correction_data = get_comment_correction_data(db_connection)
            assert type(reference_id_to_comment_correction_data) == dict()

            resource_id_to_journal = get_journal_data(db_connection)
            assert type(resource_id_to_journal) == dict()

            ## test get_reference_data_and_generate_json()
            json_file = json_path + "reference_RGD.json"
            get_reference_data_and_generate_json('RGD', reference_id_to_comment_correction_data,
                                                 resource_id_to_journal,
                                                 json_file, datestamp)
            assert path.exists(json_file)
            assert stat(json_file).st_size > 10000
            json_data = json.load(open(json_file))
            assert 'data' in json_data
            assert len(json_data['data']) == 2

            ## add more tests here

        finally:
            cleanup_tmp_files()
