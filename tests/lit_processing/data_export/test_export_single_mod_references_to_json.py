import json
from os import path, stat, rename, environ
from datetime import date
from sqlalchemy import text

from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import \
    dump_data, concatenate_json_files, get_meta_data, generate_json_file, get_reference_col_names,\
    get_reference_data_and_generate_json
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_cross_reference_data_for_ref_ids, get_author_data_for_ref_ids, \
    get_mesh_term_data_for_ref_ids, get_mod_corpus_association_data_for_ref_ids, \
    get_mod_reference_type_data_for_ref_ids

from ...fixtures import cleanup_tmp_files_when_done, load_sanitized_references, db, populate_test_mod_reference_types # noqa


class TestExportSingleModReferencesToJson:

    def test_data_retrieval_functions(self, db, load_sanitized_references): # noqa

        reference_id_list = []
        curie_to_reference_id = {}

        ## ZFIN papers
        rs = db.execute(text("SELECT reference_id, curie FROM cross_reference where curie in ('PMID:33622238', 'PMID:34354223', 'PMID:35151207')"))
        rows = rs.fetchall()
        for x in rows:
            reference_id_list.append(x[0])
            curie_to_reference_id[x[1]] = x[0]

        ref_ids = ", ".join([str(x) for x in reference_id_list])

        reference_id_to_xrefs = get_cross_reference_data_for_ref_ids(db, ref_ids)
        reference_id_to_authors = get_author_data_for_ref_ids(db, ref_ids)
        reference_id_to_mesh_terms = get_mesh_term_data_for_ref_ids(db, ref_ids)
        reference_id_to_mod_corpus_data = get_mod_corpus_association_data_for_ref_ids(db, ref_ids)
        reference_id_to_mod_reference_types = get_mod_reference_type_data_for_ref_ids(db, ref_ids)

        ref_id = curie_to_reference_id['PMID:33622238']

        assert len(reference_id_to_xrefs.get(ref_id)) == 3
        assert len(reference_id_to_authors.get(ref_id)) == 2
        assert len(reference_id_to_mesh_terms.get(ref_id)) == 24
        assert len(reference_id_to_mod_corpus_data.get(ref_id)) == 1
        assert len(reference_id_to_mod_reference_types.get(ref_id)) == 1

        ref_id = curie_to_reference_id['PMID:34354223']
        authors = reference_id_to_authors.get(ref_id)
        assert authors[0]['first_name'] == 'Sandra'
        modCorpusAssociations = reference_id_to_mod_corpus_data.get(ref_id)
        assert modCorpusAssociations[0]['mod_corpus_sort_source'] == 'Dqm_files'
        assert modCorpusAssociations[0]['mod_abbreviation'] == 'ZFIN'

        ref_id = curie_to_reference_id['PMID:35151207']
        for cr in reference_id_to_xrefs.get(ref_id):
            curie = cr['curie']
            if curie.startswith('DOI'):
                assert curie == 'DOI:10.1016/j.bbrc.2022.01.127'

    def test_get_meta_data(self):

        datestamp = str(date.today()).replace("-", "")
        metaData = get_meta_data('SGD', datestamp)
        assert 'dateProduced' in metaData
        assert metaData['dateProduced'] == datestamp
        assert 'dataProvider' in metaData
        assert 'mod' in metaData['dataProvider']
        assert metaData['dataProvider']['mod'] == 'SGD'

    def test_get_reference_col_names(self):

        ref_col_names = get_reference_col_names()
        assert 'title' in ref_col_names
        assert 'curie' in ref_col_names

    def test_dump_data(self, db, load_sanitized_references, cleanup_tmp_files_when_done): # noqa

        ## test dump_data()
        dump_data('ZFIN', None, None)
        base_path = environ.get('XML_PATH')
        json_path = path.join(base_path, "json_data/")
        json_file = json_path + "reference_ZFIN.json"
        assert path.exists(json_file)
        assert stat(json_file).st_size > 10000
        json_data = json.load(open(json_file))
        assert 'data' in json_data
        assert len(json_data['data']) == 3

        ## test concatenate_json_files()
        dump_data('WB', None, None)
        base_path = environ.get('XML_PATH')
        json_path = path.join(base_path, "json_data/")
        rename(json_path + "reference_ZFIN.json", json_path + "reference_ZFIN.json_0")
        rename(json_path + "reference_WB.json", json_path + "reference_ZFIN.json_1")
        json_file = json_path + "reference_ZFIN.json"
        concatenate_json_files(json_file, 2)
        assert path.exists(json_file)
        assert stat(json_file).st_size > 10000
        json_data = json.load(open(json_file))
        data = json_data['data']
        metaData = json_data['metaData']
        assert len(data) == 6

        ## test generate_json_file()
        ## just to make sure there is only one copy of metaData in the concatenated json_file
        new_json_file = json_file + "_new"
        generate_json_file(metaData, data, new_json_file)
        assert json.dumps(json.load(open(new_json_file)), sort_keys=True) == json.dumps(json.load(open(json_file)),
                                                                                        sort_keys=True)

        ## test get_reference_data_and_generate_json()
        reference_id_to_reference_relation_data = {}
        resource_id_to_journal = {}
        json_file = json_path + "reference_WB.json"
        datestamp = str(date.today()).replace("-", "")
        get_reference_data_and_generate_json('WB', reference_id_to_reference_relation_data,
                                             resource_id_to_journal,
                                             json_file, datestamp)
        assert path.exists(json_file)
        assert stat(json_file).st_size > 500
        json_data = json.load(open(json_file))
        assert 'data' in json_data
        assert len(json_data['data']) == 3
