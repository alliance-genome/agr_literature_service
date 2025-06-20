

from datetime import date
from sqlalchemy import text
import json
import os
import tempfile
from unittest.mock import patch, MagicMock

from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import \
    get_meta_data, get_reference_col_names, generate_json_data, generate_json_file, dump_data
from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_cross_reference_data_for_ref_ids, get_author_data_for_ref_ids, \
    get_mesh_term_data_for_ref_ids, get_mod_corpus_association_data_for_ref_ids, \
    get_mod_reference_type_data_for_ref_ids, get_citation_data, get_license_data

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

    def test_generate_json_file(self):

        test_data = [{"test_key": "test_value", "reference_id": 123}]
        test_metadata = {"dateProduced": "20240101", "dataProvider": {"mod": "TEST"}}

        # Create a proper temporary file path
        tmp_dir = tempfile.mkdtemp()
        tmp_path = os.path.join(tmp_dir, 'test_file.json')

        try:
            generate_json_file(test_metadata, test_data, tmp_path)

            assert os.path.exists(tmp_path)

            with open(tmp_path, 'r') as f:
                result = json.load(f)

            assert 'data' in result
            assert 'metaData' in result
            assert result['data'] == test_data
            assert result['metaData'] == test_metadata

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if os.path.exists(tmp_dir):
                os.rmdir(tmp_dir)

    def test_generate_json_data(self, db, load_sanitized_references):  # noqa

        reference_id_list = []
        rs = db.execute(text("SELECT reference_id FROM cross_reference where curie = 'PMID:33622238' LIMIT 1"))
        row = rs.fetchone()
        if not row:
            return  # Skip test if no data found

        reference_id_list.append(row[0])
        ref_id = row[0]
        ref_ids = str(ref_id)

        reference_id_to_xrefs = get_cross_reference_data_for_ref_ids(db, ref_ids)
        reference_id_to_authors = get_author_data_for_ref_ids(db, ref_ids)
        reference_id_to_mesh_terms = get_mesh_term_data_for_ref_ids(db, ref_ids)
        reference_id_to_mod_corpus_data = get_mod_corpus_association_data_for_ref_ids(db, ref_ids)
        reference_id_to_mod_reference_types = get_mod_reference_type_data_for_ref_ids(db, ref_ids)

        # Mock data for other required parameters
        reference_id_to_reference_relation_data = {}
        resource_id_to_journal = {}
        reference_id_to_citation_data = {}
        reference_id_to_license_data = {}

        # Get reference data
        ref_cols = get_reference_col_names()
        cols = ", ".join(ref_cols)
        ref_data = db.execute(text(f"SELECT {cols} FROM reference WHERE reference_id = :ref_id"),
                              {'ref_id': ref_id}).fetchall()

        data = []
        count = generate_json_data(
            ref_data,
            reference_id_to_xrefs,
            reference_id_to_authors,
            reference_id_to_reference_relation_data,
            reference_id_to_mod_reference_types,
            reference_id_to_mesh_terms,
            reference_id_to_mod_corpus_data,
            resource_id_to_journal,
            reference_id_to_citation_data,
            reference_id_to_license_data,
            data
        )

        assert count == 1
        assert len(data) == 1
        assert 'reference_id' in data[0]
        assert 'curie' in data[0]
        assert 'authors' in data[0]
        assert 'cross_references' in data[0]
        assert 'mod_reference_types' in data[0]
        assert 'mesh_terms' in data[0]
        assert 'mod_corpus_associations' in data[0]

    @patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.upload_json_file_to_s3')
    @patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.create_postgres_session')
    def test_dump_data_single_mod(self, mock_db_session, mock_upload):  # noqa

        # Mock database session and queries
        mock_db = MagicMock()
        mock_db_session.return_value = mock_db

        # Create a mock row object that has both tuple behavior and attribute access
        class MockRow:
            def __init__(self, *args):
                self._data = args
                # Map tuple positions to column names based on get_reference_col_names()
                col_names = get_reference_col_names()
                for i, col_name in enumerate(col_names):
                    if i < len(args):
                        setattr(self, col_name, args[i])

            def __getitem__(self, key):
                return self._data[key]

            def __len__(self):
                return len(self._data)

        mock_row = MockRow(123, 'PMID:123', 1, 'Test Title', 'en', '2024-01-01', None, None,
                           '1', None, None, '1-10', 'Test Abstract', None, None, None,
                           'Research', None, None, '2024-01-01', '2024-01-01')

        # Mock mod_id query
        mock_db.execute.return_value.fetchall.side_effect = [
            [(1,)],  # mod_ids
            [(123,)],  # reference_ids
            [mock_row]  # reference data
        ]

        with patch('os.environ.get') as mock_env:
            mock_env.return_value = tempfile.gettempdir() + '/'

            with patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.get_all_reference_relation_data') as mock_rels, \
                 patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.get_journal_by_resource_id') as mock_journals, \
                 patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.get_citation_data') as mock_cites, \
                 patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.get_license_data') as mock_licenses, \
                 patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.get_cross_reference_data_for_ref_ids') as mock_xrefs, \
                 patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.get_author_data_for_ref_ids') as mock_authors, \
                 patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.get_mesh_term_data_for_ref_ids') as mock_mesh, \
                 patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.get_mod_reference_type_data_for_ref_ids') as mock_mod_types, \
                 patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.get_mod_corpus_association_data_for_ref_ids') as mock_mod_corpus:

                # Mock return values
                mock_rels.return_value = {}
                mock_journals.return_value = {}
                mock_cites.return_value = {}
                mock_licenses.return_value = {}
                mock_xrefs.return_value = {123: []}
                mock_authors.return_value = {123: []}
                mock_mesh.return_value = {123: []}
                mock_mod_types.return_value = {123: []}
                mock_mod_corpus.return_value = {123: []}
                mock_upload.return_value = None

                result = dump_data(mod='SGD')

                assert result is not None
                mock_upload.assert_called_once()

    def test_citation_and_license_data_integration(self, db, load_sanitized_references):  # noqa

        # Test that citation and license data functions are accessible
        citation_data = get_citation_data(db)
        license_data = get_license_data(db)

        assert isinstance(citation_data, dict)
        assert isinstance(license_data, dict)

    # New comprehensive tests

    def test_upload_json_file_to_s3_test_env_check(self):
        # Test that function returns None for test environment without doing any work
        from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import upload_json_file_to_s3

        with patch('os.environ.get') as mock_env:
            mock_env.return_value = 'test'

            # Should return None immediately for test environment
            result = upload_json_file_to_s3('/tmp/', 'test.json', '20240101', False)

            assert result is None

    def test_upload_json_file_to_s3_function_exists(self):
        # Simple test that just verifies the function exists and can be imported
        from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import upload_json_file_to_s3
        import inspect

        # Check that function exists and has expected parameters
        sig = inspect.signature(upload_json_file_to_s3)
        expected_params = ['json_path', 'json_file', 'datestamp', 'ondemand']
        actual_params = list(sig.parameters.keys())

        for param in expected_params:
            assert param in actual_params

    @patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.log')
    def test_generate_json_file_unicode_error(self, mock_log):

        # Test data with Unicode issues
        problematic_data = [{"title": "Test\ufffd\ufffd", "reference_id": 123}]
        test_metadata = {"dateProduced": "20240101", "dataProvider": {"mod": "TEST"}}

        tmp_dir = tempfile.mkdtemp()
        tmp_path = os.path.join(tmp_dir, 'test_unicode.json')

        try:
            with patch('json.dump') as mock_json_dump:
                mock_json_dump.side_effect = UnicodeEncodeError('utf-8', '', 0, 1, 'invalid start byte')

                generate_json_file(test_metadata, problematic_data, tmp_path)

                # Should log the error
                mock_log.info.assert_called()

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if os.path.exists(tmp_dir):
                os.rmdir(tmp_dir)

    @patch('agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json.log')
    def test_generate_json_file_general_error(self, mock_log):

        test_data = [{"test_key": "test_value"}]
        test_metadata = {"dateProduced": "20240101"}

        tmp_dir = tempfile.mkdtemp()
        tmp_path = os.path.join(tmp_dir, 'test_error.json')

        try:
            with patch('json.dump') as mock_json_dump:
                mock_json_dump.side_effect = Exception("General error")

                generate_json_file(test_metadata, test_data, tmp_path)

                # Should log the error
                mock_log.info.assert_called()

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if os.path.exists(tmp_dir):
                os.rmdir(tmp_dir)

    def test_get_meta_data_edge_cases(self):

        # Test with empty mod
        metadata = get_meta_data('', '20240101')
        assert metadata['dataProvider']['mod'] == ''

        # Test with None datestamp
        metadata = get_meta_data('TEST', None)
        assert metadata['dateProduced'] is None

        # Test with special characters
        metadata = get_meta_data('MOD-TEST_123', '20240229')
        assert metadata['dataProvider']['mod'] == 'MOD-TEST_123'
        assert metadata['dateProduced'] == '20240229'

    def test_get_reference_col_names_completeness(self):

        col_names = get_reference_col_names()

        # Test that all expected columns are present
        expected_columns = [
            'reference_id', 'curie', 'resource_id', 'title', 'language',
            'date_published', 'date_arrived_in_pubmed', 'date_last_modified_in_pubmed',
            'volume', 'plain_language_abstract', 'pubmed_abstract_languages',
            'page_range', 'abstract', 'keywords', 'pubmed_types', 'publisher',
            'category', 'pubmed_publication_status', 'issue_name',
            'date_updated', 'date_created'
        ]

        for col in expected_columns:
            assert col in col_names

        # Test that it returns a list
        assert isinstance(col_names, list)
        assert len(col_names) > 0

    def test_dump_data_function_signature(self):
        # Lightweight test that just verifies the function exists and accepts parameters
        from agr_literature_service.lit_processing.data_export.export_single_mod_references_to_json import dump_data
        import inspect

        # Check function signature
        sig = inspect.signature(dump_data)
        expected_params = ['mod', 'email', 'ondemand', 'ui_root_url']
        actual_params = list(sig.parameters.keys())

        for param in expected_params:
            assert param in actual_params

    def test_send_data_export_report_function_exists(self):
        # Simple test that verifies the function exists and can be imported
        from agr_literature_service.lit_processing.utils.report_utils import send_data_export_report
        import inspect

        # Check that function exists and has expected parameters
        sig = inspect.signature(send_data_export_report)
        expected_params = ['status', 'email', 'mod', 'email_message']
        actual_params = list(sig.parameters.keys())

        for param in expected_params:
            assert param in actual_params

    def test_generate_json_file_with_special_characters(self):

        # Test with special characters that should be handled properly
        test_data = [
            {"title": "Test with émojis 🧬 and spëcial chars", "reference_id": 123},
            {"abstract": "Contains\nnewlines\tand\ttabs", "reference_id": 124}
        ]
        test_metadata = {"dateProduced": "20240101", "dataProvider": {"mod": "TEST"}}

        tmp_dir = tempfile.mkdtemp()
        tmp_path = os.path.join(tmp_dir, 'test_special.json')

        try:
            generate_json_file(test_metadata, test_data, tmp_path)

            assert os.path.exists(tmp_path)

            with open(tmp_path, 'r', encoding='utf-8') as f:
                result = json.load(f)

            assert 'data' in result
            assert len(result['data']) == 2
            assert "émojis 🧬" in result['data'][0]['title']

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            if os.path.exists(tmp_dir):
                os.rmdir(tmp_dir)
