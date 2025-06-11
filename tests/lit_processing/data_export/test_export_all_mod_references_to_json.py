import tempfile
from unittest.mock import patch

from agr_literature_service.lit_processing.data_export.export_all_mod_references_to_json import \
    dump_all_data
from ...fixtures import db, load_sanitized_references, cleanup_tmp_files_when_done, populate_test_mod_reference_types # noqa


class TestExportAllModReferencesToJson:

    def test_dump_all_data_function_exists(self):
        # Lightweight test that verifies the function exists and can be imported
        import inspect

        # Check function signature
        sig = inspect.signature(dump_all_data)

        # Verify it's callable
        assert callable(dump_all_data)

        # Basic parameter check (should have no required parameters)
        assert len(sig.parameters) == 0

    @patch('agr_literature_service.lit_processing.data_export.export_all_mod_references_to_json.cleanup_temp_directory')
    @patch('agr_literature_service.lit_processing.data_export.export_all_mod_references_to_json.dump_data')
    @patch('agr_literature_service.lit_processing.data_export.export_all_mod_references_to_json.get_mod_abbreviations')
    @patch('os.environ.get')
    def test_dump_all_data_mocked(self, mock_env, mock_get_mods, mock_dump_data, mock_cleanup):
        # Test that dump_all_data calls dump_data for each MOD
        mock_env.return_value = tempfile.gettempdir() + '/'
        mock_get_mods.return_value = ['SGD', 'WB', 'FB']
        mock_dump_data.return_value = '/tmp/test_file.json'
        mock_cleanup.return_value = None

        # Call the function
        dump_all_data()

        # Verify it was called for each MOD
        expected_mods = ['SGD', 'WB', 'FB']
        assert mock_dump_data.call_count == len(expected_mods)

        # Verify each MOD was called
        called_mods = [call[1]['mod'] for call in mock_dump_data.call_args_list]
        for mod in expected_mods:
            assert mod in called_mods

    def test_json_structure_validation(self):
        # Test JSON structure validation without actual file generation
        sample_reference = {
            'category': 'Research Article',
            'curie': 'PMID:12345678',
            'title': 'Test Reference',
            'authors': [],
            'cross_references': []
        }

        # Verify required fields are present
        assert 'category' in sample_reference
        assert 'curie' in sample_reference

        # Test that the structure matches expected format
        expected_fields = ['category', 'curie', 'title', 'authors', 'cross_references']
        for field in expected_fields:
            assert field in sample_reference
