from agr_literature_service.lit_processing.utils.db_read_utils import \
    get_journal_data
from agr_literature_service.lit_processing.data_ingest.post_reference_to_db import \
    insert_reference, insert_authors, insert_cross_references, get_doi_data, \
    set_primaryId, read_data_and_load_references
from ...fixtures import db, populate_test_mod_reference_types # noqa


class TestPostReferenceToDb:

    ## post_references() has been tested in "data_export" tests

    def test_read_data_and_load_references_function_exists(self):
        # Lightweight test that verifies the function exists and can be imported
        import inspect

        # Check function signature
        sig = inspect.signature(read_data_and_load_references)
        expected_params = ['db', 'json_data', 'journal_to_resource_id', 'doi_to_reference_id', 'mod_to_mod_id', 'testmod']
        actual_params = list(sig.parameters.keys())

        for param in expected_params:
            assert param in actual_params

    def test_set_primaryId_function(self):
        # Test set_primaryId with sample data
        sample_entry = {
            'crossReferences': [
                {'id': 'PMID:33622238'}
            ]
        }
        primaryId = set_primaryId(sample_entry)
        assert primaryId == 'PMID:33622238'

        # Test with DOI
        sample_entry_doi = {
            'crossReferences': [
                {'id': 'DOI:10.1234/test'}
            ]
        }
        primaryId = set_primaryId(sample_entry_doi)
        assert primaryId == 'DOI:10.1234/test'

    def test_individual_insert_functions_signatures(self):
        # Test that all insert functions exist and have expected signatures
        import inspect

        # Test insert_reference function signature
        sig = inspect.signature(insert_reference)
        expected_params = ['db', 'primaryId', 'journal_to_resource_id', 'entry']
        actual_params = list(sig.parameters.keys())
        for param in expected_params:
            assert param in actual_params

        # Test insert_authors function signature
        sig = inspect.signature(insert_authors)
        expected_params = ['db', 'primaryId', 'reference_id', 'authors']
        actual_params = list(sig.parameters.keys())
        for param in expected_params:
            assert param in actual_params

        # Test insert_cross_references function signature
        sig = inspect.signature(insert_cross_references)
        expected_params = ['db', 'primaryId', 'reference_id', 'doi_to_reference_id', 'cross_references']
        actual_params = list(sig.parameters.keys())
        for param in expected_params:
            assert param in actual_params

    def test_get_journal_data_function_exists(self):
        # Test that get_journal_data function exists and is callable
        import inspect

        sig = inspect.signature(get_journal_data)
        assert 'db' in sig.parameters.keys()
        assert callable(get_journal_data)

    def test_get_doi_data_function_exists(self):
        # Test that get_doi_data function exists and is callable
        import inspect

        sig = inspect.signature(get_doi_data)
        assert 'db' in sig.parameters.keys()
        assert callable(get_doi_data)
