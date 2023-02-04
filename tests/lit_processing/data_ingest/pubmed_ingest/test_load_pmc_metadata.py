from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.load_pmc_metadata import \
    resolve_displayname_conflict


class TestLoadPmcMetadata:

    def test_resolve_displayname_conflict(self):

        file_name_with_suffix = "test_filename.pdf"
        new_file_name = resolve_displayname_conflict(file_name_with_suffix)
        assert new_file_name == 'test_filename_1.pdf'
