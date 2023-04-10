from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.load_pmc_metadata import \
    resolve_displayname_conflict


class TestLoadPmcMetadata:

    def test_resolve_displayname_conflict(self):
        ref_file_uniq_filename_set = {(1, "file1.txt"), (1, "file2.txt"), (1, "file1_1.txt")}
        file_name_with_suffix = "file1.txt"
        reference_id = 1

        new_file_name = resolve_displayname_conflict(ref_file_uniq_filename_set,
                                                     file_name_with_suffix,
                                                     reference_id)
        assert new_file_name == 'file1_2.txt'
