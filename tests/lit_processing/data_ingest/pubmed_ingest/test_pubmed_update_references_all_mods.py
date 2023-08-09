from os import path

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_all_mods \
    import get_daily_update_files


class TestPubmedUpdateReferencesAllMods:

    def test_get_daily_update_files(self):

        dailyfiles = get_daily_update_files(1)
        assert dailyfiles[0].startswith("pubmed23n")
        assert dailyfiles[0].endswith(".xml.gz")
        
