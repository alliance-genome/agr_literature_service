from os import path

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_update_references_all_mods import\
    get_pmids_with_xml


class TestPubmedUpdateReferencesAllMods:

    def test_get_pmids_with_xml(self):

        xml_path = path.join(path.dirname(path.abspath(__file__)), "../../sample_data/pubmed_xml/")
        pmids_with_xml = get_pmids_with_xml(xml_path)
        assert pmids_with_xml['33622238'] == 1
        assert pmids_with_xml['34354223'] == 1
