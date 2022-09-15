from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
    get_alliance_category_from_pubmed_types


class TestXmlToJson:

    def test_get_alliance_category_from_pubmed_types(self):
        assert get_alliance_category_from_pubmed_types(["Journal Article", "Research Support, N.I.H., Extramural",
                                                        "Research Support, Non-U.S. Gov't"]) == "Research_Article"
        assert get_alliance_category_from_pubmed_types(["Journal Article"]) == "Research_Article"
        # TODO: add more cases
