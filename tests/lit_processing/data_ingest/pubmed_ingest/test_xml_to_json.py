import json
import os
from os import environ

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
    get_alliance_category_from_pubmed_types, generate_json
from ....fixtures import cleanup_tmp_files_when_done # noqa


class TestXmlToJson:

    def test_get_alliance_category_from_pubmed_types(self):
        assert get_alliance_category_from_pubmed_types(["Journal Article", "Research Support, N.I.H., Extramural",
                                                        "Research Support, Non-U.S. Gov't"]) == "Research_Article"
        assert get_alliance_category_from_pubmed_types(["Journal Article"]) == "Research_Article"
        assert get_alliance_category_from_pubmed_types(["Journal Article", "Review"]) == "Review_Article"
        assert get_alliance_category_from_pubmed_types(["Journal Article", "Published Erratum"]) == "Correction"
        assert get_alliance_category_from_pubmed_types(["Journal Article", "Research Support, Non-U.S. Gov't",
                                                        "Retraction Notice"]) == "Retraction"
        assert get_alliance_category_from_pubmed_types(["Preprint"]) == "Preprint"

    def test_generate_json(self, cleanup_tmp_files_when_done): # noqa
        base_path = environ.get('XML_PATH')
        pmids = ["10022914", "20301347", "21413225", "26051182", "28308877", "30003105", "31188077", "34530988",
                 "10206683", "21290765", "21873635", "27899353", "2", "30110134", "31193955", "7567443",
                 "19678847", "21413221", "21976771", "28304499", "30002370", "30979869", "33054145", "8"]
        generate_json(pmids, [], base_dir=os.path.join(
            os.path.dirname(__file__), "../../../../agr_literature_service/lit_processing/tests/"))
        for pmid in pmids:
            filename = os.path.join(base_path, "pubmed_json", pmid + ".json")
            assert os.path.exists(filename)
            assert os.stat(filename).st_size > 0
            json_obj = json.load(open(filename))
            assert "title" in json_obj
            # the pubmed field is populated only for the references that have an ArticleIdType that is equal to
            # pubmed
            if "pubmed" in json_obj:
                assert json_obj["pubmed"] == pmid
                assert "journal" in json_obj
                assert "nlm" in json_obj
                assert "authors" in json_obj
            else:
                assert "PMID:" + pmid in [xref['id'] for xref in json_obj["crossReferences"]]
            assert "allianceCategory" in json_obj
            assert "publicationStatus" in json_obj
        md5_filename = os.path.join(base_path, "pubmed_json", "md5sum")
        assert os.path.exists(md5_filename)
        for line in open(md5_filename):
            cols = line.split("\t")
            assert cols[0] in pmids
            assert cols[1] != ""
