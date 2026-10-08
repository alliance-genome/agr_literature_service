import json
import os
from os import environ

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import \
    extract_author_email, get_alliance_category_from_pubmed_types, generate_json
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
        # SCRUM-6513: an author's email comes from that author's own affiliations
        # ("Electronic address: ..." or a bare address), and only that author gets it.
        expected_emails = {
            "34530988": {19: "paolo.sordino@szn.it"},
            "30979869": {26: "isf20@cam.ac.uk", 27: "yongx@bcm.edu"},
            "30002370": {8: "baify@im.ac.cn"},
        }
        for pmid, by_rank in expected_emails.items():
            json_obj = json.load(open(os.path.join(base_path, "pubmed_json", pmid + ".json")))
            emails = {a["authorRank"]: a["email"] for a in json_obj["authors"] if "email" in a}
            assert emails == by_rank
        md5_filename = os.path.join(base_path, "pubmed_json", "md5sum")
        assert os.path.exists(md5_filename)
        for line in open(md5_filename):
            cols = line.split("\t")
            assert cols[0] in pmids
            assert cols[1] != ""


def test_extract_author_email():
    assert extract_author_email(
        ["Dept X, Kyoto, Japan. Electronic address: John.Doe@Kyoto-U.ac.jp."]) == "john.doe@kyoto-u.ac.jp"
    assert extract_author_email(["Lab A, Univ B.", "Univ C, USA. yongx@bcm.edu."]) == "yongx@bcm.edu"
    # first address wins when an affiliation lists several
    assert extract_author_email(["Email: a.b@c.org; c@d.edu"]) == "a.b@c.org"
    assert extract_author_email(["contact <x_y@foo.co.uk>"]) == "x_y@foo.co.uk"
    assert extract_author_email(["no address", "user@localhost"]) is None
    assert extract_author_email([]) is None
    assert extract_author_email(None) is None


def test_generate_json_skips_an_unparseable_xml_file(tmp_path, monkeypatch):
    """A broken cached download is reported as not found instead of aborting
    the whole run (ParseError seen 2026-10-08)."""
    from agr_literature_service.lit_processing.data_ingest.pubmed_ingest import xml_to_json
    xml_dir = tmp_path / "pubmed_xml"
    xml_dir.mkdir()
    (xml_dir / "1.xml").write_text('<?xml version="1.0" ?> <PubmedArticleSet> <PubmedArticle><MedlineCitation>'
                                   '<PMID Version="1">1</PMID><Article><ArticleTitle>T</ArticleTitle>'
                                   '<PublicationType>Journal Article</PublicationType>')
    monkeypatch.setattr(xml_to_json, "base_path", str(tmp_path) + "/")
    not_found = set()
    xml_to_json.generate_json(["1"], [], not_found, base_dir=str(tmp_path) + "/")
    assert not_found == {"1"}
    assert not (tmp_path / "pubmed_json" / "1.json").exists()
