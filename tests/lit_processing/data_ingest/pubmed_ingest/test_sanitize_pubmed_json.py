import json
import os
from os import environ

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.sanitize_pubmed_json import \
    sanitize_pubmed_json_list
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.xml_to_json import generate_json
from tests.utils import cleanup_tmp_files


class TestSanitizePubmedJson:

    def test_sanitize_pubmed_json_list(self):
        try:
            base_path = environ.get('XML_PATH')
            pmids = ["10022914", "26051182"]
            generate_json(pmids, [], base_dir=os.path.join(
                os.path.dirname(__file__), "../../../../agr_literature_service/lit_processing/tests/"))
            generate_json(pmids, [])
            inject_object = {"modCorpusAssociations": [
                {
                    "modAbbreviation": "FB",
                    "modCorpusSortSource": "mod_pubmed_search",
                    "corpus": None
                }
            ]}
            sanitize_pubmed_json_list(pmids, [inject_object])
            sanitized_json_file = os.path.join(base_path, "sanitized_reference_json/REFERENCE_PUBMED_PMID.json")
            assert os.path.exists(sanitized_json_file)
            json_obj = json.load(open(sanitized_json_file))
            nlms = [json.load(open(os.path.join(base_path, "pubmed_json", pmid + ".json")))["nlm"] for pmid in pmids]
            assert len(json_obj) == 2
            assert "modCorpusAssociations" in json_obj[0]
            assert "modCorpusAssociations" in json_obj[1]
            assert json_obj[0]["primaryId"] == "PMID:" + pmids[0]
            assert json_obj[1]["primaryId"] == "PMID:" + pmids[1]
            assert "allianceCategory" in json_obj[0]
            assert json_obj[0]["resource"] in ["NLM:" + nlm for nlm in nlms]
            assert json_obj[1]["resource"] in ["NLM:" + nlm for nlm in nlms]
            assert type(json_obj[0]["dateLastModified"]) == str
            assert json_obj[0]["dateLastModified"] != ""
            assert type(json_obj[0]["datePublished"]) == str
            assert json_obj[0]["datePublished"] != ""
            assert type(json_obj[1]["dateArrivedInPubmed"]) == str
            assert json_obj[1]["dateArrivedInPubmed"] != ""
        finally:
            cleanup_tmp_files()
