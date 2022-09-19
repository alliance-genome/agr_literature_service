import json
import os

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.generate_pubmed_nlm_resource import \
    populate_from_local_file, populate_nlm_info

from ....fixtures import cleanup_tmp_files_when_done  # noqa


class TestGeneratePubmedNlmResource:

    def test_populate_from_local_file(self):  # noqa
        data = populate_from_local_file(base_dir=os.path.join(os.path.dirname(__file__), "../../sample_data/"))
        assert len(data.split("\n")) == 42

    def test_populate_nlm_info(self):
        data = populate_from_local_file(base_dir=os.path.join(os.path.dirname(__file__), "../../sample_data/"))
        nlm_info = populate_nlm_info(data)
        expected_output = [{
                               'primaryId': 'NLM:0372516', 'nlm': '0372516', 'crossReferences': [{'id': 'NLM:0372516'}],
                               'title': 'Biochemical and biophysical research communications',
                               'isoAbbreviation': 'Biochem Biophys Res Commun',
                               'medlineAbbreviation': 'Biochem Biophys Res Commun', 'printISSN': '0006-291X',
                               'onlineISSN': '1090-2104'
                           }, {
                               'primaryId': 'NLM:101262417', 'nlm': '101262417',
                               'crossReferences': [{'id': 'NLM:101262417'}],
                               'title': 'The journal of physiological sciences : JPS',
                               'isoAbbreviation': 'J Physiol Sci', 'medlineAbbreviation': 'J Physiol Sci',
                               'printISSN': '1880-6546', 'onlineISSN': '1880-6562'
                           }, {
                               'primaryId': 'NLM:101719179', 'nlm': '101719179',
                               'crossReferences': [{'id': 'NLM:101719179'}], 'title': 'Communications biology',
                               'isoAbbreviation': 'Commun Biol', 'medlineAbbreviation': 'Commun Biol',
                               'onlineISSN': '2399-3642'
                           }, {
                               'primaryId': 'NLM:101231978', 'nlm': '101231978',
                               'crossReferences': [{'id': 'NLM:101231978'}],
                               'title': "Alzheimer's & dementia : the journal of the Alzheimer's Association",
                               'isoAbbreviation': 'Alzheimers Dement', 'medlineAbbreviation': 'Alzheimers Dement',
                               'printISSN': '1552-5260', 'onlineISSN': '1552-5279'
                           }, {
                               'primaryId': 'NLM:101759238', 'nlm': '101759238',
                               'crossReferences': [{'id': 'NLM:101759238'}], 'title': 'microPublication biology',
                               'isoAbbreviation': 'MicroPubl Biol', 'medlineAbbreviation': 'MicroPubl Biol',
                               'onlineISSN': '2578-9430'
                           }]
        assert json.dumps(nlm_info, sort_keys=True) == json.dumps(expected_output, sort_keys=True)
