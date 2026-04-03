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
        # ISSN values are now stored as cross-references with issn_type field
        expected_output = [
            {
                'primaryId': 'NLM:0372516',
                'nlm': '0372516',
                'crossReferences': [
                    {'id': 'NLM:0372516'},
                    {'id': 'ISSN:0006-291X', 'issn_type': 'Print'},
                    {'id': 'ISSN:1090-2104', 'issn_type': 'Online'}
                ],
                'title': 'Biochemical and biophysical research communications',
                'titleAbbreviation': 'Biochem Biophys Res Commun'
            },
            {
                'primaryId': 'NLM:101262417',
                'nlm': '101262417',
                'crossReferences': [
                    {'id': 'NLM:101262417'},
                    {'id': 'ISSN:1880-6546', 'issn_type': 'Print'},
                    {'id': 'ISSN:1880-6562', 'issn_type': 'Online'}
                ],
                'title': 'The journal of physiological sciences : JPS',
                'titleAbbreviation': 'J Physiol Sci'
            },
            {
                'primaryId': 'NLM:101719179',
                'nlm': '101719179',
                'crossReferences': [
                    {'id': 'NLM:101719179'},
                    {'id': 'ISSN:2399-3642', 'issn_type': 'Online'}
                ],
                'title': 'Communications biology',
                'titleAbbreviation': 'Commun Biol'
            },
            {
                'primaryId': 'NLM:101231978',
                'nlm': '101231978',
                'crossReferences': [
                    {'id': 'NLM:101231978'},
                    {'id': 'ISSN:1552-5260', 'issn_type': 'Print'},
                    {'id': 'ISSN:1552-5279', 'issn_type': 'Online'}
                ],
                'title': "Alzheimer's & dementia : the journal of the Alzheimer's Association",
                'titleAbbreviation': 'Alzheimers Dement'
            },
            {
                'primaryId': 'NLM:101759238',
                'nlm': '101759238',
                'crossReferences': [
                    {'id': 'NLM:101759238'},
                    {'id': 'ISSN:2578-9430', 'issn_type': 'Online'}
                ],
                'title': 'microPublication biology',
                'titleAbbreviation': 'MicroPubl Biol'
            }
        ]
        assert json.dumps(nlm_info, sort_keys=True) == json.dumps(expected_output, sort_keys=True)
