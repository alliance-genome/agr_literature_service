from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest import pubmed_search_new_references
from agr_literature_service.lit_processing.utils.report_utils import send_pubmed_search_report

MODULE = 'agr_literature_service.lit_processing.data_ingest.pubmed_ingest.pubmed_search_new_references'


class TestQueryModsWhenPubmedIsDown:

    @patch(f'{MODULE}.send_pubmed_search_report')
    @patch(f'{MODULE}.process_retracted_papers')
    @patch(f'{MODULE}.query_pubmed_for_mod', side_effect=RuntimeError('HTTP Error 500: Internal Server Error'))
    @patch(f'{MODULE}.get_pmids_from_exclude_list', return_value=set())
    @patch(f'{MODULE}.get_mod_abbreviations', return_value=['SGD', 'XB'])
    @patch(f'{MODULE}.set_global_user_id')
    @patch(f'{MODULE}.get_db', side_effect=lambda: iter([MagicMock()]))
    @patch(f'{MODULE}.sqlalchemy_load_ref_xref', return_value=({}, {}, {}))
    @patch(f'{MODULE}.update_resource_pubmed_nlm')
    def test_report_is_sent_listing_failed_mods(self, _nlm, _xref, _db, _user, _mods, _exclude,
                                                _query, _retracted, mock_report):
        # Every MOD failing used to raise UnboundLocalError on log_path, so no
        # report went out and nobody learnt the search had been skipped (SCRUM-6603).
        pubmed_search_new_references.query_mods(None, None)

        mock_report.assert_called_once()
        assert mock_report.call_args.kwargs['failed_mods'] == ['SGD', 'XB']


class TestSendPubmedSearchReport:

    @patch('agr_literature_service.lit_processing.utils.report_utils.send_report')
    def test_failed_mods_are_reported_as_errors(self, mock_send_report):
        send_pubmed_search_report({'all': set(), 'SGD': set(), 'XB': set()}, ['SGD', 'XB'], None, None, {}, [],
                                  failed_mods=['SGD', 'XB'])

        message = mock_send_report.call_args.args[1]
        assert 'SGD, XB' in message
        assert 'No new papers from PubMed Search' not in message

    @patch('agr_literature_service.lit_processing.utils.report_utils.send_report')
    def test_no_failed_mods_keeps_existing_message(self, mock_send_report):
        send_pubmed_search_report({'all': set()}, ['SGD'], None, None, {}, [])

        assert mock_send_report.call_args.args[1] == "No new papers from PubMed Search"
