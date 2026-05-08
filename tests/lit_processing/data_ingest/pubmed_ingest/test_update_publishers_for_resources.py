"""
Tests for update_publishers_for_resources.py
"""

from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources import (
    find_resources_missing_publisher_with_nlm,
    fetch_nlm_catalog_data,
    update_resource,
    process_resources,
)


class TestFindResourcesMissingPublisherWithNlm:
    """Tests for find_resources_missing_publisher_with_nlm function."""

    def test_finds_resources_with_nlm_no_publisher(self):
        """Should find resources that have NLM cross-ref but no publisher."""
        mock_db = MagicMock()
        mock_db.query.return_value.join.return_value.filter.return_value.order_by.return_value.all.return_value = [
            (1, 'AGRKB:101000000000001', 'NLM:0410462'),
            (2, 'AGRKB:101000000000002', 'NLM:0372516'),
        ]

        results = find_resources_missing_publisher_with_nlm(mock_db)

        assert len(results) == 2
        assert results[0] == (1, 'AGRKB:101000000000001', '0410462')
        assert results[1] == (2, 'AGRKB:101000000000002', '0372516')

    def test_finds_resources_with_limit(self):
        """Should respect the limit parameter."""
        mock_db = MagicMock()
        mock_query = mock_db.query.return_value.join.return_value.filter.return_value.order_by.return_value
        mock_query.limit.return_value.all.return_value = [
            (1, 'AGRKB:101000000000001', 'NLM:0410462'),
        ]

        results = find_resources_missing_publisher_with_nlm(mock_db, limit=1)

        mock_query.limit.assert_called_once_with(1)
        assert len(results) == 1

    def test_returns_empty_list_when_no_matches(self):
        """Should return empty list when no resources match criteria."""
        mock_db = MagicMock()
        mock_db.query.return_value.join.return_value.filter.return_value.order_by.return_value.all.return_value = []

        results = find_resources_missing_publisher_with_nlm(mock_db)

        assert results == []


class TestFetchNlmCatalogData:
    """Tests for fetch_nlm_catalog_data function."""

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.search_nlm_catalog')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.fetch_nlm_catalog_xml')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.parse_nlm_catalog_xml')
    def test_fetches_and_parses_catalog_data(self, mock_parse, mock_fetch, mock_search):
        """Should fetch and parse NLM catalog data successfully."""
        mock_search.return_value = '410462'
        mock_fetch.return_value = '<xml>...</xml>'
        mock_parse.return_value = {
            'publisher': 'Nature Publishing Group',
            'titleSynonyms': ['Nature (London)'],
            'title': 'Nature'
        }

        result = fetch_nlm_catalog_data('0410462')

        mock_search.assert_called_once_with('0410462', 'nlmid')
        mock_fetch.assert_called_once_with('410462')
        assert result['publisher'] == 'Nature Publishing Group'
        assert result['titleSynonyms'] == ['Nature (London)']

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.search_nlm_catalog')
    def test_returns_empty_dict_when_no_uid_found(self, mock_search):
        """Should return empty dict when NLM catalog search returns no UID."""
        mock_search.return_value = ''

        result = fetch_nlm_catalog_data('9999999')

        assert result == {}

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.search_nlm_catalog')
    def test_returns_empty_dict_on_exception(self, mock_search):
        """Should return empty dict when an exception occurs."""
        mock_search.side_effect = Exception('API error')

        result = fetch_nlm_catalog_data('0410462')

        assert result == {}


class TestUpdateResource:
    """Tests for update_resource function."""

    def test_updates_publisher_when_empty(self):
        """Should update publisher when resource has no publisher."""
        mock_db = MagicMock()
        mock_resource = MagicMock()
        mock_resource.publisher = None
        mock_resource.title_synonyms = None
        mock_db.query.return_value.filter_by.return_value.one_or_none.return_value = mock_resource

        result = update_resource(mock_db, 1, 'Nature Publishing Group', None)

        assert result is True
        assert mock_resource.publisher == 'Nature Publishing Group'
        mock_db.add.assert_called_once_with(mock_resource)

    def test_updates_publisher_when_empty_string(self):
        """Should update publisher when resource has empty string publisher."""
        mock_db = MagicMock()
        mock_resource = MagicMock()
        mock_resource.publisher = ''
        mock_resource.title_synonyms = []
        mock_db.query.return_value.filter_by.return_value.one_or_none.return_value = mock_resource

        result = update_resource(mock_db, 1, 'Elsevier', None)

        assert result is True
        assert mock_resource.publisher == 'Elsevier'

    def test_does_not_overwrite_existing_publisher(self):
        """Should not overwrite existing publisher."""
        mock_db = MagicMock()
        mock_resource = MagicMock()
        mock_resource.publisher = 'Existing Publisher'
        mock_resource.title_synonyms = []
        mock_db.query.return_value.filter_by.return_value.one_or_none.return_value = mock_resource

        result = update_resource(mock_db, 1, 'New Publisher', None)

        assert result is False
        assert mock_resource.publisher == 'Existing Publisher'

    def test_adds_new_title_synonyms(self):
        """Should add new title synonyms without duplicates."""
        mock_db = MagicMock()
        mock_resource = MagicMock()
        mock_resource.publisher = 'Publisher'
        mock_resource.title_synonyms = ['Existing Synonym']
        mock_db.query.return_value.filter_by.return_value.one_or_none.return_value = mock_resource

        result = update_resource(mock_db, 1, None, ['New Synonym', 'Existing Synonym'])

        assert result is True
        # Should have both synonyms (merged, no duplicates)
        assert set(mock_resource.title_synonyms) == {'Existing Synonym', 'New Synonym'}

    def test_does_not_add_duplicate_synonyms(self):
        """Should not update if all synonyms already exist."""
        mock_db = MagicMock()
        mock_resource = MagicMock()
        mock_resource.publisher = 'Publisher'
        mock_resource.title_synonyms = ['Synonym A', 'Synonym B']
        mock_db.query.return_value.filter_by.return_value.one_or_none.return_value = mock_resource

        result = update_resource(mock_db, 1, None, ['Synonym A', 'Synonym B'])

        assert result is False

    def test_dry_run_does_not_modify(self):
        """Should not modify resource in dry-run mode."""
        mock_db = MagicMock()
        mock_resource = MagicMock()
        mock_resource.publisher = None
        mock_resource.title_synonyms = None
        mock_db.query.return_value.filter_by.return_value.one_or_none.return_value = mock_resource

        result = update_resource(mock_db, 1, 'Publisher', ['Synonym'], dry_run=True)

        assert result is True
        mock_db.add.assert_not_called()

    def test_returns_false_when_resource_not_found(self):
        """Should return False when resource not found."""
        mock_db = MagicMock()
        mock_db.query.return_value.filter_by.return_value.one_or_none.return_value = None

        result = update_resource(mock_db, 999, 'Publisher', None)

        assert result is False


class TestProcessResources:
    """Tests for process_resources function."""

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.find_resources_missing_publisher_with_nlm')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.fetch_nlm_catalog_data')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.update_resource')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.time.sleep')
    def test_processes_resources_successfully(self, mock_sleep, mock_update, mock_fetch, mock_find):
        """Should process resources and return correct stats."""
        mock_db = MagicMock()
        mock_find.return_value = [
            (1, 'AGRKB:101000000000001', '0410462'),
            (2, 'AGRKB:101000000000002', '0372516'),
        ]
        mock_fetch.side_effect = [
            {'publisher': 'Publisher A', 'titleSynonyms': ['Syn A']},
            {'publisher': 'Publisher B'},
        ]
        mock_update.return_value = True

        stats = process_resources(mock_db, dry_run=False)

        assert stats['processed'] == 2
        assert stats['updated'] == 2
        assert stats['skipped'] == 0
        assert stats['errors'] == 0
        mock_db.commit.assert_called_once()

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.find_resources_missing_publisher_with_nlm')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.fetch_nlm_catalog_data')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.time.sleep')
    def test_skips_resources_with_no_catalog_data(self, mock_sleep, mock_fetch, mock_find):
        """Should skip resources when no catalog data is found."""
        mock_db = MagicMock()
        mock_find.return_value = [(1, 'AGRKB:101000000000001', '9999999')]
        mock_fetch.return_value = {}

        stats = process_resources(mock_db, dry_run=True)

        assert stats['processed'] == 1
        assert stats['skipped'] == 1
        assert stats['updated'] == 0

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.find_resources_missing_publisher_with_nlm')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.fetch_nlm_catalog_data')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.time.sleep')
    def test_counts_errors(self, mock_sleep, mock_fetch, mock_find):
        """Should count errors when exceptions occur."""
        mock_db = MagicMock()
        mock_find.return_value = [(1, 'AGRKB:101000000000001', '0410462')]
        mock_fetch.side_effect = Exception('Unexpected error')

        stats = process_resources(mock_db, dry_run=False)

        assert stats['processed'] == 1
        assert stats['errors'] == 1

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.find_resources_missing_publisher_with_nlm')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.time.sleep')
    def test_dry_run_does_not_commit(self, mock_sleep, mock_find):
        """Should not commit in dry-run mode."""
        mock_db = MagicMock()
        mock_find.return_value = []

        process_resources(mock_db, dry_run=True)

        mock_db.commit.assert_not_called()

    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.find_resources_missing_publisher_with_nlm')
    @patch('agr_literature_service.lit_processing.data_ingest.pubmed_ingest.update_publishers_for_resources.time.sleep')
    def test_respects_limit_parameter(self, mock_sleep, mock_find):
        """Should pass limit to find_resources function."""
        mock_db = MagicMock()
        mock_find.return_value = []

        process_resources(mock_db, limit=5)

        mock_find.assert_called_once_with(mock_db, 5)
