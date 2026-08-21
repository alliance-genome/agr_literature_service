"""
Tests for load_doaj_licenses.py
"""

import pytest
from unittest.mock import MagicMock

from agr_literature_service.lit_processing.data_ingest.journal_license_ingest.load_doaj_licenses import (
    find_matching_resources,
    get_most_restricted_license,
    normalize_issn,
    normalize_license_name,
    parse_doaj_csv,
    parse_license_list,
    update_resources,
)

DOAJ_CSV_HEADER = (
    '"Journal title","Journal ISSN (print version)",'
    '"Journal EISSN (online version)","Journal license",'
    '"When did the journal start to publish all content using an open license?"'
)


class TestLicenseParsing:

    def test_parse_license_list(self):
        assert parse_license_list("CC BY, CC BY-NC-ND") == ["CC BY", "CC BY-NC-ND"]
        assert parse_license_list("CC BY") == ["CC BY"]
        assert parse_license_list("") == []
        assert parse_license_list("  ") == []

    def test_normalize_license_name_aliases_public_domain(self):
        assert normalize_license_name("Public domain") == "CC0"
        assert normalize_license_name("cc by") == "CC BY"

    def test_most_restricted_license(self):
        assert get_most_restricted_license(["CC BY", "CC BY-NC-ND"]) == "CC BY-NC-ND"
        assert get_most_restricted_license(["CC0", "CC BY"]) == "CC BY"
        assert get_most_restricted_license(["CC BY"]) == "CC BY"
        assert get_most_restricted_license(["Public domain"]) == "CC0"

    def test_most_restricted_license_unrecognized(self):
        assert get_most_restricted_license(["Publisher's own license"]) is None

    def test_normalize_issn(self):
        assert normalize_issn("1234-5678") == "12345678"
        assert normalize_issn(" 1234-567x ") == "1234567X"
        assert normalize_issn("") == ""


class TestParseDoajCsv:

    def test_parse_maps_both_issns_to_same_journal(self):
        csv_content = DOAJ_CSV_HEADER + '\n' + \
            '"Genetics Journal","1234-5678","8765-4321","CC BY, CC BY-NC","2010"'
        result = parse_doaj_csv(csv_content)

        assert set(result.keys()) == {"12345678", "87654321"}
        assert result["12345678"] is result["87654321"]
        journal = result["12345678"]
        assert journal["title"] == "Genetics Journal"
        assert journal["license_list"] == ["CC BY", "CC BY-NC"]
        assert journal["oa_start_year"] == 2010

    def test_parse_skips_rows_without_license_or_issn(self):
        csv_content = DOAJ_CSV_HEADER + '\n' + \
            '"No License Journal","1111-1111","","","2010"\n' + \
            '"No ISSN Journal","","","CC BY","2010"\n' + \
            '"Good Journal","","2222-2222","CC BY",""'
        result = parse_doaj_csv(csv_content)

        assert set(result.keys()) == {"22222222"}
        assert result["22222222"]["oa_start_year"] is None

    def test_parse_invalid_year(self):
        csv_content = DOAJ_CSV_HEADER + '\n' + \
            '"Journal","1234-5678","","CC BY","not-a-year"'
        result = parse_doaj_csv(csv_content)
        assert result["12345678"]["oa_start_year"] is None

    def test_parse_missing_columns_raises(self):
        csv_content = '"Journal title","Journal license"\n"Journal","CC BY"'
        with pytest.raises(ValueError, match="missing expected column"):
            parse_doaj_csv(csv_content)


class TestFindMatchingResources:

    def test_matches_resources_by_issn_xref(self):
        journal = {"title": "Genetics Journal", "license_list": ["CC BY"], "oa_start_year": 2010}
        issn_to_journal = {"12345678": journal}

        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.all.return_value = [
            (10, "ISSN:1234-5678"),
            (11, "ISSN:9999-9999"),
        ]

        matches = find_matching_resources(mock_db, issn_to_journal)

        assert matches == [(10, journal)]

    def test_same_resource_multiple_issns_deduplicated(self):
        journal = {"title": "Genetics Journal", "license_list": ["CC BY"], "oa_start_year": 2010}
        issn_to_journal = {"12345678": journal, "87654321": journal}

        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.all.return_value = [
            (10, "ISSN:1234-5678"),
            (10, "ISSN:8765-4321"),
        ]

        matches = find_matching_resources(mock_db, issn_to_journal)

        assert matches == [(10, journal)]


class TestUpdateResources:

    @staticmethod
    def _make_resource(resource_id, license_list=None, license_start_year=None,
                       copyright_license_id=None):
        resource = MagicMock()
        resource.resource_id = resource_id
        resource.curie = f"AGRKB:10100000000000{resource_id}"
        resource.license_list = license_list
        resource.license_start_year = license_start_year
        resource.copyright_license_id = copyright_license_id
        return resource

    def test_updates_resource_missing_license(self):
        resource = self._make_resource(10)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.one_or_none.return_value = resource

        journal = {"title": "Genetics Journal", "license_list": ["CC BY", "CC BY-NC"],
                   "oa_start_year": 2010}
        license_map = {"CC BY": 1, "CC BY-NC": 3}

        stats = update_resources(mock_db, [(10, journal)], license_map)

        assert stats["updated"] == 1
        assert resource.license_list == ["CC BY", "CC BY-NC"]
        assert resource.license_start_year == 2010
        assert resource.copyright_license_id == 3  # most restricted: CC BY-NC
        mock_db.commit.assert_called()

    def test_skips_resource_with_existing_license_by_default(self):
        resource = self._make_resource(10, license_list=["CC BY-SA"], copyright_license_id=2)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.one_or_none.return_value = resource

        journal = {"title": "Genetics Journal", "license_list": ["CC BY"], "oa_start_year": 2010}

        stats = update_resources(mock_db, [(10, journal)], {"CC BY": 1})

        assert stats["updated"] == 0
        assert stats["skipped_has_license"] == 1
        assert resource.license_list == ["CC BY-SA"]

    def test_updates_existing_license_with_flag(self):
        resource = self._make_resource(10, license_list=["CC BY-SA"], copyright_license_id=2)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.one_or_none.return_value = resource

        journal = {"title": "Genetics Journal", "license_list": ["CC BY"], "oa_start_year": 2010}

        stats = update_resources(mock_db, [(10, journal)], {"CC BY": 1}, update_existing=True)

        assert stats["updated"] == 1
        assert resource.license_list == ["CC BY"]
        assert resource.copyright_license_id == 1

    def test_skips_unchanged_resource(self):
        resource = self._make_resource(10, license_list=["CC BY"], license_start_year=2010,
                                       copyright_license_id=1)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.one_or_none.return_value = resource

        journal = {"title": "Genetics Journal", "license_list": ["CC BY"], "oa_start_year": 2010}

        stats = update_resources(mock_db, [(10, journal)], {"CC BY": 1}, update_existing=True)

        assert stats["updated"] == 0
        assert stats["skipped_unchanged"] == 1

    def test_dry_run_does_not_modify(self):
        resource = self._make_resource(10)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.one_or_none.return_value = resource

        journal = {"title": "Genetics Journal", "license_list": ["CC BY"], "oa_start_year": 2010}

        stats = update_resources(mock_db, [(10, journal)], {"CC BY": 1}, dry_run=True)

        assert stats["updated"] == 1
        assert resource.license_list is None
        mock_db.commit.assert_not_called()
