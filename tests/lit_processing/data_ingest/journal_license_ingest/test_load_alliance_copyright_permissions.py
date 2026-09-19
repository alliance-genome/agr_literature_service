"""
Tests for the seed-file parsing in load_alliance_copyright_permissions.py,
including the shipped data/alliance_copyright_permissions.tsv itself.
"""
import csv
from pathlib import Path

import pytest

from agr_literature_service.lit_processing.data_ingest.journal_license_ingest.load_alliance_copyright_permissions import (
    Stats,
    parse_grant_rows,
    publisher_matches,
    upsert_permission,
)

SEED_FILE = (
    Path(__file__).resolve().parents[4]
    / "agr_literature_service" / "lit_processing" / "data_ingest"
    / "journal_license_ingest" / "data" / "alliance_copyright_permissions.tsv"
)

BASE = {
    "publisher": "Test Press",
    "publisher_synonyms": "Test Press|Test Press (London)",
    "journal_title": "Journal of Testing",
    "permission_name": "Test Press: full permission",
    "permission_type": "full permission",
    "attribution_text": "Reproduced with permission. © <Publisher_name>",
    "can_display_images": "yes",
    "start_year": "",
    "end_year": "",
    "link_notes": "",
    "skip": "",
    "skip_reason": "",
}


def make_raw(**overrides):
    row = dict(BASE)
    row.update(overrides)
    return row


class TestParseGrantRows:

    def test_basic_row(self):
        rows, skipped = parse_grant_rows([make_raw()])
        assert skipped == []
        assert len(rows) == 1
        row = rows[0]
        assert row.publisher_synonyms == ["Test Press", "Test Press (London)"]
        assert row.can_display_images is True
        assert row.start_year is None and row.end_year is None
        # the sheet's Permission Type is persisted via the link notes
        assert row.notes == "Permission type: full permission."

    def test_skip_row_is_reported_not_loaded(self):
        rows, skipped = parse_grant_rows([make_raw(skip="yes", skip_reason="red in sheet")])
        assert rows == []
        assert len(skipped) == 1
        assert "red in sheet" in skipped[0]

    def test_year_range_and_notes(self):
        rows, _ = parse_grant_rows([make_raw(start_year="2010", end_year="2014", link_notes="Vols. 30-34")])
        assert rows[0].start_year == 2010
        assert rows[0].end_year == 2014
        assert rows[0].notes == "Permission type: full permission. Vols. 30-34"

    def test_notes_without_permission_type(self):
        rows, _ = parse_grant_rows([make_raw(permission_type="", link_notes="a caveat")])
        assert rows[0].notes == "a caveat"

    def test_no_display_row(self):
        rows, _ = parse_grant_rows([make_raw(can_display_images="no")])
        assert rows[0].can_display_images is False

    def test_malformed_row_raises(self):
        with pytest.raises(ValueError):
            parse_grant_rows([make_raw(attribution_text="")])
        with pytest.raises(ValueError):
            parse_grant_rows([make_raw(can_display_images="maybe")])

    def test_blank_line_ignored(self):
        rows, skipped = parse_grant_rows([make_raw(publisher="", journal_title="")])
        assert rows == [] and skipped == []


class TestDryRunPermissionCounting:
    """A permission name shared by several journal rows (Portland Press x6)
    must be counted and logged once in dry-run, matching what --apply creates."""

    def test_shared_name_counted_once(self):
        rows, _ = parse_grant_rows([
            make_raw(journal_title=f"Journal {i}") for i in range(3)
        ])
        stats = Stats()
        existing = {}
        for row in rows:
            result = upsert_permission(None, row, existing, stats, apply=False)
            assert result is None  # dry run never returns a model
        assert stats.permissions_created == 1
        assert stats.permissions_updated == 0
        assert stats.permissions_unchanged == 0

    def test_distinct_names_counted_separately(self):
        rows, _ = parse_grant_rows([
            make_raw(permission_name="Press A: full permission"),
            make_raw(permission_name="Press B: full permission"),
        ])
        stats = Stats()
        existing = {}
        for row in rows:
            upsert_permission(None, row, existing, stats, apply=False)
        assert stats.permissions_created == 2


class TestPublisherMatches:

    class FakeResource:
        def __init__(self, publisher):
            self.publisher = publisher

    def test_exact_synonym_matches(self):
        assert publisher_matches(self.FakeResource("Test Press (London)"),
                                 ["Test Press", "Test Press (London)"]) is True

    def test_normalization_ignores_punctuation_and_case(self):
        assert publisher_matches(self.FakeResource("test press, london"),
                                 ["Test Press, London"]) is True

    def test_unknown_publisher_is_mismatch(self):
        assert publisher_matches(self.FakeResource("Somebody Else"), ["Test Press"]) is False

    def test_empty_resource_publisher_passes(self):
        assert publisher_matches(self.FakeResource(None), ["Test Press"]) is True


class TestShippedSeedFile:
    """The curated TSV itself must parse cleanly and encode the working-group
    decisions: red publishers and eNeuro skipped, SfN year-ranged, both SfN
    2026- grants present with opposite display flags."""

    @pytest.fixture(scope="class")
    def parsed(self):
        with open(SEED_FILE, newline="") as fh:
            return parse_grant_rows(csv.DictReader(fh, delimiter="\t"))

    def test_red_publishers_folia_and_eneuro_are_skipped(self, parsed):
        _, skipped = parsed
        text = "\n".join(skipped)
        assert "Cold Spring Harbor" in text
        assert "Company of Biologists" in text
        assert "eNeuro" in text
        # Folia Biologica is parked until curators untangle whether the
        # permissions belong to the Krakow or the Praha journal
        assert "Folia Biologica" in text
        assert len(skipped) == 10

    def test_active_rows(self, parsed):
        rows, _ = parsed
        assert len(rows) == 18
        journals = {r.journal_title for r in rows}
        assert {"Genetics", "G3", "Biochem J", "The Journal of Cell Biology",
                "J Neurosci"} <= journals
        assert "Folia Biologica" not in journals
        # every non-SfN-exclusive grant allows display
        for r in rows:
            expected = "exclusive" not in r.permission_name
            assert r.can_display_images is expected, r.permission_name

    def test_sfn_year_ranges(self, parsed):
        rows, _ = parsed
        sfn = [(r.start_year, r.end_year, r.can_display_images)
               for r in rows if r.journal_title == "J Neurosci"]
        assert (None, 2009, True) in sfn
        assert (2010, 2014, True) in sfn
        assert (2015, 2025, True) in sfn
        assert (2026, None, True) in sfn    # OA CC-BY
        assert (2026, None, False) in sfn   # non-OA exclusive license
        assert len(sfn) == 5

    def test_oxford_is_journal_level_only(self, parsed):
        rows, _ = parsed
        oup = [r for r in rows if r.publisher.startswith("Oxford")]
        assert sorted(r.journal_title for r in oup) == ["G3", "Genetics"]
        for r in oup:
            assert "not by Oxford University Press" in (r.notes or "")
