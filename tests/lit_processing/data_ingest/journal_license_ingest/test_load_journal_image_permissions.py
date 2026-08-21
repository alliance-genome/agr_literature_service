"""
Tests for the permission classification logic in load_journal_image_permissions.py
"""

from agr_literature_service.lit_processing.data_ingest.journal_license_ingest.load_journal_image_permissions import (
    detect_permission_type,
    has_positive_permission_signal,
)


def make_row(**overrides):
    row = {
        "Journal (NLM abbrev)": "Test J",
        "Publisher": "Test Publisher",
        "Full Journal Name": "Test Journal",
        "WB Acknowledgements": "",
        "FB": "", "MGI": "", "RGD": "", "SGD": "", "WB": "", "XB": "", "ZFIN": "",
        "License type": "",
        "Hybrid Journal": "",
        "Comments": "",
    }
    row.update(overrides)
    return row


class TestHasPositivePermissionSignal:

    def test_blanket_permission_is_positive(self):
        row = make_row(WB="Blanket from publisher")
        assert has_positive_permission_signal(row, subset_can_display=False) is True

    def test_plain_cc_license_is_positive(self):
        row = make_row(**{"License type": "CC BY 4.0"})
        assert has_positive_permission_signal(row, subset_can_display=False) is True

    def test_empty_row_is_negative(self):
        assert has_positive_permission_signal(make_row(), subset_can_display=False) is False

    def test_hybrid_creative_commons_is_negative(self):
        """Hybrid journals only license a subset of articles under CC, so
        'Hybrid Creative Commons' alone must not grant image display."""
        row = make_row(MGI="Hybrid Creative Commons")
        assert has_positive_permission_signal(row, subset_can_display=False) is False

    def test_hybrid_column_with_cc_license_is_negative(self):
        row = make_row(**{"License type": "CC BY 4.0", "Hybrid Journal": "yes"})
        assert has_positive_permission_signal(row, subset_can_display=False) is False

    def test_hybrid_with_explicit_blanket_grant_is_positive(self):
        row = make_row(MGI="Hybrid Creative Commons", WB="Blanket from publisher")
        assert has_positive_permission_signal(row, subset_can_display=False) is True

    def test_hybrid_with_contract_is_positive(self):
        row = make_row(MGI="Hybrid Creative Commons", SGD="Contract")
        assert has_positive_permission_signal(row, subset_can_display=False) is True

    def test_hybrid_with_granted_is_positive(self):
        row = make_row(**{"Hybrid Journal": "yes, Full text access, and OA"},
                       WB="Blanket from publisher", ZFIN="Granted")
        assert has_positive_permission_signal(row, subset_can_display=False) is True

    def test_oa_matches_as_whole_word_only(self):
        row = make_row(SGD="OA")
        assert has_positive_permission_signal(row, subset_can_display=False) is True
        row = make_row(Comments="images have broad usage restrictions")
        assert has_positive_permission_signal(row, subset_can_display=False) is False


class TestDetectPermissionType:

    def test_open_access_from_oa_word(self):
        assert detect_permission_type(make_row(SGD="OA")) == "Open Access"

    def test_no_open_access_from_oa_substring(self):
        assert detect_permission_type(make_row(SGD="broad permission")) is None

    def test_blanket_takes_priority(self):
        row = make_row(WB="Blanket", SGD="OA")
        assert detect_permission_type(row) == "Blanket Permission"
