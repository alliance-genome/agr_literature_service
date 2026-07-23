"""
Unit tests for the image-permission helpers in reference_crud.

_extract_publication_year and _build_resource_permission_metadata are pure.
get_effective_image_permission is exercised on the branches that do not need
real DB rows (a MagicMock session covers the single copyright-license query).
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from agr_literature_service.api.crud import reference_crud


def _ref(**kwargs):
    base = dict(date_published_start=None, date_published=None, date_published_end=None,
                resource_id=None, copyright_license_id=None)
    base.update(kwargs)
    return SimpleNamespace(**base)


class TestExtractPublicationYear:

    def test_prefers_date_published_start(self):
        ref = _ref(date_published_start="2019-05-01", date_published="2021")
        assert reference_crud._extract_publication_year(ref) == 2019

    def test_falls_back_to_date_published(self):
        ref = _ref(date_published="published in 2005 somewhere")
        assert reference_crud._extract_publication_year(ref) == 2005

    def test_falls_back_to_end_date(self):
        ref = _ref(date_published_end="1998")
        assert reference_crud._extract_publication_year(ref) == 1998

    def test_no_year_returns_none(self):
        assert reference_crud._extract_publication_year(_ref(date_published="no digits")) is None

    def test_all_empty_returns_none(self):
        assert reference_crud._extract_publication_year(_ref()) is None


class TestBuildResourcePermissionMetadata:

    def test_none_returns_all_null_fields(self):
        meta = reference_crud._build_resource_permission_metadata(None)
        assert meta["image_permission_id"] is None
        assert meta["resource_image_permission_id"] is None
        assert meta["start_year"] is None

    def test_populated_from_permission(self):
        image_permission = SimpleNamespace(
            image_permission_id=7, name="CC-BY", permission_text="ok",
            permission_url="http://u", permission_doc_url="http://d")
        rip = SimpleNamespace(
            image_permission=image_permission, resource_image_permission_id=3,
            start_year=2000, end_year=2010, notes="note")
        meta = reference_crud._build_resource_permission_metadata(rip)
        assert meta["image_permission_id"] == 7
        assert meta["image_permission_name"] == "CC-BY"
        assert meta["resource_image_permission_id"] == 3
        assert meta["start_year"] == 2000
        assert meta["end_year"] == 2010
        assert meta["notes"] == "note"


class TestGetEffectiveImagePermission:

    def test_default_no_permission(self):
        ref = _ref(date_published="2020")  # no resource, no copyright license
        result = reference_crud.get_effective_image_permission(MagicMock(), "AGRKB:1", reference=ref)
        assert result["can_display_images"] is False
        assert result["source"] == "none"
        assert result["publication_year"] == 2020
        assert result["image_permission_id"] is None

    def test_reference_open_access_takes_priority(self):
        ref = _ref(date_published="2020", copyright_license_id=42)
        fake_license = SimpleNamespace(copyright_license_id=42, name="CC0", open_access=True)
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one_or_none.return_value = fake_license
        result = reference_crud.get_effective_image_permission(db, "AGRKB:1", reference=ref)
        assert result["source"] == "reference_open_access"
        assert result["can_display_images"] is True
        assert result["copyright_license_name"] == "CC0"

    def test_reference_license_missing_falls_through_to_default(self):
        ref = _ref(date_published="2020", copyright_license_id=99)
        db = MagicMock()
        db.query.return_value.filter_by.return_value.one_or_none.return_value = None
        result = reference_crud.get_effective_image_permission(db, "AGRKB:1", reference=ref)
        assert result["source"] == "none"
