"""
Unit tests for the image-permission helpers in reference_crud.

_extract_publication_year and _build_resource_permission_metadata are pure.
get_effective_image_permission is exercised on the branches that do not need
real DB rows (a MagicMock session covers the single copyright-license query).
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from agr_literature_service.api.crud import reference_crud
from agr_literature_service.api.models import (ReferenceModel, ResourceModel,
                                               CopyrightLicenseModel,
                                               ImagePermissionModel,
                                               ResourceImagePermissionModel)
from ..fixtures import db # noqa


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
        db = MagicMock()  # noqa
        db.query.return_value.filter_by.return_value.one_or_none.return_value = fake_license
        result = reference_crud.get_effective_image_permission(db, "AGRKB:1", reference=ref)
        assert result["source"] == "reference_open_access"
        assert result["can_display_images"] is True
        assert result["copyright_license_name"] == "CC0"

    def test_reference_license_missing_falls_through_to_default(self):
        ref = _ref(date_published="2020", copyright_license_id=99)
        db = MagicMock()  # noqa
        db.query.return_value.filter_by.return_value.one_or_none.return_value = None
        result = reference_crud.get_effective_image_permission(db, "AGRKB:1", reference=ref)
        assert result["source"] == "none"


def _mk_resource(db, curie, copyright_license_id=None, license_start_year=None): # noqa
    resource = ResourceModel(curie=curie, title="Journal",
                             copyright_license_id=copyright_license_id,
                             license_start_year=license_start_year)
    db.add(resource)
    db.commit()
    db.refresh(resource)
    return resource


def _mk_reference(db, curie, resource_id=None, date_published=None,  # noqa
                  copyright_license_id=None): # noqa
    ref = ReferenceModel(curie=curie, category="research_article",
                         resource_id=resource_id, date_published=date_published,
                         copyright_license_id=copyright_license_id)
    db.add(ref)
    db.commit()
    db.refresh(ref)
    return ref


def _mk_image_permission(db, name, can_display=True): # noqa
    ip = ImagePermissionModel(name=name, can_display_images=can_display,
                              permission_text="permission granted")
    db.add(ip)
    db.commit()
    db.refresh(ip)
    return ip


def _mk_rip(db, resource_id, image_permission_id, start_year=None, end_year=None): # noqa
    rip = ResourceImagePermissionModel(resource_id=resource_id,
                                       image_permission_id=image_permission_id,
                                       start_year=start_year, end_year=end_year)
    db.add(rip)
    db.commit()
    db.refresh(rip)
    return rip


class TestResourceImagePermissionForReference:

    def test_no_resource_id_returns_none(self, db): # noqa
        ref = _mk_reference(db, "AGRKB:101000100001")
        assert reference_crud._resource_image_permission_for_reference(db, ref) is None

    def test_no_permission_rows_returns_none(self, db): # noqa
        resource = _mk_resource(db, "AGR:AGR-Resource-100001")
        ref = _mk_reference(db, "AGRKB:101000100002", resource_id=resource.resource_id)
        assert reference_crud._resource_image_permission_for_reference(db, ref) is None

    def test_year_matches_dated_row(self, db): # noqa
        resource = _mk_resource(db, "AGR:AGR-Resource-100002")
        ip = _mk_image_permission(db, "perm-a")
        rip = _mk_rip(db, resource.resource_id, ip.image_permission_id,
                      start_year=2000, end_year=2010)
        ref = _mk_reference(db, "AGRKB:101000100003",
                            resource_id=resource.resource_id, date_published="2005")
        got = reference_crud._resource_image_permission_for_reference(db, ref)
        assert got is not None
        assert got.resource_image_permission_id == rip.resource_image_permission_id

    def test_year_matches_nothing_returns_none(self, db): # noqa
        resource = _mk_resource(db, "AGR:AGR-Resource-100003")
        ip = _mk_image_permission(db, "perm-b")
        _mk_rip(db, resource.resource_id, ip.image_permission_id,
                start_year=2000, end_year=2010)
        ref = _mk_reference(db, "AGRKB:101000100004",
                            resource_id=resource.resource_id, date_published="1990")
        assert reference_crud._resource_image_permission_for_reference(db, ref) is None

    def test_no_year_returns_earliest_undated_row(self, db): # noqa
        resource = _mk_resource(db, "AGR:AGR-Resource-100004")
        ip = _mk_image_permission(db, "perm-c")
        rip1 = _mk_rip(db, resource.resource_id, ip.image_permission_id)
        ref = _mk_reference(db, "AGRKB:101000100005",
                            resource_id=resource.resource_id)  # no date
        got = reference_crud._resource_image_permission_for_reference(db, ref)
        assert got.resource_image_permission_id == rip1.resource_image_permission_id


class TestGetEffectiveImagePermissionDb:

    def test_resource_open_access_path(self, db): # noqa
        lic = CopyrightLicenseModel(name="CC-BY-resource", open_access=True)
        db.add(lic)
        db.commit()
        db.refresh(lic)
        resource = _mk_resource(db, "AGR:AGR-Resource-100005",
                                copyright_license_id=lic.copyright_license_id,
                                license_start_year=2000)
        ref = _mk_reference(db, "AGRKB:101000100006",
                            resource_id=resource.resource_id, date_published="2005")
        result = reference_crud.get_effective_image_permission(db, ref.curie, reference=ref)
        assert result["source"] == "resource_open_access"
        assert result["can_display_images"] is True

    def test_resource_image_permission_path(self, db): # noqa
        ip = _mk_image_permission(db, "perm-effective", can_display=True)
        resource = _mk_resource(db, "AGR:AGR-Resource-100006")
        _mk_rip(db, resource.resource_id, ip.image_permission_id,
                start_year=2000, end_year=2010)
        ref = _mk_reference(db, "AGRKB:101000100007",
                            resource_id=resource.resource_id, date_published="2005")
        result = reference_crud.get_effective_image_permission(db, ref.curie, reference=ref)
        assert result["source"] == "resource_image_permission"
        assert result["can_display_images"] is True
        assert result["image_permission_name"] == "perm-effective"
