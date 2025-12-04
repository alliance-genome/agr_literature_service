# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    PersonModel,
    PersonSettingModel,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


PersonSettingTestData = namedtuple(
    "PersonSettingTestData",
    [
        "response",
        "new_person_setting_id",
        "person_id",
        "okta_id",
    ],
)


@pytest.fixture
def seeded_person(db):
    """Create a Person with okta_id for lookup endpoints."""
    person = PersonModel(
        display_name="Alice Curator",
        okta_id="okta-alice-123",
    )
    db.add(person)
    db.commit()
    db.refresh(person)
    return {"person_id": person.person_id, "okta_id": person.okta_id}


@pytest.fixture
def test_person_setting(db, auth_headers, seeded_person):  # noqa
    """Create a baseline person_setting row to reuse across tests."""
    with TestClient(app) as client:
        payload = {
            "person_id": seeded_person["person_id"],
            "component_name": "TopicEntityTable",
            "setting_name": "Default Columns",
            "default_setting": True,
            "json_settings": {"cols": ["gene", "species", "tag"]},
        }
        response = client.post("/person_setting/", json=payload, headers=auth_headers)
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield PersonSettingTestData(
            response=response,
            new_person_setting_id=body.get("person_setting_id"),
            person_id=seeded_person["person_id"],
            okta_id=seeded_person["okta_id"],
        )


class TestPersonSetting:

    def test_get_bad_person_setting(self):
        with TestClient(app) as client:
            response = client.get("/person_setting/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_person_setting(self, db, test_person_setting):  # noqa
        assert test_person_setting.response.status_code == status.HTTP_201_CREATED

        ps = (
            db.query(PersonSettingModel)
            .filter(PersonSettingModel.person_setting_id == test_person_setting.new_person_setting_id)
            .one()
        )
        assert ps.person_id == test_person_setting.person_id
        assert ps.component_name == "TopicEntityTable"
        assert ps.setting_name == "Default Columns"
        assert ps.default_setting is True

    def test_create_person_setting_invalid_person(self, auth_headers):
        with TestClient(app) as client:
            payload = {
                "person_id": 9999999,
                "component_name": "TopicEntityTable",
                "setting_name": "Bad Person",
                "default_setting": False,
                "json_settings": {},
            }
            res = client.post("/person_setting/", json=payload, headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_only_one_default_per_person_component(self, auth_headers, test_person_setting):  # noqa
        with TestClient(app) as client:
            dup_default = {
                "person_id": test_person_setting.person_id,
                "component_name": "TopicEntityTable",
                "setting_name": "Second Default",
                "default_setting": True,
                "json_settings": {"cols": ["status"]},
            }
            res = client.post("/person_setting/", json=dup_default, headers=auth_headers)
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            non_default = {
                "person_id": test_person_setting.person_id,
                "component_name": "TopicEntityTable",
                "setting_name": "Alt Layout",
                "default_setting": False,
                "json_settings": {"cols": ["species", "gene"]},
            }
            ok = client.post("/person_setting/", json=non_default, headers=auth_headers)
            assert ok.status_code == status.HTTP_201_CREATED

    def test_update_person_setting(self, db, auth_headers, test_person_setting):  # noqa
        with TestClient(app) as client:
            patch_payload = {
                "json_settings": {"cols": ["species", "gene", "tag", "status"]},
                "default_setting": True,
            }
            res = client.patch(
                f"/person_setting/{test_person_setting.new_person_setting_id}",
                json=patch_payload,
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_202_ACCEPTED
            assert res.json().get("message") == "updated"

            fetched = client.get(f"/person_setting/{test_person_setting.new_person_setting_id}")
            assert fetched.status_code == status.HTTP_200_OK
            body = fetched.json()
            assert body["person_setting_id"] == test_person_setting.new_person_setting_id
            assert body["default_setting"] is True
            assert "status" in str(body.get("json_settings"))

    def test_show_person_setting(self, test_person_setting):  # noqa
        with TestClient(app) as client:
            response = client.get(f"/person_setting/{test_person_setting.new_person_setting_id}")
            assert response.status_code == status.HTTP_200_OK
            body = response.json()
            assert body["person_setting_id"] == test_person_setting.new_person_setting_id
            assert body["component_name"] == "TopicEntityTable"

    def test_find_by_name(self, test_person_setting):  # noqa
        with TestClient(app) as client:
            res = client.get("/person_setting/by/name", params={"name": "Alice"})
            assert res.status_code == status.HTTP_200_OK
            rows = res.json()
            assert isinstance(rows, list)
            assert any(r["person_setting_id"] == test_person_setting.new_person_setting_id for r in rows)

            res_empty = client.get("/person_setting/by/name", params={"name": "ZZZ_NOT_FOUND"})
            assert res_empty.status_code == status.HTTP_200_OK
            assert res_empty.json() == []

    def test_destroy_person_setting(self, test_person_setting, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/person_setting/{test_person_setting.new_person_setting_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT

            res = client.get(f"/person_setting/{test_person_setting.new_person_setting_id}")
            assert res.status_code == status.HTTP_404_NOT_FOUND

            res = client.delete(
                f"/person_setting/{test_person_setting.new_person_setting_id}",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND
