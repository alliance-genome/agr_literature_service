from unittest import mock

from starlette.testclient import TestClient
from fastapi import status

import agr_literature_service.api.resource_descriptor_cache as rdc
from agr_literature_service.api.main import app
from .fixtures import auth_headers  # noqa


ATEAM_SAMPLE = [
    {
        "prefix": "TESTMOD",
        "name": "Test Mod",
        "synonyms": ["TM", "TMOD"],
        "idExample": "TESTMOD:123",
        "idPattern": r"^TESTMOD:\d+$",
        "defaultUrlTemplate": "http://test.org/[%s]",
        "resourcePages": [{"name": "homepage", "urlTemplate": "http://test.org/"}],
    }
]


class TestResourceDescriptor:

    def test_get_res_des(self, auth_headers):  # noqa
        rdc._seed([rdc.ResourceDescriptor(db_prefix="XREF", default_url="http://x/[%s]")])
        with TestClient(app) as client:
            response = client.get(url="/resource_descriptor", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert any(item["db_prefix"] == "XREF" for item in response.json())

    def test_force_refresh_loads_from_ateam(self, monkeypatch):
        monkeypatch.setattr(rdc, "_fetch", rdc._fetch_from_ateam)
        with mock.patch("agr_curation_api.AGRCurationAPIClient") as MockClient:
            MockClient.return_value.get_resource_descriptors.return_value = ATEAM_SAMPLE
            result = rdc.force_refresh()
        assert "TESTMOD" in {r.db_prefix for r in result}
