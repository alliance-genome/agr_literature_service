from starlette.testclient import TestClient
from fastapi import status

import agr_literature_service.api.resource_descriptor_cache as rdc
from agr_literature_service.api.main import app
from ..fixtures import db  # noqa: F401
from .fixtures import auth_headers, descriptor_rows  # noqa: F401


class TestResourceDescriptor:

    def test_get_res_des(self, auth_headers):  # noqa
        rdc._seed([rdc.ResourceDescriptor(db_prefix="XREF", default_url="http://x/[%s]")])
        with TestClient(app) as client:
            response = client.get(url="/resource_descriptor", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert any(item["db_prefix"] == "XREF" for item in response.json())

    def test_force_refresh_loads_from_ateam_tables(self, monkeypatch, descriptor_rows):  # noqa: F811
        monkeypatch.setattr(rdc, "_fetch", rdc._fetch_from_db)
        result = rdc.force_refresh()
        assert "TESTMOD" in {r.db_prefix for r in result}
