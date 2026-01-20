from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from .fixtures import auth_headers  # noqa


class TestResourceDescriptor:

    def test_get_res_des(self, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url="/resource_descriptor", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
