from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app


class TestResourceDescriptor:

    def test_get_res_des(self):
        with TestClient(app) as client:
            response = client.get(url="/resource_descriptor")
            assert response.status_code == status.HTTP_200_OK
