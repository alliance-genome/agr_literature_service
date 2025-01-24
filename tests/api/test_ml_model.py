import io
import json
import os
import tempfile

import pytest
from fastapi import status
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.models import MLModel
from .fixtures import auth_headers  # noqa
from .test_mod import test_mod  # noqa
from ..fixtures import db  # noqa


model_file_test_content = b"This is a test joblib file."


@pytest.fixture
def test_ml_model(db, auth_headers, test_mod):  # noqa
    print("***** Adding a test ML model *****")
    with TestClient(app) as client:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".joblib") as tmp_file:
            tmp_file.write(model_file_test_content)
            tmp_file_path = tmp_file.name

        with open(tmp_file_path, "rb") as file:
            new_model = {
                "task_type": "document_classification",
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "topic": "ATP:0000061",
                "version_num": None,
                "file_extension": "joblib",
                "model_type": "MLP",
                "precision": 0.9,
                "recall": 0.8,
                "f1_score": 0.85,
                "parameters": "{'activation': 'relu', 'alpha': 0.0006896560619767168, 'beta_1': 0.0409026638675211, "
                              "'beta_2': 0.0010956501293432625, 'epsilon': 0.0016887047887665103, 'hidden_layer_sizes':"
                              " [500, 500, 500], 'learning_rate': 'constant', 'learning_rate_init': "
                              "0.01378769917615668, 'solver': 'sgd'}",
                "dataset_id": None
            }
            model_data_json = json.dumps(new_model)
            files = {
                "file": ("file.joblib", file, "application/octet-stream"),
                "model_data_file": ("model_data.txt", io.BytesIO(model_data_json.encode('utf-8')), "text/plain")
            }
            mod_auth_headers = auth_headers.copy()
            del mod_auth_headers["Content-Type"]
            response = client.post(
                url="/ml_model/upload",
                files=files,
                data=new_model,
                headers=mod_auth_headers
            )
        os.remove(tmp_file_path)
        yield response.json()
        client.delete(url=f"/ml_model/{response.json()['ml_model_id']}", headers=auth_headers)


test_ml_model2 = test_ml_model


class TestMLModel:

    def test_get_bad_model(self, test_mod):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_upload_model(self, db, test_ml_model, test_mod):  # noqa
        assert test_ml_model["ml_model_id"]
        # check db for model
        ml_model = db.query(MLModel).filter(MLModel.task_type == "document_classification").one()
        assert ml_model.mod.abbreviation == test_mod.new_mod_abbreviation
        assert ml_model.topic == "ATP:0000061"
        assert ml_model.version_num == 1

    def test_get_model_metadata(self, test_ml_model, test_mod):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/1")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["task_type"] == "document_classification"

    def test_get_latest_model_metadata(self, test_ml_model, test_ml_model2, test_mod):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061")
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["task_type"] == "document_classification"
            assert response.json()["version_num"] == 2

    def test_download_model_file(self, test_ml_model, test_ml_model2, test_mod):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/download/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/1")
            assert response.status_code == status.HTTP_200_OK
            assert response.headers["content-type"] == "application/octet-stream"
            assert response.content == model_file_test_content

    def test_download_latest_model_file(self, test_ml_model, test_mod):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/download/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061")
            assert response.status_code == status.HTTP_200_OK
            assert response.headers["content-type"] == "application/octet-stream"

    def test_destroy_model(self, test_ml_model, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/ml_model/{test_ml_model['ml_model_id']}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT
            # It should now give an error on lookup.
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/1")
            assert response.status_code == status.HTTP_404_NOT_FOUND
            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/ml_models/{test_ml_model['ml_model_id']}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
