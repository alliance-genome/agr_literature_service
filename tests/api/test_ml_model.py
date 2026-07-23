import io
import json
import os
import tempfile

import pytest
from botocore.exceptions import ClientError
from fastapi import status, HTTPException
from starlette.testclient import TestClient

from agr_literature_service.api.crud import ml_model_crud
from agr_literature_service.api.main import app
from agr_literature_service.api.models import MLModel, ModModel
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
                "dataset_id": None,
                "production": True,
                "negated": True,
                "data_novelty": "ATP:0000062",
                "species": None,
                "file_classes": ["main"]
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

    def test_get_bad_model(self, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/-1",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
            response = client.get(
                url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000062",
                headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND
            response = client.get(
                url=f"/ml_model/download/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000062",
                headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_upload_model(self, db, test_ml_model, test_mod):  # noqa
        assert test_ml_model["ml_model_id"]
        # check db for model
        ml_model = db.query(MLModel).filter(MLModel.task_type == "document_classification").one()
        assert ml_model.mod.abbreviation == test_mod.new_mod_abbreviation
        assert ml_model.topic == "ATP:0000061"
        assert ml_model.version_num == 1
        assert ml_model.data_novelty == "ATP:0000062"
        assert ml_model.file_classes == ["main"]
        # audit fields are auto-stamped on create (AuditedModel)
        assert ml_model.date_created is not None
        assert ml_model.date_updated is not None
        assert ml_model.created_by is not None
        assert ml_model.updated_by is not None
        # and surfaced in the response schema
        assert test_ml_model["date_created"] is not None
        assert test_ml_model["created_by"] is not None

    def test_get_all_models_no_filter(self, test_ml_model, test_ml_model2, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url="/ml_model/all", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            payload = response.json()
            assert isinstance(payload, list)
            assert len(payload) >= 2
            assert all("ml_model_id" in m for m in payload)

    def test_get_all_models_filter_by_mod(self, test_ml_model, test_ml_model2, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(
                url=f"/ml_model/all?mod_abbreviation={test_mod.new_mod_abbreviation}",
                headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            payload = response.json()
            assert isinstance(payload, list)
            assert len(payload) >= 2
            assert all(m["mod_abbreviation"] == test_mod.new_mod_abbreviation for m in payload)

    def test_get_all_models_unknown_mod(self, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url="/ml_model/all?mod_abbreviation=BOGUS", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_get_all_models_has_atp_name_fields(self, test_ml_model, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url="/ml_model/all", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            payload = response.json()
            assert len(payload) >= 1
            row = next(m for m in payload if m["topic"] == "ATP:0000061")
            assert "topic_name" in row
            assert "data_novelty_name" in row
            assert "species_name" in row
            assert isinstance(row["topic_name"], str)
            assert isinstance(row["data_novelty_name"], str)

    def test_get_model_metadata(self, test_ml_model, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/1",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["task_type"] == "document_classification"

    def test_get_latest_model_metadata(self, test_ml_model, test_ml_model2, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["task_type"] == "document_classification"
            assert response.json()["version_num"] == 2

    def test_download_model_file(self, test_ml_model, test_ml_model2, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/download/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/1",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.headers["content-type"] == "application/octet-stream"
            assert response.content == model_file_test_content

    def test_download_latest_model_file(self, test_ml_model, test_mod, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/ml_model/download/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.headers["content-type"] == "application/octet-stream"

# Now try with multiple models. Inserting these via direct sql to avoid files etc,
# and just the bare minimum needed for testing new version stuff.
    def test_various_version_model(self, db, test_mod, auth_headers):  # noqa

        mod = db.query(ModModel).filter_by(abbreviation=test_mod.new_mod_abbreviation).one()
        assert mod.short_name == "AtDB"
        assert mod.full_name == "Test genome database"
        print(f" mod_id = {test_mod.new_mod_id}")

        first = MLModel(mod_id=test_mod.new_mod_id,
                        version_num=10,
                        topic="ATP:0000061",
                        production=False,
                        task_type='document_classification',
                        file_extension="joblib",
                        model_type='old')
        db.add(first)
        db.commit()
        prod = MLModel(mod_id=test_mod.new_mod_id,
                       version_num=11,
                       topic="ATP:0000061",
                       production=True,
                       task_type='document_classification',
                       file_extension="joblib",
                       model_type='prod')
        db.add(prod)
        db.commit()
        last = MLModel(mod_id=test_mod.new_mod_id,
                       version_num=12,
                       topic="ATP:0000061",
                       production=False,
                       task_type='document_classification',
                       file_extension="joblib",
                       model_type='latest')
        db.add(last)
        db.commit()

        with TestClient(app) as client:
            # fetch by version number
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/10",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["task_type"] == "document_classification"
            assert response.json()["version_num"] == 10
            assert response.json()["model_type"] == first.model_type

            # fetch latest by using NOT specifying the version
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["task_type"] == "document_classification"
            assert response.json()["version_num"] == 12
            assert response.json()["model_type"] == last.model_type

            # fetch latest by specifying latest
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/latest",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["task_type"] == "document_classification"
            assert response.json()["version_num"] == 12
            assert response.json()["model_type"] == last.model_type

            # fetch production version
            response = client.get(url=f"/ml_model/metadata/document_classification/{test_mod.new_mod_abbreviation}/ATP:0000061/production",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json()["task_type"] == "document_classification"
            assert response.json()["version_num"] == prod.version_num
            assert response.json()["model_type"] == prod.model_type


def test_download_model_missing_s3_file_returns_404(db, monkeypatch):  # noqa
    # A DB record can exist without its S3 .gz object (e.g. after destroy(),
    # which deletes the S3 file but keeps the row). download_file_from_s3
    # swallows the ClientError and returns False without writing the local
    # file. download_model_file must then raise a 404, not let gzip.open blow
    # up with an unhandled FileNotFoundError (which surfaces as an HTTP 500).
    mod = ModModel(abbreviation="MLT", short_name="MLT", full_name="ML test mod")
    db.add(mod)
    db.commit()
    model = MLModel(mod_id=mod.mod_id,
                    version_num=99,
                    topic="ATP:0000110",
                    production=False,
                    task_type="biocuration_entity_extraction",
                    file_extension="joblib",
                    model_type="no_file")
    db.add(model)
    db.commit()

    monkeypatch.setattr(
        "agr_literature_service.api.crud.ml_model_crud.download_file_from_s3",
        lambda *args, **kwargs: False)

    with pytest.raises(HTTPException) as exc_info:
        ml_model_crud.download_model_file(
            db, "biocuration_entity_extraction", "MLT", "ATP:0000110", "99")
    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND


def test_download_model_corrupt_gzip_returns_502(db, monkeypatch):  # noqa
    # The S3 object exists but is not valid gzip (truncated / wrong content).
    # download_model_file must surface a 502 rather than let gzip.open raise an
    # unhandled BadGzipFile (HTTP 500).
    mod = ModModel(abbreviation="MLT2", short_name="MLT2", full_name="ML test mod 2")
    db.add(mod)
    db.commit()
    model = MLModel(mod_id=mod.mod_id,
                    version_num=42,
                    topic="ATP:0000110",
                    production=False,
                    task_type="biocuration_entity_extraction",
                    file_extension="joblib",
                    model_type="no_file")
    db.add(model)
    db.commit()

    def fake_download(filepath, *args, **kwargs):
        # Simulate a successful download of a non-gzip payload.
        with open(filepath, "wb") as fh:
            fh.write(b"this is not gzip content")
        return True

    monkeypatch.setattr(
        "agr_literature_service.api.crud.ml_model_crud.download_file_from_s3", fake_download)

    with pytest.raises(HTTPException) as exc_info:
        ml_model_crud.download_model_file(
            db, "biocuration_entity_extraction", "MLT2", "ATP:0000110", "42")
    assert exc_info.value.status_code == status.HTTP_502_BAD_GATEWAY


def test_download_model_s3_error_returns_502(db, monkeypatch):  # noqa
    # download_file_from_s3 only swallows ClientError; if the storage call raises
    # (credentials/connectivity/other), download_model_file must return 502, not 500.
    mod = ModModel(abbreviation="MLT3", short_name="MLT3", full_name="ML test mod 3")
    db.add(mod)
    db.commit()
    model = MLModel(mod_id=mod.mod_id,
                    version_num=7,
                    topic="ATP:0000110",
                    production=False,
                    task_type="biocuration_entity_extraction",
                    file_extension="joblib",
                    model_type="no_file")
    db.add(model)
    db.commit()

    def raise_client_error(*args, **kwargs):
        raise ClientError({"Error": {"Code": "500", "Message": "boom"}}, "GetObject")

    monkeypatch.setattr(
        "agr_literature_service.api.crud.ml_model_crud.download_file_from_s3", raise_client_error)

    with pytest.raises(HTTPException) as exc_info:
        ml_model_crud.download_model_file(
            db, "biocuration_entity_extraction", "MLT3", "ATP:0000110", "7")
    assert exc_info.value.status_code == status.HTTP_502_BAD_GATEWAY
