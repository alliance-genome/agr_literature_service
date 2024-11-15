from collections import namedtuple
from typing import Type

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import DatasetModel
from agr_literature_service.api.models.dataset_model import DatasetEntryModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa
from .test_mod import test_mod # noqa
from .test_topic_entity_tag import test_topic_entity_tag # noqa
from .test_reference import test_reference # noqa
from .test_topic_entity_tag_source import test_topic_entity_tag_source # noqa

TestDatasetData = namedtuple('TestDatasetData', ['response', 'mod_abbreviation', 'data_type',
                                                 'dataset_type', 'version'])


test_atp_id = "TEST_ATP_ID"
test_dataset_type = "document"


@pytest.fixture
def test_dataset(db, test_mod, auth_headers):  # noqa
    print("***** Adding a test dataset *****")
    with TestClient(app) as client:
        new_dataset = {
            "mod_abbreviation": test_mod.new_mod_abbreviation,
            "data_type": test_atp_id,
            "dataset_type": test_dataset_type,
            "title": "Test dataset",
            "description": "This is a test dataset"
        }
        response = client.post(url="/datasets/", json=new_dataset, headers=auth_headers)
        new_dataset_metadata = response.json()
        yield TestDatasetData(response, new_dataset_metadata["mod_abbreviation"], new_dataset_metadata["data_type"],
                              new_dataset_metadata["dataset_type"], new_dataset_metadata["version"])


class TestDataset:
    def test_create_dataset(self, db, auth_headers, test_mod, test_dataset):  # noqa
        assert test_dataset.response.status_code == status.HTTP_201_CREATED
        # check db for dataset
        dataset = db.query(DatasetModel).join(DatasetModel.mod).filter(
            DatasetModel.mod.has(abbreviation=test_dataset.mod_abbreviation),
            DatasetModel.data_type == test_dataset.data_type,
            DatasetModel.dataset_type == test_dataset.dataset_type,
            DatasetModel.version == test_dataset.version
        ).one()
        assert test_dataset.mod_abbreviation == test_mod.new_mod_abbreviation
        assert test_dataset.data_type == test_atp_id
        assert test_dataset.dataset_type == test_dataset_type
        assert dataset.mod.abbreviation == test_mod.new_mod_abbreviation
        assert dataset.data_type == test_atp_id
        assert dataset.dataset_type == test_dataset_type


    def test_show_dataset(self, db, test_dataset, test_mod):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/datasets/metadata/{test_dataset.mod_abbreviation}/{test_dataset.data_type}/"
                                      f"{test_dataset.dataset_type}/{test_dataset.version}/")
            assert response.status_code == status.HTTP_200_OK
            dataset = response.json()
            assert dataset['mod_abbreviation'] == test_mod.new_mod_abbreviation
            assert dataset['data_type'] == test_atp_id
            assert dataset['dataset_type'] == test_dataset_type
            assert dataset['description'] == "This is a test dataset"

    def test_add_dataset_entry(self, db, auth_headers, test_dataset, test_topic_entity_tag):  # noqa
        with TestClient(app) as client:
            dataset_entry_data = {
                "mod_abbreviation": test_dataset.mod_abbreviation,
                "data_type": test_dataset.data_type,
                "dataset_type": test_dataset.dataset_type,
                "version": test_dataset.version,
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "entity": None,
                "supporting_topic_entity_tag_id": test_topic_entity_tag.new_tet_id
            }
            response = client.post(url="/datasets/data_entry/", json=dataset_entry_data, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED

            dataset_metadata = client.get(url=f"/datasets/metadata/{test_dataset.mod_abbreviation}/"
                                              f"{test_dataset.data_type}/{test_dataset.dataset_type}/"
                                              f"{test_dataset.version}/").json()
            # Verify the addition in the database
            dataset: Type[DatasetModel] = db.query(DatasetModel).filter(
                DatasetModel.dataset_id == dataset_metadata["dataset_id"]).one()
            dataset_entry: DatasetEntryModel = dataset.dataset_entries[0]
            assert dataset_entry.reference_curie == test_topic_entity_tag.related_ref_curie


    def test_remove_dataset_entry(self, db, auth_headers, test_dataset, test_topic_entity_tag):  # noqa
        with TestClient(app) as client:
            dataset_entry_data = {
                "mod_abbreviation": test_dataset.mod_abbreviation,
                "data_type": test_dataset.data_type,
                "dataset_type": test_dataset.dataset_type,
                "version": test_dataset.version,
                "reference_curie": test_topic_entity_tag.related_ref_curie,
                "entity": None,
                "supporting_topic_entity_tag_id": test_topic_entity_tag.new_tet_id
            }
            dataset_metadata = client.get(url=f"/datasets/metadata/{test_dataset.mod_abbreviation}/"
                                              f"{test_dataset.data_type}/{test_dataset.dataset_type}/"
                                              f"{test_dataset.version}/").json()
            response = client.post(url="/datasets/data_entry/", json=dataset_entry_data, headers=auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            response = client.delete(url=f"/datasets/data_entry/{test_dataset.mod_abbreviation}/"
                                         f"{test_dataset.data_type}/"
                                         f"{test_dataset.dataset_type}/{test_dataset.version}/"
                                         f"{test_topic_entity_tag.related_ref_curie}/", headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

            # Verify the removal in the database
            dataset = db.query(DatasetModel).filter(DatasetModel.dataset_id == dataset_metadata["dataset_id"]).one()
            assert len(dataset.dataset_entries) == 0

    def test_download_dataset(self, test_mod, test_dataset):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/datasets/download/{test_mod.new_mod_abbreviation}/{test_dataset.data_type}/"
                                      f"{test_dataset.dataset_type}/{test_dataset.version}")
            assert response.status_code == status.HTTP_200_OK
            dataset = response.json()
            assert dataset['mod_abbreviation'] == test_mod.new_mod_abbreviation
            assert dataset['data_type'] == test_atp_id
            assert dataset['dataset_type'] == test_dataset_type
            assert dataset['description'] == "This is a test dataset"
            assert len(dataset['data_training']) == 0
            assert len(dataset['data_testing']) == 0

    def test_download_dataset_wrong(self):
        with TestClient(app) as client:
            response = client.get(url="/datasets/NONEXISTENT/INVALID/FAKE/")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_update_dataset(self, db, auth_headers, test_dataset):  # noqa
        with TestClient(app) as client:
            updated_dataset = {
                "title": "Updated title",
                "description": "Updated description"
            }
            response = client.patch(url=f"/datasets/{test_dataset.mod_abbreviation}/{test_dataset.data_type}/"
                                        f"{test_dataset.dataset_type}/{test_dataset.version}",
                                    json=updated_dataset, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED
            assert response.json() == "updated"
            dataset_metadata = client.get(url=f"/datasets/metadata/{test_dataset.mod_abbreviation}/"
                                              f"{test_dataset.data_type}/{test_dataset.dataset_type}/"
                                              f"{test_dataset.version}/").json()

            # Verify the update in the database
            dataset = db.query(DatasetModel).filter(DatasetModel.dataset_id == dataset_metadata["dataset_id"]).one()
            assert dataset.title == "Updated title"

    def test_delete_dataset(self, db, auth_headers, test_dataset):  # noqa
        with TestClient(app) as client:
            dataset_metadata = client.get(url=f"/datasets/metadata/{test_dataset.mod_abbreviation}/"
                                              f"{test_dataset.data_type}/{test_dataset.dataset_type}/"
                                              f"{test_dataset.version}/").json()
            response = client.delete(url=f"/datasets/{test_dataset.mod_abbreviation}/{test_dataset.data_type}/"
                                         f"{test_dataset.dataset_type}/{test_dataset.version}/", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # Verify the deletion in the database
            dataset = db.query(DatasetModel).filter(DatasetModel.dataset_id == dataset_metadata["dataset_id"]).first()
            assert dataset is None
