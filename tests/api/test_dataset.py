from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import DatasetModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa
from .test_mod import test_mod # noqa

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
        with TestClient(app) as client:
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

    def test_download_dataset(self, test_mod, test_dataset):  # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/datasets/{test_mod.new_mod_abbreviation}/{test_atp_id}/document/")
            assert response.status_code == status.HTTP_200_OK
            dataset = response.json()
            assert dataset['mod_abbreviation'] == test_mod.new_mod_abbreviation
            assert dataset['data_type_topic'] == test_atp_id
            assert dataset['dataset_type'] == test_dataset_type
            assert dataset['dataset_note'] == "This is a test dataset"
            assert len(dataset['data']) == 0

    def test_download_dataset_wrong(self):
        with TestClient(app) as client:
            response = client.get(url="/datasets/NONEXISTENT/INVALID/FAKE/")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_update_dataset(self, db, auth_headers, test_dataset):  # noqa
        with TestClient(app) as client:
            updated_dataset = {
                "data_type_topic": "NEW_TOPIC",
                "dataset_type": "entity"
            }
            response = client.patch(url=f"/datasets/TEST/REFERENCE/SAMPLE/",
                                    json=updated_dataset, headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert response.json() == "updated"

            # Verify the update in the database
            dataset = db.query(DatasetModel).filter(DatasetModel.dataset_id == test_dataset.dataset_id).one()
            assert dataset.dataset_title == "Updated Test Dataset"
            assert dataset.dataset_release_version == "1.1"

    def test_delete_dataset(self, db, auth_headers, test_dataset):  # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/datasets/TEST/REFERENCE/SAMPLE/", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # Verify the deletion in the database
            dataset = db.query(DatasetModel).filter(DatasetModel.dataset_id == test_dataset.dataset_id).first()
            assert dataset is None

    def test_add_topic_entity_tag(self, db, auth_headers, test_dataset):  # noqa
        with TestClient(app) as client:
            # Assuming we have a topic entity tag with id 1
            response = client.post(url=f"/datasets/topic_entity_tag/TEST/REFERENCE/SAMPLE/?topic_entity_tag_id=1",
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

            # Verify the addition in the database
            dataset = db.query(DatasetModel).filter(DatasetModel.dataset_id == test_dataset.dataset_id).one()
            assert any(tag.id == 1 for tag in dataset.topic_entity_tags)

    def test_remove_topic_entity_tag(self, db, auth_headers, test_dataset):  # noqa
        with TestClient(app) as client:
            # First, add a topic entity tag
            client.post(url=f"/datasets/topic_entity_tag/TEST/REFERENCE/SAMPLE/?topic_entity_tag_id=1",
                        headers=auth_headers)

            # Now remove it
            response = client.delete(url=f"/datasets/topic_entity_tag/TEST/REFERENCE/SAMPLE/?topic_entity_tag_id=1",
                                     headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

            # Verify the removal in the database
            dataset = db.query(DatasetModel).filter(DatasetModel.dataset_id == test_dataset.dataset_id).one()
            assert all(tag.id != 1 for tag in dataset.topic_entity_tags)


if __name__ == "__main__":
    pytest.main()