from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status
from unittest.mock import patch

from agr_literature_service.api.main import app
from agr_literature_service.api.models import WorkflowTagModel, ReferenceModel
from agr_literature_service.api.crud.workflow_tag_crud import get_parent, get_children
from ..fixtures import db # noqa
from .fixtures import auth_headers # noqa
from .test_reference import test_reference # noqa
from .test_mod import test_mod # noqa

TestWTData = namedtuple('TestWTData', ['response', 'new_wt_id', 'related_ref_curie', 'related_mod_id',
                                       'related_mod_abbreviation'])


@pytest.fixture
def test_workflow_tag(db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test workflow tag *****")
    with TestClient(app) as client:
        new_wt = {"reference_curie": test_reference.new_ref_curie,
                  "mod_abbreviation": test_mod.new_mod_abbreviation,
                  "workflow_tag_id": "ont1",
                  }
        response = client.post(url="/workflow_tag/", json=new_wt, headers=auth_headers)
        yield TestWTData(response, response.json(), test_reference.new_ref_curie, test_mod.new_mod_id,
                         test_mod.new_mod_abbreviation)


@patch("agr_literature_service.api.crud.workflow_tag_crud.get_ancestors")
def mock_get_ancestors(name):
    # MUST start with ATP:0000003 for this to work
    print(f"***** Mocking get_ancestors name = {name}")
    if name == 'ATP:0000003':
        return ['colour', 'size', 'type']
    elif name == 'colour':
        return ['red', 'blue', 'green']
    elif name == 'size':
        return ['1', '2', '3']
    elif name == 'red':
        return ['dark red', 'crimson']
    elif name == 'type':
        return ['5', '6']
    else:
        print("returning NOTHING!!")
        return []


@patch("agr_literature_service.api.crud.topic_entity_tag_crud_utils.get_descendants")
def mock_get_desc(name):
    # MUST start with ATP:0000003 for this to work
    print(f"***** Mocking get_ancestors name = {name}")
    if name == 'ATP:0000003':
        return ['colour', 'size', 'type']
    elif name == 'colour':
        return ['red', 'blue', 'green']
    elif name == 'size':
        return ['1', '2', '3']
    elif name == 'red':
        return ['dark red', 'crimson']
    elif name == 'type':
        return ['5', '6']
    else:
        print("returning NOTHING!!")
        return []


class TestWorkflowTag:

    def test_get_bad_ref_wt(self):
        with TestClient(app) as client:
            response = client.get(url="/workflow_tag/-1")
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_create_bad_missing_args(self, test_workflow_tag, auth_headers): # noqa
        with TestClient(app) as client:
            xml = {"reference_curie": test_workflow_tag.related_ref_curie,
                   "workflow_tag_id": "ont1"
                   }
            response = client.post(url="/workflow_tag/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            xml = {"reference_curie": test_workflow_tag.related_ref_curie,
                   "mod_abbreviation": test_workflow_tag.related_mod_abbreviation
                   }
            response = client.post(url="/workflow_tag/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            xml = {"mod_abbreviation": test_workflow_tag.related_mod_abbreviation,
                   "workflow_tag_id": "ont1"
                   }
            response = client.post(url="/workflow_tag/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            xml = {'mod_abbreviation': "",
                   'workflow_tag_id': "ont tgba",
                   'reference_curie': test_workflow_tag.related_ref_curie}
            response = client.post(url="/workflow_tag/", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_ref_wt(self, db, test_workflow_tag): # noqa
        assert test_workflow_tag.response.status_code == status.HTTP_201_CREATED
        # check results in database
        ref_wt_obj = db.query(WorkflowTagModel). \
            join(ReferenceModel,
                 WorkflowTagModel.reference_id == ReferenceModel.reference_id). \
            filter(ReferenceModel.curie == test_workflow_tag.related_ref_curie).one()
        assert ref_wt_obj.workflow_tag_id == "ont1"

    def test_patch_ref_wt(self, db, test_workflow_tag, auth_headers): # noqa
        with TestClient(app) as client:
            # change workflow_tag
            xml = {"workflow_tag_id": "ont test patch",
                   "mod_abbreviation": test_workflow_tag.related_mod_abbreviation
                   }
            response = client.patch(url=f"/workflow_tag/{test_workflow_tag.new_wt_id}", json=xml, headers=auth_headers)
            assert response.status_code == status.HTTP_202_ACCEPTED

            ref_wt_obj: WorkflowTagModel = db.query(WorkflowTagModel).filter(
                WorkflowTagModel.reference_workflow_tag_id == test_workflow_tag.new_wt_id).one()
            assert ref_wt_obj.reference.curie == test_workflow_tag.related_ref_curie
            assert ref_wt_obj.workflow_tag_id == "ont test patch"

            transactions = client.get(url=f"/workflow_tag/{test_workflow_tag.new_wt_id}/versions").json()
            assert transactions[0]['changeset']['workflow_tag_id'][1] == 'ont1'
            assert not transactions[0]['changeset']['mod_id'][0]
            assert transactions[1]['changeset']['workflow_tag_id'][0] == 'ont1'
            assert transactions[1]['changeset']['workflow_tag_id'][1] == 'ont test patch'

    def test_patch_ref_wt_blank_mod_abbr(self, db, test_workflow_tag, auth_headers):  # noqa
        with TestClient(app) as client:
            # change workflow_tag
            patch_data = {"mod_abbreviation": ""}
            response = client.patch(url=f"/workflow_tag/{test_workflow_tag.new_wt_id}", json=patch_data,
                                    headers=auth_headers)
            # assert response is response
            # TODO uncomment this test after fixing the api
            assert response.status_code == status.HTTP_202_ACCEPTED
            response = client.get(url=f"/workflow_tag/{test_workflow_tag.new_wt_id}")
            assert response.json()["mod_abbreviation"] == ""

    def test_show_ref_wt(self, test_workflow_tag): # noqa
        with TestClient(app) as client:
            response = client.get(url=f"/workflow_tag/{test_workflow_tag.new_wt_id}")
            assert response.status_code == status.HTTP_200_OK
            res = response.json()
            assert res['reference_curie'] == test_workflow_tag.related_ref_curie
            assert res['workflow_tag_id'] == 'ont1'
            assert res['mod_abbreviation'] == test_workflow_tag.related_mod_abbreviation

    def test_destroy_ref_wt(self, test_workflow_tag, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.delete(url=f"/workflow_tag/{test_workflow_tag.new_wt_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_204_NO_CONTENT

            # It should now give an error on lookup.
            response = client.get(url=f"/workflow_tag/{test_workflow_tag.new_wt_id}")
            assert response.status_code == status.HTTP_404_NOT_FOUND

            # Deleting it again should give an error as the lookup will fail.
            response = client.delete(url=f"/workflow_tag/{test_workflow_tag.new_wt_id}", headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    def test_parent_child_dict(self, test_workflow_tag, auth_headers): # noqa
        with TestClient(app) as client:
            response = client.get(url="/workflow_tag/reset_workflow_dict", headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert get_parent('colour') == 'ATP:0000003'
            children = get_children('ATP:0000003')
            assert 'colour' in children
            assert 'size' in children
            assert 'type' in children
