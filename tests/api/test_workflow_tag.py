from collections import namedtuple

import pytest
from sqlalchemy import and_
from starlette.testclient import TestClient
from fastapi import status
from unittest.mock import patch

from agr_literature_service.api.main import app
from agr_literature_service.api.models import WorkflowTagModel, ReferenceModel, WorkflowTransitionModel, ModModel
from agr_literature_service.api.crud.workflow_tag_crud import (
    get_workflow_process_from_tag,
    get_workflow_tags_from_process,
    load_workflow_parent_children)
from ..fixtures import load_workflow_parent_children_mock
from ..fixtures import db  # noqa
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

    @patch("agr_literature_service.api.crud.workflow_tag_crud.load_workflow_parent_children",
           load_workflow_parent_children_mock)
    def test_parent_child_dict(self, test_workflow_tag, auth_headers): # noqa
        # load_workflow_parent_children()
        assert get_workflow_process_from_tag('ATP:0000164') == 'ATP:0000161'
        children = get_workflow_tags_from_process('ATP:0000177')
        assert 'ATP:0000172' in children
        assert 'ATP:0000140' in children
        assert 'ATP:0000165' in children

    @patch("agr_literature_service.api.crud.workflow_tag_crud.load_workflow_parent_children",
           load_workflow_parent_children_mock)
    def test_transition_to_workflow_status_and_get_current_workflow_status(self, db, test_mod, test_reference,  # noqa
                                                                           auth_headers):  # noqa
        mod = db.query(ModModel).filter(ModModel.abbreviation == test_mod.new_mod_abbreviation).one()
        db.add(WorkflowTransitionModel(mod=mod, transition_from='ATP:0000141', transition_to='ATP:0000139'))
        db.add(WorkflowTransitionModel(mod=mod, transition_from='ATP:0000139', transition_to='ATP:0000141',
                                       requirements=["not_referencefiles_present"]))
        db.add(WorkflowTransitionModel(mod=mod, transition_from='ATP:0000139', transition_to='ATP:0000135',
                                       requirements=["not_referencefiles_present"], transition_type="manual_only"))
        db.add(WorkflowTransitionModel(mod=mod, transition_from='ATP:0000139', transition_to='ATP:0000134'))
        db.add(WorkflowTransitionModel(mod=mod, transition_from='ATP:0000135', transition_to='ATP:0000139',
                                       requirements=["referencefiles_present"]))
        db.add(WorkflowTransitionModel(mod=mod, transition_from='ATP:0000134', transition_to='ATP:0000141',
                                       requirements=["not_referencefiles_present"], transition_type="manual_only"))
        reference = db.query(ReferenceModel).filter(ReferenceModel.curie == test_reference.new_ref_curie).one()
        db.add(WorkflowTagModel(reference=reference, mod=mod, workflow_tag_id='ATP:0000141'))
        db.commit()
        with TestClient(app) as client:
            load_workflow_parent_children()
            transition_req = {
                "curie_or_reference_id": test_reference.new_ref_curie,
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "new_workflow_tag_atp_id": "ATP:0000139"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_200_OK
            assert db.query(WorkflowTagModel).filter(
                and_(
                    WorkflowTagModel.reference_id == reference.reference_id,
                    WorkflowTagModel.mod_id == mod.mod_id,
                    WorkflowTagModel.workflow_tag_id == 'ATP:0000139'
                )
            ).first()

            new_status_response = client.get(url=f"/workflow_tag/get_current_workflow_status/"
                                                 f"{test_reference.new_ref_curie}/{test_mod.new_mod_abbreviation}/"
                                                 f"{'ATP:0000140'}", headers=auth_headers)
            assert new_status_response.status_code == status.HTTP_200_OK
            assert new_status_response.json() == 'ATP:0000139'
            transition_req = {
                "curie_or_reference_id": test_reference.new_ref_curie,
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "new_workflow_tag_atp_id": "ATP:0000135",
                "transition_type": "manual",
            }
            client.post(url="/workflow_tag/transition_to_workflow_status", json=transition_req, headers=auth_headers)
            wrong_transition_req = {
                "curie_or_reference_id": test_reference.new_ref_curie,
                "mod_abbreviation": test_mod.new_mod_abbreviation,
                "new_workflow_tag_atp_id": "ATP:0000139"
            }
            response = client.post(url="/workflow_tag/transition_to_workflow_status", json=wrong_transition_req,
                                   headers=auth_headers)
            assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
