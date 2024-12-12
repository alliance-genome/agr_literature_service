import pytest  # noqa
from fastapi import status
from starlette.testclient import TestClient
from agr_literature_service.api.main import app
from tests.api.fixtures import auth_headers # noqa
from tests.api.test_mod import test_mod # noqa
from tests.api. test_reference import test_reference  # noqa
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.lit_processing.data_check.check_wft_in_progress import check_wft_in_progress


@pytest.fixture
def test_wft_check(db, auth_headers, test_reference, test_mod): # noqa
    print("***** Adding a test check what ever *****")
    with TestClient(app) as client:
        # set to files uploaded. "Start of process"
        new_wt = {"reference_curie": test_reference.new_ref_curie,
                  "mod_abbreviation": test_mod.new_mod_abbreviation,
                  "workflow_tag_id": "ATP:0000134",
                  }
        response = client.post(url="/workflow_tag/", json=new_wt, headers=auth_headers)
        assert response.status_code == status.HTTP_201_CREATED

        # Add "text_conversion needed"
        new_wt = {"reference_curie": test_reference.new_ref_curie,
                  "mod_abbreviation": test_mod.new_mod_abbreviation,
                  "workflow_tag_id": "ATP:0000162",
                  }
        response = client.post(url="/workflow_tag/", json=new_wt, headers=auth_headers)
        assert response.status_code == status.HTTP_201_CREATED

        # transition to 'in progress'
        # Transition table not loaded so change the values directly!
        wft = db.query(WorkflowTagModel).filter(WorkflowTagModel.workflow_tag_id == "ATP:0000162").one()
        wft.workflow_tag_id = "ATP:0000139"
        db.commit()

        # sanity check, make sure wft is not 139
        wft = db.query(WorkflowTagModel).filter(WorkflowTagModel.workflow_tag_id == "ATP:0000162").one()
        assert wft.workflow_tag_id == "ATP:0000139"

        # run the check which should set the wft to 'needed'
        check_wft_in_progress()
        wft = db.query(WorkflowTagModel).filter(WorkflowTagModel.workflow_tag_id == "ATP:0000162").one()
        assert wft.workflow_tag_id == "ATP:0000162"
