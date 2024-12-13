import pytest  # noqa
from fastapi import status
from starlette.testclient import TestClient
from sqlalchemy import text
from agr_literature_service.api.main import app
from ...api.fixtures import auth_headers  # noqa
from ...api.test_mod import test_mod  # noqa
from ...api.test_reference import test_reference  # noqa
from ...fixtures import db  # noqa
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.lit_processing.data_check.check_wft_in_progress import check_wft_in_progress


class TestWorkflowTagCheck:


    def test_wft_check(self, db, auth_headers, test_reference, test_mod): # noqa
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
            wft1 = db.query(WorkflowTagModel).filter(WorkflowTagModel.workflow_tag_id == "ATP:0000162").one()
            wft1.workflow_tag_id = "ATP:0000198"
            db.commit()

            # sanity check, make sure wft is 198
            wft2 = db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_workflow_tag_id == wft1.reference_workflow_tag_id).one()
            assert wft2.workflow_tag_id == "ATP:0000198"

            # debug, uncomment if needed
            # transactions = client.get(url=f"/workflow_tag/{wft2.reference_workflow_tag_id}/versions").json()
            # for tran in transactions:
            #     print(tran)

            # run the check which should set the wft to 'needed'
            check_wft_in_progress(db, debug=True)
            check_wft_in_progress(db, debug=False)

            # debug, uncomment if needed
            # transactions = client.get(url=f"/workflow_tag/{wft2.reference_workflow_tag_id}/versions").json()
            # for tran in transactions:
            #     print(tran)

            # debug uncomment if needed
            # wfts = db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_id == wft2.reference_id).all()
            # for wft in wfts:
            #    print(wft)

            wft = db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_workflow_tag_id == wft1.reference_workflow_tag_id).one()
            assert wft.workflow_tag_id == "ATP:0000162"

            # set back to in progress
            wft.workflow_tag_id = "ATP:0000139"
            db.commit()

            # set "initial state" > 6 weeks ago and run again
            # Need to edit the version table!!!
            sql = f"""update workflow_tag_version 
                         set date_created = '2023-11-01' 
                         where workflow_tag_id = 'ATP:0000134' and
                               reference_id = {wft1.reference_id} and
                               operation_type = 0 """
            db.execute(text(sql))

            check_wft_in_progress(db, debug=False)

            # should now be set to failed (164)
            wft = db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_workflow_tag_id == wft1.reference_workflow_tag_id).one()
            assert wft.workflow_tag_id == "ATP:0000164"
