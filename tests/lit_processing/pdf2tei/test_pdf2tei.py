import io
import json
import os
from unittest.mock import patch

import pytest
from starlette import status
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.models import WorkflowTransitionModel, ReferencefileModel
from agr_literature_service.lit_processing.pdf2tei.pdf2tei import main as convert_pdf_to_tei
from ...api.fixtures import auth_headers  # noqa
from ...api.test_mod import test_mod  # noqa
from ...api.test_reference import test_reference  # noqa
from ...api.test_workflow_tag import get_descendants_mock  # noqa
from ...fixtures import db  # noqa


class TestPdf2TEI:

    @pytest.mark.webtest
    @patch("agr_literature_service.api.crud.workflow_tag_crud.get_descendants", get_descendants_mock)
    def test_pdf2tei(self, db, auth_headers, test_reference, test_mod): # noqa
        with TestClient(app) as client:
            mod_response = client.get(url=f"/mod/{test_mod.new_mod_abbreviation}")
            mod_abbreviation = mod_response.json()["abbreviation"]
            new_mca = {
                "mod_abbreviation": mod_abbreviation,
                "reference_curie": test_reference.new_ref_curie,
                "corpus": True,
                "mod_corpus_sort_source": 'mod_pubmed_search'
            }
            client.post(url="/reference/mod_corpus_association/", json=new_mca, headers=auth_headers)

            transitions_to_add = [
                ["ATP:0000141", "ATP:0000134", ["referencefiles_present"],
                 ["proceed_on_value::category::thesis::ATP:0000162"], "on_success"],
                ["ATP:0000134", "ATP:0000162", [], [], 'text_convert_job'],
                ["ATP:0000162", "ATP:0000163", [], [], 'on_success'],
                ["ATP:0000162", "ATP:0000164", [], [], 'on_failed']
            ]

            for transition_to_add in transitions_to_add:
                db.add(WorkflowTransitionModel(mod_id=test_mod.new_mod_id,
                                               transition_from=transition_to_add[0],
                                               transition_to=transition_to_add[1],
                                               requirements=transition_to_add[2],
                                               actions=transition_to_add[3],
                                               condition=transition_to_add[4]))
            db.commit()
            req_data = {
                "curie_or_reference_id": test_reference.new_ref_curie,
                "mod_abbreviation": mod_abbreviation,
                "new_workflow_tag_atp_id": "ATP:0000141",
                "transition_type": "manual"
            }
            client.post(url="/workflow_tag/transition_to_workflow_status", json=req_data, headers=auth_headers)
            with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "test_data", "test.pdf"),
                      "rb") as f:
                pdf_bytes = f.read()
            metadata = {
                "reference_curie": test_reference.new_ref_curie,
                "display_name": "test",
                "file_class": "main",
                "file_publication_status": "final",
                "file_extension": "pdf",
                "pdf_type": "pdf",
                "mod_abbreviation": mod_abbreviation
            }
            metadata_json = json.dumps(metadata)
            files = {
                "file": ("test.pdf", io.BytesIO(pdf_bytes), "application/pdf"),
                "metadata_file": ("metadata.txt", io.BytesIO(metadata_json.encode('utf-8')), "text/plain")
            }
            mod_auth_headers = auth_headers.copy()
            del mod_auth_headers["Content-Type"]
            response = client.post(url="/reference/referencefile/file_upload/", files=files, headers=mod_auth_headers)
            assert response.status_code == status.HTTP_201_CREATED
            convert_pdf_to_tei()
            all_ref_files = db.query(ReferencefileModel).filter(ReferencefileModel.file_class == "tei").all()
            assert len(all_ref_files) == 1
            file_response = client.get(url=f"/reference/referencefile/download_file/{all_ref_files[0].referencefile_id}",
                                       headers=auth_headers)
            assert file_response.content
            response = client.get(url=f"/workflow_tag/get_current_workflow_status/{test_reference.new_ref_curie}/"
                                      f"{mod_abbreviation}/ATP:0000161", headers=auth_headers)
            assert response.json() == "ATP:0000163"
