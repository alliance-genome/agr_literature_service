import io
import json
import os
from unittest.mock import patch, Mock, MagicMock

from sqlalchemy.orm import Session
from starlette import status
from starlette.testclient import TestClient

from agr_literature_service.api.main import app
from agr_literature_service.api.models import WorkflowTransitionModel, ReferencefileModel
from agr_literature_service.lit_processing.pdf2md.pdf2md import (
    get_pdfx_token,
    submit_pdf_to_pdfx,
    poll_pdfx_status,
    download_pdfx_result,
    process_single_pdf,
    process_newest_pdfs,
    main as convert_pdf_to_md,
    EXTRACTION_METHODS
)
from ...api.fixtures import auth_headers  # noqa
from ...api.test_mod import test_mod  # noqa
from ...api.test_reference import test_reference  # noqa
from ...fixtures import load_name_to_atp_and_relationships_mock
from ...fixtures import db  # noqa


sample_markdown_content = b"""# Sample Document

## Abstract

This is a sample markdown document converted from PDF.

## Introduction

Lorem ipsum dolor sit amet, consectetur adipiscing elit.

## Methods

The methods section describes the experimental procedures.

## Results

The results show significant findings.

## Conclusion

In conclusion, this study demonstrates important results.
"""

file_upload_process_atp_id = "ATP:0000140"


class TestPdfxToken:
    """Tests for PDFX token acquisition."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.requests.post")
    def test_get_pdfx_token_success(self, mock_post):
        """Test successful token acquisition."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "test_token_12345",
            "expires_in": 3600,
            "token_type": "Bearer"
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        with patch.dict(os.environ, {
            "PDFX_CLIENT_ID": "test_client_id",
            "PDFX_CLIENT_SECRET": "test_client_secret",
            "PDFX_TOKEN_URL": "https://auth.example.com/oauth2/token",
            "PDFX_SCOPE": "pdfx-api/extract"
        }):
            # Clear token cache
            from agr_literature_service.lit_processing.pdf2md import pdf2md
            pdf2md._token_cache = {"token": None, "expires_at": 0}

            token = get_pdfx_token()
            assert token == "test_token_12345"
            mock_post.assert_called_once()

    def test_get_pdfx_token_missing_credentials(self):
        """Test token acquisition with missing credentials."""
        with patch.dict(os.environ, {}, clear=True):
            # Clear any existing env vars
            os.environ.pop("PDFX_CLIENT_ID", None)
            os.environ.pop("PDFX_CLIENT_SECRET", None)

            from agr_literature_service.lit_processing.pdf2md import pdf2md
            pdf2md._token_cache = {"token": None, "expires_at": 0}

            try:
                get_pdfx_token()
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "PDFX_CLIENT_ID" in str(e)


class TestPdfxSubmission:
    """Tests for PDFX PDF submission."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.requests.post")
    def test_submit_pdf_success(self, mock_post):
        """Test successful PDF submission."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "process_id": "abc123-def456"
        }
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        with patch.dict(os.environ, {"PDFX_API_URL": "https://pdfx.example.com"}):
            process_id = submit_pdf_to_pdfx(
                file_content=b"fake pdf content",
                token="test_token",
                methods="grobid,docling,marker",
                merge=True,
                reference_curie="AGRKB:101000000000001"
            )
            assert process_id == "abc123-def456"

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.requests.post")
    def test_submit_pdf_no_process_id(self, mock_post):
        """Test submission response without process_id."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "something went wrong"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        with patch.dict(os.environ, {"PDFX_API_URL": "https://pdfx.example.com"}):
            try:
                submit_pdf_to_pdfx(
                    file_content=b"fake pdf content",
                    token="test_token"
                )
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "No process_id" in str(e)


class TestPdfxPolling:
    """Tests for PDFX status polling."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.requests.get")
    def test_poll_completed(self, mock_get):
        """Test polling for completed job."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "completed",
            "results": {"grobid": "ready", "docling": "ready", "marker": "ready"}
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with patch.dict(os.environ, {"PDFX_API_URL": "https://pdfx.example.com"}):
            result = poll_pdfx_status("abc123", "test_token", timeout=60)
            assert result["status"] == "completed"

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.requests.get")
    def test_poll_failed(self, mock_get):
        """Test polling for failed job."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "failed",
            "error": "PDF processing error"
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with patch.dict(os.environ, {"PDFX_API_URL": "https://pdfx.example.com"}):
            try:
                poll_pdfx_status("abc123", "test_token", timeout=60)
                assert False, "Should have raised RuntimeError"
            except RuntimeError as e:
                assert "failed" in str(e).lower()

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.time.sleep")
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.requests.get")
    def test_poll_timeout(self, mock_get, mock_sleep):
        """Test polling timeout."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "processing"}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with patch.dict(os.environ, {"PDFX_API_URL": "https://pdfx.example.com"}):
            try:
                poll_pdfx_status("abc123", "test_token", timeout=1, poll_interval=0.1)
                assert False, "Should have raised TimeoutError"
            except TimeoutError:
                pass


class TestPdfxDownload:
    """Tests for PDFX result download."""

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.requests.get")
    def test_download_success(self, mock_get):
        """Test successful markdown download."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = sample_markdown_content
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with patch.dict(os.environ, {"PDFX_API_URL": "https://pdfx.example.com"}):
            content = download_pdfx_result("abc123", "grobid", "test_token")
            assert content == sample_markdown_content


class TestExtractionMethods:
    """Tests for extraction method configuration."""

    def test_extraction_methods_defined(self):
        """Test that all extraction methods are properly defined."""
        assert "grobid" in EXTRACTION_METHODS
        assert "docling" in EXTRACTION_METHODS
        assert "marker" in EXTRACTION_METHODS
        assert "merged" in EXTRACTION_METHODS

        assert EXTRACTION_METHODS["grobid"] == "converted_grobid_main"
        assert EXTRACTION_METHODS["docling"] == "converted_docling_main"
        assert EXTRACTION_METHODS["marker"] == "converted_marker_main"
        assert EXTRACTION_METHODS["merged"] == "converted_merged_main"


def mock_get_jobs_to_run(name: str, mod_abbreviation: str, db: Session):  # noqa
    results = {'ATP:0000162': ['ATP:0000162']}
    return results[name]


def pdfx_token_mock():
    return "mock_token_12345"


def submit_pdf_mock(file_content, token, methods="grobid,docling,marker", merge=True,
                    reference_curie=None, mod_abbreviation=None):
    return "mock_process_id"


def poll_status_mock(process_id, token, timeout=900, poll_interval=10):
    return {"status": "completed"}


def download_result_mock(process_id, method, token):
    return sample_markdown_content


class TestPdf2MD:
    """Integration tests for PDF to Markdown conversion."""

    @staticmethod
    @patch("agr_literature_service.api.crud.workflow_transition_actions.proceed_on_value.get_jobs_to_run",
           mock_get_jobs_to_run)
    def upload_initial_main_reference_file(db, client, test_mod, test_reference, auth_headers):  # noqa
        mod_response = client.get(url=f"/mod/{test_mod.new_mod_abbreviation}", headers=auth_headers)
        mod_abbreviation = mod_response.json()["abbreviation"]
        new_mca = {
            "mod_abbreviation": mod_abbreviation,
            "reference_curie": test_reference.new_ref_curie,
            "corpus": True,
            "mod_corpus_sort_source": 'mod_pubmed_search'
        }
        client.post(url="/reference/mod_corpus_association/", json=new_mca, headers=auth_headers)

        new_cross_ref = {
            "curie": "0015_AtDB:123456",
            "reference_curie": test_reference.new_ref_curie,
            "pages": ["reference"]
        }
        client.post(url="/cross_reference/", json=new_cross_ref, headers=auth_headers)

        transitions_to_add = [
            ["ATP:0000141", "ATP:0000134",
             ["referencefiles_present"], ["proceed_on_value::category::thesis::ATP:0000162"], "on_success"],
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

        return mod_abbreviation

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.get_pdfx_token", pdfx_token_mock)
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.submit_pdf_to_pdfx", submit_pdf_mock)
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.poll_pdfx_status", poll_status_mock)
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.download_pdfx_result", download_result_mock)
    @patch("agr_literature_service.api.crud.workflow_transition_actions.proceed_on_value.get_jobs_to_run",
           mock_get_jobs_to_run)
    def test_pdf2md(self, db, auth_headers, test_reference, test_mod):  # noqa
        with TestClient(app) as client:
            load_name_to_atp_and_relationships_mock()
            mod_abbreviation = self.upload_initial_main_reference_file(
                db, client, test_mod, test_reference, auth_headers
            )
            convert_pdf_to_md()

            # Check that markdown files were created for each method
            for method, file_class in EXTRACTION_METHODS.items():
                all_ref_files = db.query(ReferencefileModel).filter(
                    ReferencefileModel.file_class == file_class
                ).all()
                assert len(all_ref_files) == 1, f"Expected 1 {method} markdown file, got {len(all_ref_files)}"

            # Check workflow status transitioned to success
            response = client.get(
                url=f"/workflow_tag/get_current_workflow_status/{test_reference.new_ref_curie}/"
                    f"{mod_abbreviation}/ATP:0000161",
                headers=auth_headers
            )
            assert response.json() == "ATP:0000163"

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.get_pdfx_token", pdfx_token_mock)
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.submit_pdf_to_pdfx")
    @patch("agr_literature_service.api.crud.workflow_transition_actions.proceed_on_value.get_jobs_to_run",
           mock_get_jobs_to_run)
    def test_pdf2md_failed_submission(self, mock_submit, db, auth_headers, test_reference, test_mod):  # noqa
        with TestClient(app) as client:
            load_name_to_atp_and_relationships_mock()
            mod_abbreviation = self.upload_initial_main_reference_file(
                db, client, test_mod, test_reference, auth_headers
            )

            # Make submission fail
            mock_submit.side_effect = Exception("PDFX service unavailable")

            convert_pdf_to_md()

            # Verify no markdown files were created
            for file_class in EXTRACTION_METHODS.values():
                all_ref_files = db.query(ReferencefileModel).filter(
                    ReferencefileModel.file_class == file_class
                ).all()
                assert len(all_ref_files) == 0

            # Check workflow status transitioned to failed
            response = client.get(
                url=f"/workflow_tag/get_current_workflow_status/{test_reference.new_ref_curie}/"
                    f"{mod_abbreviation}/ATP:0000161",
                headers=auth_headers
            )
            assert response.json() == "ATP:0000164"

    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.get_pdfx_token", pdfx_token_mock)
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.submit_pdf_to_pdfx", submit_pdf_mock)
    @patch("agr_literature_service.lit_processing.pdf2md.pdf2md.poll_pdfx_status")
    @patch("agr_literature_service.api.crud.workflow_transition_actions.proceed_on_value.get_jobs_to_run",
           mock_get_jobs_to_run)
    def test_pdf2md_polling_timeout(self, mock_poll, db, auth_headers, test_reference, test_mod):  # noqa
        with TestClient(app) as client:
            load_name_to_atp_and_relationships_mock()
            mod_abbreviation = self.upload_initial_main_reference_file(
                db, client, test_mod, test_reference, auth_headers
            )

            # Make polling timeout
            mock_poll.side_effect = TimeoutError("PDFX job timed out")

            convert_pdf_to_md()

            # Verify no markdown files were created
            for file_class in EXTRACTION_METHODS.values():
                all_ref_files = db.query(ReferencefileModel).filter(
                    ReferencefileModel.file_class == file_class
                ).all()
                assert len(all_ref_files) == 0

            # Check workflow status transitioned to failed
            response = client.get(
                url=f"/workflow_tag/get_current_workflow_status/{test_reference.new_ref_curie}/"
                    f"{mod_abbreviation}/ATP:0000161",
                headers=auth_headers
            )
            assert response.json() == "ATP:0000164"
