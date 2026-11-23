import pytest

from agr_literature_service.lit_processing.pdf2md import pdf2md_utils


@pytest.fixture(autouse=True)
def _stub_admin_token(monkeypatch):
    """The PDFX request helpers fetch the Cognito admin token themselves, for every
    request (SCRUM-6978). Never call Cognito from these tests."""
    monkeypatch.setattr(pdf2md_utils, "get_admin_token", lambda: "test_token")
