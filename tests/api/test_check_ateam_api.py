"""Unit tests for check_crud.check_ateam_api: no database, no network."""
from agr_literature_service.api.crud import check_crud

DOWN = {"status": "No species can be created until the ATeam API is back up"}


def test_unset_url_reports_down_instead_of_crashing(monkeypatch):
    monkeypatch.setattr(check_crud, "get_authentication_token", lambda: "token")
    monkeypatch.delenv("ATEAM_API_URL", raising=False)
    assert check_crud.check_ateam_api() == DOWN


def test_unreachable_api_reports_down(monkeypatch):
    def refuse(request):
        raise OSError("connection refused")

    monkeypatch.setattr(check_crud, "get_authentication_token", lambda: "token")
    monkeypatch.setenv("ATEAM_API_URL", "https://ateam.invalid/api")
    monkeypatch.setattr(check_crud.urllib.request, "urlopen", refuse)
    assert check_crud.check_ateam_api() == DOWN


def test_token_failure_reports_down(monkeypatch):
    def no_token():
        raise RuntimeError("cognito down")

    monkeypatch.setattr(check_crud, "get_authentication_token", no_token)
    monkeypatch.setenv("ATEAM_API_URL", "https://ateam.invalid/api")
    assert check_crud.check_ateam_api() == DOWN
