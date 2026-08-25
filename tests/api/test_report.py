"""Unit tests for report_crud.

These exercise the report-file listing and reader directly against a temporary
LOG_PATH, so they need no database or running API.
"""
import os

import pytest
from fastapi import HTTPException

from agr_literature_service.api.crud import report_crud


@pytest.fixture
def log_tree(tmp_path, monkeypatch):
    (tmp_path / "QC").mkdir()
    (tmp_path / "pubmed_update").mkdir()
    (tmp_path / "pdf2md.log").write_text("root log\n")
    (tmp_path / "QC" / "duplicate_orcid_report.log").write_text("orcid\n")
    (tmp_path / "pubmed_update" / "update_pubmed_papers_FB_20260530.log").write_text("fb\n")
    monkeypatch.setenv("LOG_PATH", str(tmp_path))
    monkeypatch.setenv("LOG_URL", "https://dev.alliancegenome.org/reports/")
    return tmp_path


class TestListReportFiles:

    def test_lists_every_file_recursively(self, log_tree):
        paths = {entry["path"] for entry in report_crud.list_report_files()}
        assert paths == {
            "pdf2md.log",
            "QC/duplicate_orcid_report.log",
            "pubmed_update/update_pubmed_papers_FB_20260530.log",
        }

    def test_reports_the_containing_directory(self, log_tree):
        by_path = {e["path"]: e for e in report_crud.list_report_files()}
        assert by_path["pdf2md.log"]["directory"] == ""
        assert by_path["QC/duplicate_orcid_report.log"]["directory"] == "QC"

    def test_size_is_numeric_bytes(self, log_tree):
        entry = next(e for e in report_crud.list_report_files() if e["path"] == "pdf2md.log")
        assert entry["size"] == len("root log\n")
        assert isinstance(entry["size"], int)

    def test_modified_is_an_iso_utc_timestamp(self, log_tree):
        entry = next(e for e in report_crud.list_report_files() if e["path"] == "pdf2md.log")
        assert entry["modified"].endswith("Z")
        assert entry["modified"][4] == "-" and entry["modified"][10] == "T"

    def test_builds_a_single_slash_url_from_log_url(self, log_tree):
        by_path = {e["path"]: e for e in report_crud.list_report_files()}
        assert by_path["QC/duplicate_orcid_report.log"]["url"] == (
            "https://dev.alliancegenome.org/reports/QC/duplicate_orcid_report.log"
        )

    def test_carries_the_file_name(self, log_tree):
        by_path = {e["path"]: e for e in report_crud.list_report_files()}
        assert by_path["QC/duplicate_orcid_report.log"]["name"] == "duplicate_orcid_report.log"

    def test_returns_nothing_when_log_path_is_unset(self, monkeypatch):
        monkeypatch.delenv("LOG_PATH", raising=False)
        assert report_crud.list_report_files() == []

    def test_returns_nothing_when_log_path_does_not_exist(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOG_PATH", str(tmp_path / "absent"))
        assert report_crud.list_report_files() == []


class TestGetReportFile:

    def test_returns_the_file_contents(self, log_tree):
        result = report_crud.get_report_file("QC/duplicate_orcid_report.log")
        assert result["content"] == "orcid\n"
        assert result["path"] == "QC/duplicate_orcid_report.log"
        assert result["truncated"] is False

    def test_reports_the_full_size_alongside_the_content(self, log_tree):
        result = report_crud.get_report_file("pdf2md.log")
        assert result["size"] == len("root log\n")

    def test_tail_returns_only_the_last_bytes_and_says_so(self, log_tree):
        (log_tree / "big.log").write_text("aaaaaaaaaa" + "bbbb")
        result = report_crud.get_report_file("big.log", tail=4)
        assert result["content"] == "bbbb"
        assert result["truncated"] is True
        assert result["size"] == 14

    def test_tail_larger_than_the_file_returns_everything_untruncated(self, log_tree):
        result = report_crud.get_report_file("pdf2md.log", tail=10000)
        assert result["content"] == "root log\n"
        assert result["truncated"] is False

    def test_a_file_over_the_cap_is_truncated_without_being_asked(self, log_tree, monkeypatch):
        """The bound holds server-side: no tail argument still caps the read."""
        monkeypatch.setattr(report_crud, "MAX_BYTES", 8)
        (log_tree / "huge.log").write_text("a" * 20 + "tailtail")
        result = report_crud.get_report_file("huge.log")
        assert result["content"] == "tailtail"
        assert result["truncated"] is True
        assert result["size"] == 28

    def test_a_tail_over_the_cap_is_clamped_to_it(self, log_tree, monkeypatch):
        monkeypatch.setattr(report_crud, "MAX_BYTES", 8)
        (log_tree / "huge.log").write_text("a" * 20 + "tailtail")
        result = report_crud.get_report_file("huge.log", tail=10000)
        assert result["content"] == "tailtail"
        assert result["truncated"] is True

    def test_a_file_under_the_cap_is_returned_whole(self, log_tree, monkeypatch):
        monkeypatch.setattr(report_crud, "MAX_BYTES", 8)
        result = report_crud.get_report_file("QC/duplicate_orcid_report.log")
        assert result["content"] == "orcid\n"
        assert result["truncated"] is False

    @pytest.mark.parametrize("bad_path", [
        "../outside.log",
        "QC/../../outside.log",
        "QC/../../../etc/passwd",
        "/etc/passwd",
        "",
        None,
        # realpath raises ValueError on this one, not OSError; without the
        # explicit catch it escapes as a 500 rather than the 400 the rest get.
        "\x00x",
    ])
    def test_rejects_a_path_that_escapes_the_log_root(self, log_tree, bad_path):
        (log_tree.parent / "outside.log").write_text("secret\n")
        with pytest.raises(HTTPException) as caught:
            report_crud.get_report_file(bad_path)
        assert caught.value.status_code == 400

    def test_rejects_a_symlink_pointing_outside_the_log_root(self, log_tree):
        (log_tree.parent / "outside.log").write_text("secret\n")
        (log_tree / "escape.log").symlink_to(log_tree.parent / "outside.log")
        with pytest.raises(HTTPException) as caught:
            report_crud.get_report_file("escape.log")
        assert caught.value.status_code == 400

    def test_raises_not_found_for_a_missing_file(self, log_tree):
        with pytest.raises(HTTPException) as caught:
            report_crud.get_report_file("QC/nope.log")
        assert caught.value.status_code == 404

    def test_raises_not_found_for_a_directory(self, log_tree):
        with pytest.raises(HTTPException) as caught:
            report_crud.get_report_file("QC")
        assert caught.value.status_code == 404

    def test_raises_when_log_path_is_unconfigured(self, monkeypatch):
        monkeypatch.delenv("LOG_PATH", raising=False)
        with pytest.raises(HTTPException) as caught:
            report_crud.get_report_file("pdf2md.log")
        assert caught.value.status_code == 404


# The API config demands a set of environment variables before main.py can be
# imported. CI supplies them via .env.test/docker-compose, so these defaults only
# fill gaps for a local run, and monkeypatch keeps them out of other modules.
API_ENV_DEFAULTS = {
    "ENV_STATE": "test", "PSQL_USERNAME": "u", "PSQL_PASSWORD": "p",
    "PSQL_HOST": "localhost", "PSQL_PORT": "5432", "PSQL_DATABASE": "d",
    "ELASTICSEARCH_HOST": "localhost", "ELASTICSEARCH_PORT": "9200",
    "ELASTICSEARCH_INDEX": "i",
}


@pytest.fixture
def client(monkeypatch, tmp_path_factory):
    """TestClient with authentication stubbed out.

    These endpoints only require that the caller is signed in, so overriding the
    dependency keeps the test hermetic instead of reaching out to Cognito.
    """
    for key, value in API_ENV_DEFAULTS.items():
        monkeypatch.setenv(key, os.environ.get(key, value))
    monkeypatch.setenv("XML_PATH", os.environ.get(
        "XML_PATH", str(tmp_path_factory.mktemp("xml"))))

    # Imported here, not at module scope, so the config above is in place first.
    from starlette.testclient import TestClient
    from agr_literature_service.api.auth import get_authenticated_user
    from agr_literature_service.api.main import app

    app.dependency_overrides[get_authenticated_user] = lambda: {"sub": "test-user"}
    yield TestClient(app)
    app.dependency_overrides.pop(get_authenticated_user, None)


class TestReportRouter:

    def test_lists_the_report_files(self, client, log_tree):
        response = client.get("/report/files")
        assert response.status_code == 200
        paths = {entry["path"] for entry in response.json()}
        assert "QC/duplicate_orcid_report.log" in paths

    def test_returns_one_report_file(self, client, log_tree):
        response = client.get("/report/file", params={"path": "QC/duplicate_orcid_report.log"})
        assert response.status_code == 200
        assert response.json()["content"] == "orcid\n"

    def test_tails_a_report_file(self, client, log_tree):
        (log_tree / "big.log").write_text("aaaaaaaaaabbbb")
        response = client.get("/report/file", params={"path": "big.log", "tail": 4})
        assert response.status_code == 200
        assert response.json() == {
            "path": "big.log", "name": "big.log", "size": 14,
            "truncated": True, "content": "bbbb",
        }

    def test_refuses_a_traversing_path(self, client, log_tree):
        response = client.get("/report/file", params={"path": "../../etc/passwd"})
        assert response.status_code == 400

    def test_reports_a_missing_file_as_not_found(self, client, log_tree):
        response = client.get("/report/file", params={"path": "QC/nope.log"})
        assert response.status_code == 404
