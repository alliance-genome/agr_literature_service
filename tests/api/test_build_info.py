"""Tests for reporting the running commit (SCRUM-6878)."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import yaml
from fastapi.testclient import TestClient

from agr_literature_service.api.build_info import get_build_info
from agr_literature_service.api.main import app

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_build_info_reads_the_environment(monkeypatch):
    monkeypatch.setenv("GIT_SHA", "82d05073")
    monkeypatch.setenv("BUILD_DATE", "2026-11-17T14:56:20Z")
    assert get_build_info() == {"commit": "82d05073", "build_date": "2026-11-17T14:56:20Z"}


def test_build_info_is_unknown_when_not_set_or_blank(monkeypatch):
    monkeypatch.delenv("GIT_SHA", raising=False)
    monkeypatch.setenv("BUILD_DATE", "")
    assert get_build_info() == {"commit": "unknown", "build_date": "unknown"}


def test_version_endpoint_needs_no_login(monkeypatch):
    monkeypatch.setenv("GIT_SHA", "82d05073")
    monkeypatch.setenv("BUILD_DATE", "2026-11-17T14:56:20Z")
    response = TestClient(app).get("/version")
    assert response.status_code == 200
    assert response.json() == {"commit": "82d05073", "build_date": "2026-11-17T14:56:20Z"}


def test_openapi_version_carries_the_commit():
    assert app.openapi()["info"]["version"].startswith("0.1.0+")


def test_gunicorn_ready_line_names_the_commit(monkeypatch, capsys):
    monkeypatch.setenv("GIT_SHA", "82d05073")
    monkeypatch.setenv("BUILD_DATE", "2026-11-17T14:56:20Z")
    spec = importlib.util.spec_from_file_location("gunicorn_conf", REPO_ROOT / "gunicorn.conf.py")
    gunicorn_conf = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gunicorn_conf)
    server = SimpleNamespace(cfg=SimpleNamespace(workers=4), log=MagicMock())
    gunicorn_conf.when_ready(server)
    assert "commit 82d05073, built 2026-11-17T14:56:20Z" in capsys.readouterr().out


def test_images_are_built_with_the_commit():
    services = yaml.safe_load((REPO_ROOT / "docker-compose.yaml").read_text())["services"]
    for service, dockerfile in (("api", "api.dockerfile"), ("automated_scripts", "automated_scripts.dockerfile")):
        args = services[service]["build"]["args"]
        assert args["GIT_SHA"] == "${GIT_SHA:-unknown}"
        assert args["BUILD_DATE"] == "${BUILD_DATE:-unknown}"
        text = (REPO_ROOT / "docker" / dockerfile).read_text()
        assert "ENV GIT_SHA=${GIT_SHA} BUILD_DATE=${BUILD_DATE}" in text
    assert "export GIT_SHA BUILD_DATE" in (REPO_ROOT / "Makefile").read_text()
