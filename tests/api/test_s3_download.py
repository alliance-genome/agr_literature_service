import pytest
from fastapi import HTTPException

from agr_literature_service.api.s3 import download


class TestGetJsonFile:
    """SCRUM-6796: a missing JSON dump must be a 404, not a 500 from FileResponse."""

    def test_missing_dump_is_a_404(self, monkeypatch):
        # download_file_from_s3 reports an S3 404 by returning False, not by raising
        monkeypatch.setattr(download, "download_file_from_s3", lambda *args: False)
        with pytest.raises(HTTPException) as ei:
            download.get_json_file("AGR")
        assert ei.value.status_code == 404
        assert "reference/dumps/latest/reference_AGR.json.gz" in ei.value.detail

    def test_download_error_is_a_500(self, monkeypatch):
        def boom(*args):
            raise RuntimeError("no credentials")
        monkeypatch.setattr(download, "download_file_from_s3", boom)
        with pytest.raises(HTTPException) as ei:
            download.get_json_file("AGR")
        assert ei.value.status_code == 500
