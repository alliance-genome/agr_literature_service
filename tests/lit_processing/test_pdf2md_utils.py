from types import SimpleNamespace

from agr_literature_service.lit_processing.pdf2md import pdf2md_utils


def _pdf_bytes(page_count: int) -> bytes:
    pages = b"\n".join(
        f"{i} 0 obj <</Type /Page>> endobj".encode("ascii")
        for i in range(1, page_count + 1)
    )
    return b"%PDF-1.4\n" + pages + b"\n%%EOF"


def _referencefile(file_class: str = "supplement"):
    return SimpleNamespace(
        referencefile_id=123,
        display_name="supplement_pdf",
        file_class=file_class,
        referencefile_mods=[],
    )


def test_supplement_pdf_over_size_limit_is_not_submitted(monkeypatch):
    submitted = False

    def fake_submit_pdf_to_pdfx(**kwargs):
        nonlocal submitted
        submitted = True
        return "process-id"

    monkeypatch.setattr(pdf2md_utils, "download_file", lambda **kwargs: b"x" * (5 * 1024 * 1024 + 1))
    monkeypatch.setattr(pdf2md_utils, "submit_pdf_to_pdfx", fake_submit_pdf_to_pdfx)

    success, methods_uploaded, error = pdf2md_utils._process_single_pdf_file(
        db=None,
        pdf_file=_referencefile(),
        reference_curie="AGRKB:1",
        token="token",
        methods_to_extract=["merged"],
    )

    assert success is False
    assert methods_uploaded == []
    assert "exceeds 5 MB limit" in error
    assert submitted is False


def test_supplement_pdf_over_page_limit_is_not_submitted(monkeypatch):
    submitted = False

    def fake_submit_pdf_to_pdfx(**kwargs):
        nonlocal submitted
        submitted = True
        return "process-id"

    monkeypatch.setattr(pdf2md_utils, "download_file", lambda **kwargs: _pdf_bytes(21))
    monkeypatch.setattr(pdf2md_utils, "submit_pdf_to_pdfx", fake_submit_pdf_to_pdfx)

    success, methods_uploaded, error = pdf2md_utils._process_single_pdf_file(
        db=None,
        pdf_file=_referencefile(),
        reference_curie="AGRKB:1",
        token="token",
        methods_to_extract=["merged"],
    )

    assert success is False
    assert methods_uploaded == []
    assert "exceeds 20 page limit" in error
    assert submitted is False


def test_supplement_pdf_within_limits_is_submitted(monkeypatch):
    submitted = False
    uploaded = False

    def fake_submit_pdf_to_pdfx(**kwargs):
        nonlocal submitted
        submitted = True
        return "process-id"

    def fake_file_upload(**kwargs):
        nonlocal uploaded
        uploaded = True

    monkeypatch.setattr(pdf2md_utils, "download_file", lambda **kwargs: _pdf_bytes(20))
    monkeypatch.setattr(pdf2md_utils, "submit_pdf_to_pdfx", fake_submit_pdf_to_pdfx)
    monkeypatch.setattr(pdf2md_utils, "poll_pdfx_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(pdf2md_utils, "download_pdfx_result", lambda *args, **kwargs: b"# Converted\n\ncontent")
    monkeypatch.setattr(pdf2md_utils, "file_upload", fake_file_upload)

    success, methods_uploaded, error = pdf2md_utils._process_single_pdf_file(
        db=None,
        pdf_file=_referencefile(),
        reference_curie="AGRKB:1",
        token="token",
        methods_to_extract=["merged"],
    )

    assert success is True
    assert methods_uploaded == ["merged"]
    assert error is None
    assert submitted is True
    assert uploaded is True


def test_main_pdf_is_not_subject_to_supplement_limits(monkeypatch):
    submitted = False

    def fake_submit_pdf_to_pdfx(**kwargs):
        nonlocal submitted
        submitted = True
        return "process-id"

    monkeypatch.setattr(pdf2md_utils, "download_file", lambda **kwargs: b"x" * (5 * 1024 * 1024 + 1))
    monkeypatch.setattr(pdf2md_utils, "submit_pdf_to_pdfx", fake_submit_pdf_to_pdfx)
    monkeypatch.setattr(pdf2md_utils, "poll_pdfx_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(pdf2md_utils, "download_pdfx_result", lambda *args, **kwargs: b"# Converted\n\ncontent")
    monkeypatch.setattr(pdf2md_utils, "file_upload", lambda **kwargs: None)

    success, methods_uploaded, error = pdf2md_utils._process_single_pdf_file(
        db=None,
        pdf_file=_referencefile(file_class="main"),
        reference_curie="AGRKB:1",
        token="token",
        methods_to_extract=["merged"],
    )

    assert success is True
    assert methods_uploaded == ["merged"]
    assert error is None
    assert submitted is True
