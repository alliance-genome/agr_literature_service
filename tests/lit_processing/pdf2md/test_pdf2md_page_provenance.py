"""PDFX's page map is stored next to the merged Markdown it describes (SCRUM-6977).

PDFX builds a page map (``page_provenance``) for every merge job: byte ranges of
the merged Markdown and the PDF page each is on. It records the sha256 and size
of that Markdown and the sha256 of the PDF. ABC stores it unchanged, as JSON
with the merged Markdown's display_name, only when it describes the Markdown
and PDF of the same conversion.
"""
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agr_literature_service.api.models import ReferencefileModel, ReferenceModel
from agr_literature_service.lit_processing.pdf2md import pdf2md_utils
from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import (
    PAGE_PROVENANCE_FILE_CLASSES,
    page_provenance_mismatch,
    store_page_provenance,
    sync_converted_file_mods_to_sources,
)
from ...fixtures import db  # noqa

UTILS = "agr_literature_service.lit_processing.pdf2md.pdf2md_utils"
CRUD = "agr_literature_service.api.crud.referencefile_crud"
RESYNC = "agr_literature_service.api.crud.referencefile_mod_utils.resync_derived_embeddings"
CURIE = "AGRKB:101000500977"
PDF = b"%PDF-1.4 the paper"
MERGED = b"# Title\n\nFirst page text.\n\nSecond page text.\n"
PARSER = b"# Title\n\nparser output that differs\n"


def _page_map(merged=MERGED, pdf=PDF, **overrides):
    payload = {
        "schema": "pdfx-merged-page-provenance",
        "pdf_sha256": hashlib.sha256(pdf).hexdigest(),
        "merged_markdown_sha256": hashlib.sha256(merged).hexdigest(),
        "merged_markdown_size_bytes": len(merged),
        "ranges": [{"byte_start": 0, "byte_end": 27, "page": 1},
                   {"byte_start": 27, "byte_end": len(merged), "page": 2}],
    }
    payload.update(overrides)
    return json.dumps(payload, sort_keys=True).encode() + b"\n"


class TestPageProvenanceMismatch:

    def test_matching_map_is_accepted(self):
        assert page_provenance_mismatch(_page_map(), MERGED, PDF) is None

    def test_other_markdown_is_refused(self):
        assert "different merged Markdown" in page_provenance_mismatch(_page_map(), MERGED + b"x", PDF)

    def test_wrong_size_is_refused(self):
        page_map = _page_map(merged_markdown_size_bytes=len(MERGED) + 1)
        assert "different merged Markdown" in page_provenance_mismatch(page_map, MERGED, PDF)

    def test_other_pdf_is_refused(self):
        assert "different PDF" in page_provenance_mismatch(_page_map(), MERGED, b"%PDF other")

    def test_not_json_is_refused(self):
        assert "not JSON" in page_provenance_mismatch(b"<html>error</html>", MERGED, PDF)
        assert "not a JSON object" in page_provenance_mismatch(b"[1, 2]", MERGED, PDF)


@pytest.fixture
def paper(db):  # noqa
    reference = ReferenceModel(curie=CURIE, category="research_article")
    db.add(reference)
    db.commit()
    db.refresh(reference)
    return reference


def _s3_patches():
    return [patch(f"{CRUD}.boto3"), patch(f"{CRUD}.upload_file_to_bucket"),
            patch(f"{CRUD}.cleanup_old_pdf_file"), patch(f"{CRUD}.transition_WFT_for_uploaded_file")]


def _convert(db, outputs, file_class="main"):  # noqa
    """Run one PDF through _process_single_pdf_file with PDFX returning outputs."""
    pdf_file = SimpleNamespace(display_name="paper", file_class=file_class, referencefile_mods=[],
                               referencefile_id=1)
    patches = [
        patch(f"{UTILS}.download_file", return_value=PDF),
        patch(f"{UTILS}.submit_pdf_to_pdfx", return_value="pid"),
        patch(f"{UTILS}.poll_pdfx_status"),
        patch(f"{UTILS}.download_pdfx_result", side_effect=lambda pid, method: outputs[method]),
        patch(f"{UTILS}.process_extracted_images", return_value=(0, 0, [])),
        *_s3_patches(),
    ]
    for p in patches:
        p.start()
    try:
        return pdf2md_utils._process_single_pdf_file(
            db, pdf_file, CURIE, ["merged", "grobid"], enforce_supplement_limits=False)
    finally:
        for p in patches:
            p.stop()


def _files(db, reference):  # noqa
    return sorted((f.display_name, f.file_class, f.file_extension) for f in
                  db.query(ReferencefileModel).filter(ReferencefileModel.reference_id == reference.reference_id))


def test_conversion_stores_the_page_map_next_to_the_merged_markdown(db, paper):  # noqa
    success, methods, error = _convert(db, {"merged": MERGED, "grobid": PARSER,
                                            "page_provenance": _page_map()})
    assert (success, error) == (True, None)
    assert _files(db, paper) == [
        ("paper_grobid", "converted_grobid_main", "md"),
        ("paper_merged", "converted_merged_main", "md"),
        ("paper_merged", "converted_page_provenance_main", "json"),
    ]
    stored = db.query(ReferencefileModel).filter_by(file_class="converted_page_provenance_main").one()
    # stored unchanged
    assert stored.md5sum == hashlib.md5(_page_map()).hexdigest()


def test_supplement_page_map_uses_the_supplement_class(db, paper):  # noqa
    success, _, _ = _convert(db, {"merged": MERGED, "grobid": PARSER, "page_provenance": _page_map()},
                             file_class="supplement")
    assert success is True
    assert ("paper_merged", "converted_page_provenance_supplement", "json") in _files(db, paper)


def test_a_page_map_for_other_markdown_is_not_stored_and_the_conversion_succeeds(db, paper):  # noqa
    success, methods, error = _convert(db, {"merged": MERGED, "grobid": PARSER,
                                            "page_provenance": _page_map(merged=b"other text")})
    assert (success, error) == (True, None)
    assert [f for f in _files(db, paper) if f[2] == "json"] == []


def test_a_failed_page_map_download_does_not_fail_the_conversion(db, paper):  # noqa
    # outputs has no page_provenance, so its download raises KeyError
    success, methods, error = _convert(db, {"merged": MERGED, "grobid": PARSER})
    assert (success, error) == (True, None)
    assert ("paper_merged", "converted_merged_main", "md") in _files(db, paper)
    assert [f for f in _files(db, paper) if f[2] == "json"] == []


def test_no_page_map_without_merged_markdown(db, paper):  # noqa
    with patch(f"{UTILS}.store_page_provenance") as store:
        _convert(db, {"merged": b"", "grobid": PARSER, "page_provenance": _page_map()})
    store.assert_not_called()


def test_an_existing_page_map_is_kept(db, paper):  # noqa
    _convert(db, {"merged": MERGED, "grobid": PARSER, "page_provenance": _page_map()})
    merged_file = db.query(ReferencefileModel).filter_by(file_class="converted_merged_main").one()
    patches = _s3_patches()
    for p in patches:
        p.start()
    try:
        stored_again = store_page_provenance(
            db, _page_map(ranges=[]), reference_curie=CURIE, merged_file=merged_file,
            merged_markdown=MERGED, pdf_content=PDF, source_file_class="main", mod_abbreviation=None)
    finally:
        for p in patches:
            p.stop()
    assert stored_again is False
    assert len([f for f in _files(db, paper) if f[2] == "json"]) == 1


def test_page_map_follows_the_source_pdf_mods():
    source = MagicMock(file_class="main", display_name="paper", file_extension="pdf")
    source.referencefile_mods = [MagicMock(mod_id=1), MagicMock(mod_id=2)]
    page_map = MagicMock(file_class=PAGE_PROVENANCE_FILE_CLASSES["main"], display_name="paper_merged",
                         file_extension="json", referencefile_id=7)
    page_map.referencefile_mods = [MagicMock(mod_id=1)]
    session = MagicMock()
    with patch(RESYNC):
        changes = sync_converted_file_mods_to_sources(session, MagicMock(referencefiles=[source, page_map]))
    assert changes == 1
    assert [c.args[0].mod_id for c in session.add.call_args_list] == [2]


def test_other_json_files_are_not_synced():
    """Figure metadata JSON keeps its existing handling."""
    source = MagicMock(file_class="main", display_name="paper", file_extension="pdf")
    source.referencefile_mods = [MagicMock(mod_id=1)]
    other = MagicMock(file_class="converted_main_figure_metadata", display_name="paper_merged",
                      file_extension="json", referencefile_id=8)
    other.referencefile_mods = []
    session = MagicMock()
    with patch(RESYNC):
        assert sync_converted_file_mods_to_sources(session, MagicMock(referencefiles=[source, other])) == 0
