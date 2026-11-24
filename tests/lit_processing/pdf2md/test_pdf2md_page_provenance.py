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


def _sha256(value):
    return hashlib.sha256(value).hexdigest()


def _canonical_json(value):
    """PDFX's canonical JSON (app/services/page_provenance.py)."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _range(byte_start, byte_end, page_number, method):
    return {
        "byte_start": byte_start,
        "byte_end": byte_end,
        "page_number": page_number,
        "candidate_pages": [page_number],
        "method": method,
        "source": "grobid",
        "operation": None,
        "evidence_digest": _sha256(f"evidence {byte_start}".encode()),
        "range_id": _sha256(f"range {byte_start}".encode()),
    }


def _page_map(merged=MERGED, pdf=PDF, **overrides):
    """A page map shaped like PDFX's merged-page-provenance-v1 file: every field
    of the contract, ranges that partition the Markdown, and a record_sha256 over
    the rest. ABC checks only schema, pdf_sha256, merged_markdown_sha256 and
    merged_markdown_size_bytes; the other fields are here to document the file
    and to show they are stored unchanged."""
    first_page_end = len(b"# Title\n\nFirst page text.\n\n")
    core = {
        "schema": "pdfx-merged-page-provenance",
        "contract_version": "merged-page-provenance-v1",
        "pdf_sha256": _sha256(pdf),
        "merged_markdown_sha256": _sha256(merged),
        "merged_markdown_size_bytes": len(merged),
        "audit_sha256": _sha256(b"audit"),
        "merge_contract_id": "merge-contract-v1",
        "source_map_sha256": {"grobid": _sha256(b"grobid source map")},
        "expected_page_count": 2,
        "ranges": [_range(0, first_page_end, 1, "direct"),
                   _range(first_page_end, len(merged), 2, "llm_selected")],
        "summary": {"range_counts_by_method": {"direct": 1, "llm_selected": 1}},
        "llm_receipts": [],
    }
    core.update(overrides)
    payload = {**core, "record_sha256": _sha256(_canonical_json(core))}
    return _canonical_json(payload) + b"\n"


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

    def test_other_schema_is_refused(self):
        page_map = _page_map(schema="pdfx-source-page-provenance")
        assert "schema" in page_provenance_mismatch(page_map, MERGED, PDF)
        no_schema = json.loads(_page_map())
        del no_schema["schema"]
        assert "schema" in page_provenance_mismatch(json.dumps(no_schema).encode(), MERGED, PDF)

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


def _convert(db, outputs, file_class="main", stored_files=None):  # noqa
    """Run one PDF through _process_single_pdf_file with PDFX returning outputs.

    stored_files: referencefile_id -> content download_file returns for files already stored; any other id is
    the source PDF."""
    pdf_file = SimpleNamespace(display_name="paper", file_class=file_class, referencefile_mods=[],
                               referencefile_id=-1)
    stored_files = stored_files or {}
    patches = [
        patch(f"{UTILS}.download_file",
              side_effect=lambda **kwargs: stored_files.get(kwargs["referencefile_id"], PDF)),
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
    pdf2md_utils.page_provenance_counts.clear()
    with patch(f"{UTILS}.has_page_provenance", wraps=pdf2md_utils.has_page_provenance) as has_map:
        success, methods, error = _convert(db, {"merged": MERGED, "grobid": PARSER,
                                                "page_provenance": _page_map()})
    assert (success, error) == (True, None)
    # the stored map is looked up once per conversion, not again by store_page_provenance
    assert has_map.call_count == 1
    assert pdf2md_utils.page_provenance_counts == {"stored": 1}
    assert _files(db, paper) == [
        ("paper_grobid", "converted_grobid_main", "md"),
        ("paper_merged", "converted_merged_main", "md"),
        ("paper_merged", "converted_page_provenance_main", "json"),
    ]
    stored = db.query(ReferencefileModel).filter_by(file_class="converted_page_provenance_main").one()
    # stored unchanged
    assert stored.md5sum == hashlib.md5(_page_map()).hexdigest()
    assert json.loads(_page_map())["ranges"][1]["method"] == "llm_selected"


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
    pdf2md_utils.page_provenance_counts.clear()
    # outputs has no page_provenance, so its download raises KeyError
    with patch(f"{UTILS}.logger") as log:
        success, methods, error = _convert(db, {"merged": MERGED, "grobid": PARSER})
    assert (success, error) == (True, None)
    assert pdf2md_utils.page_provenance_counts == {"missed": 1}
    # the PDFX job is named, so the map can be fetched again later
    errors = [str(c.args[0]) for c in log.error.call_args_list]
    assert any("Failed to store PDFX page map" in e and "process_id=pid" in e for e in errors)
    assert ("paper_merged", "converted_merged_main", "md") in _files(db, paper)
    assert [f for f in _files(db, paper) if f[2] == "json"] == []


def test_no_page_map_without_merged_markdown(db, paper):  # noqa
    with patch(f"{UTILS}.store_page_provenance") as store:
        _convert(db, {"merged": b"", "grobid": PARSER, "page_provenance": _page_map()})
    store.assert_not_called()


def test_an_existing_page_map_is_kept(db, paper):  # noqa
    _convert(db, {"merged": MERGED, "grobid": PARSER, "page_provenance": _page_map()})
    merged_file = db.query(ReferencefileModel).filter_by(file_class="converted_merged_main").one()
    page_map_file = db.query(ReferencefileModel).filter_by(file_class="converted_page_provenance_main").one()
    patches = _s3_patches() + [patch(f"{UTILS}.download_file", return_value=_page_map())]
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
    assert [f.referencefile_id for f in
            db.query(ReferencefileModel).filter_by(file_class="converted_page_provenance_main")] == \
        [page_map_file.referencefile_id]


def test_a_stale_page_map_of_the_same_name_is_replaced(db, paper):  # noqa
    """The merged Markdown is deleted without its page map and the PDF converted again: the new Markdown gets the
    same display_name, and the old map must not stay paired with it."""
    _convert(db, {"merged": MERGED, "grobid": PARSER, "page_provenance": _page_map()})
    old_map = db.query(ReferencefileModel).filter_by(file_class="converted_page_provenance_main").one()
    old_map_id = old_map.referencefile_id
    for row in db.query(ReferencefileModel).filter(ReferencefileModel.reference_id == paper.reference_id,
                                                   ReferencefileModel.file_extension == "md"):
        db.delete(row)
    db.commit()

    new_merged = b"# Title\n\nThe converted text changed.\n"
    with patch("agr_literature_service.api.crud.referencefile_utils.remove_file_from_s3"):
        success, _, error = _convert(db, {"merged": new_merged, "grobid": PARSER,
                                          "page_provenance": _page_map(merged=new_merged)},
                                     stored_files={old_map_id: _page_map()})
    assert (success, error) == (True, None)
    merged_file = db.query(ReferencefileModel).filter_by(file_class="converted_merged_main").one()
    page_maps = db.query(ReferencefileModel).filter_by(file_class="converted_page_provenance_main").all()
    assert len(page_maps) == 1
    assert page_maps[0].referencefile_id != old_map_id
    assert page_maps[0].display_name == merged_file.display_name == "paper_merged"
    assert page_maps[0].md5sum == hashlib.md5(_page_map(merged=new_merged)).hexdigest()


def test_deleting_the_merged_markdown_deletes_its_page_map(db, paper):  # noqa
    from agr_cognito_py import ModAccess
    from agr_literature_service.api.crud import referencefile_crud
    _convert(db, {"merged": MERGED, "grobid": PARSER, "page_provenance": _page_map()})
    merged_file = db.query(ReferencefileModel).filter_by(file_class="converted_merged_main").one()
    with patch("agr_literature_service.api.crud.referencefile_utils.remove_file_from_s3"):
        referencefile_crud.destroy(db, merged_file.referencefile_id, ModAccess.ALL_ACCESS)
    remaining = _files(db, paper)
    assert ("paper_merged", "converted_page_provenance_main", "json") not in remaining
    assert ("paper_grobid", "converted_grobid_main", "md") in remaining


def test_deleting_the_reference_deletes_the_markdown_and_its_page_map(db, paper):  # noqa
    """Deleting a merged Markdown deletes its page map too, so deleting the whole
    reference must not stop with a 404 when it reaches the page map."""
    from agr_literature_service.api.crud import reference_crud
    _convert(db, {"merged": MERGED, "grobid": PARSER, "page_provenance": _page_map()})
    reference_id = paper.reference_id
    assert len(_files(db, paper)) == 3
    with patch("agr_literature_service.api.crud.referencefile_utils.remove_file_from_s3"):
        reference_crud.destroy(db, CURIE)
    assert db.query(ReferenceModel).filter_by(reference_id=reference_id).one_or_none() is None
    assert db.query(ReferencefileModel).filter_by(reference_id=reference_id).count() == 0


def test_deleting_another_file_keeps_the_page_map(db, paper):  # noqa
    from agr_cognito_py import ModAccess
    from agr_literature_service.api.crud import referencefile_crud
    _convert(db, {"merged": MERGED, "grobid": PARSER, "page_provenance": _page_map()})
    parser_file = db.query(ReferencefileModel).filter_by(file_class="converted_grobid_main").one()
    with patch("agr_literature_service.api.crud.referencefile_utils.remove_file_from_s3"):
        referencefile_crud.destroy(db, parser_file.referencefile_id, ModAccess.ALL_ACCESS)
    assert ("paper_merged", "converted_page_provenance_main", "json") in _files(db, paper)


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
