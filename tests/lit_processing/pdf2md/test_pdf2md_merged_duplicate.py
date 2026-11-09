"""pdf2md keeps the merged Markdown when it is identical to a parser's output (SCRUM-6824).

A reference holds each content (md5sum) only once: uploading the same bytes again only
links the existing row to the MOD. PDFX's merged Markdown is sometimes byte-for-byte one
parser's output, and merged used to be uploaded last, so it was the one dropped. The
paper then had no converted_merged_main file, which is all that embeddings and the
classifiers read, while pdf2md still reported the conversion as done.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agr_literature_service.api.models import ReferencefileModel, ReferenceModel
from agr_literature_service.lit_processing.pdf2md import pdf2md, pdf2md_utils
from agr_literature_service.lit_processing.pdf2md.pdf2md_utils import pdfx_upload_order
from ...fixtures import db  # noqa

UTILS = "agr_literature_service.lit_processing.pdf2md.pdf2md_utils"
CRUD = "agr_literature_service.api.crud.referencefile_crud"
CURIE = "AGRKB:101000500824"


@pytest.fixture
def paper(db):  # noqa
    reference = ReferenceModel(curie=CURIE, category="research_article")
    db.add(reference)
    db.commit()
    db.refresh(reference)
    return reference


def _add_file(db, reference, display_name, file_class, content):  # noqa
    db.add(ReferencefileModel(reference_id=reference.reference_id, display_name=display_name,
                              file_class=file_class, file_publication_status="final", file_extension="md",
                              pdf_type=None, md5sum=pdf2md_utils.hashlib.md5(content).hexdigest(),
                              is_annotation=False))
    db.commit()


def _convert(db, outputs):  # noqa
    """Run one main PDF through _process_single_pdf_file with PDFX returning outputs."""
    pdf_file = SimpleNamespace(display_name="paper", file_class="main", referencefile_mods=[],
                               referencefile_id=1)
    with patch(f"{UTILS}.download_file", return_value=b"%PDF-1.4"), \
            patch(f"{UTILS}.submit_pdf_to_pdfx", return_value="pid"), \
            patch(f"{UTILS}.poll_pdfx_status"), \
            patch(f"{UTILS}.download_pdfx_result", side_effect=lambda pid, method, token: outputs[method]), \
            patch(f"{UTILS}.process_extracted_images", return_value=(0, 0, [])), \
            patch(f"{CRUD}.boto3"), \
            patch(f"{CRUD}.upload_file_to_bucket"), \
            patch(f"{CRUD}.cleanup_old_pdf_file"), \
            patch(f"{CRUD}.transition_WFT_for_uploaded_file"):
        return pdf2md_utils._process_single_pdf_file(db, pdf_file, CURIE, "token", list(outputs))


def _files(db, reference):  # noqa
    return sorted((f.display_name, f.file_class) for f in
                  db.query(ReferencefileModel).filter(ReferencefileModel.reference_id == reference.reference_id))


def test_merged_is_uploaded_first():
    assert pdfx_upload_order(["grobid", "docling", "marker", "merged"]) == ["merged", "grobid", "docling", "marker"]
    assert pdfx_upload_order(["grobid", "docling"]) == ["grobid", "docling"]


def test_merged_identical_to_a_parser_output_is_kept(db, paper):  # noqa
    same = b"# Title\n\nThe merged text, identical to marker's."
    success, _methods, error = _convert(db, {"grobid": b"# grobid text, different", "docling": b"# docling text",
                                             "marker": same, "merged": same})

    assert success and error is None
    # marker's identical file is the one that only links to merged's row
    assert _files(db, paper) == [("paper_docling", "converted_docling_main"),
                                 ("paper_grobid", "converted_grobid_main"),
                                 ("paper_merged", "converted_merged_main")]


def test_rerun_turns_the_earlier_identical_parser_file_into_merged(db, paper):  # noqa
    # The affected papers: an earlier run stored marker's output, and its merged
    # upload, identical to it, was dropped. Uploading merged first is not enough
    # on a re-run, because the marker row already holds that content.
    same = b"# Title\n\nThe merged text, identical to marker's."
    _add_file(db, paper, "paper_grobid", "converted_grobid_main", b"# grobid text, different")
    _add_file(db, paper, "paper_marker", "converted_marker_main", same)

    success, _methods, error = _convert(db, {"merged": same, "marker": b"# marker text from the re-run"})

    assert success and error is None
    assert _files(db, paper) == [("paper_grobid", "converted_grobid_main"),
                                 ("paper_marker", "converted_marker_main"),
                                 ("paper_merged", "converted_merged_main")]


def test_merged_that_cannot_be_stored_fails_the_conversion(db, paper):  # noqa
    # The same content already on the paper as something other than a parser
    # output: it is not taken over, and the conversion must not count as done.
    same = b"# Text that is already a supplement's merged Markdown"
    _add_file(db, paper, "supp_merged", "converted_merged_supplement", same)

    success, methods, error = _convert(db, {"grobid": b"# grobid text", "merged": same})

    assert success is False
    assert error == "Merged markdown was not stored"
    assert methods == ["grobid"]
    assert ("supp_merged", "converted_merged_supplement") in _files(db, paper)
    assert not any(file_class == "converted_merged_main" for _name, file_class in _files(db, paper))


def test_main_conversion_without_merged_is_not_done():
    # pdf2md's own main-PDF path: the paper must not be marked "file converted to
    # text" when the merged file was not stored, or it is never retried
    def upload(db, reference_curie, display_name, method, *args):  # noqa
        if method == "merged":
            raise RuntimeError("merged markdown was not stored as converted_merged_main")

    main = "agr_literature_service.lit_processing.pdf2md.pdf2md"
    with patch(f"{main}.download_file", return_value=b"%PDF-1.4"), \
            patch(f"{main}.submit_pdf_to_pdfx", return_value="pid"), \
            patch(f"{main}.poll_pdfx_status"), \
            patch(f"{main}.download_pdfx_result", return_value=b"# converted markdown"), \
            patch(f"{main}.process_extracted_images", return_value=(0, 0, [])), \
            patch(f"{main}.upload_pdfx_markdown", side_effect=upload) as m_upload:
        success, error = pdf2md._convert_main_pdf_via_pdfx(
            MagicMock(), 1, CURIE, "paper", "FB", "token", ["grobid", "docling", "marker", "merged"])

    assert (success, error) == (False, "Merged markdown was not stored")
    assert [c.args[3] for c in m_upload.call_args_list] == ["merged", "grobid", "docling", "marker"]
