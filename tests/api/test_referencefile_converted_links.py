"""Linking converted Markdown rows to their source files by the display-name
suffix convention, including the Office suffixes (SCRUM-6589): a PDF, an
xlsx and a docx that share a display_name each link only to their own
Markdown, in both directions (source -> derived, derived -> source)."""
from unittest.mock import MagicMock

from agr_literature_service.api.crud.referencefile_crud import (
    _find_converted_derived_for_source,
    _find_source_for_derived,
)


def _rf(name, file_class, ext, rf_id, reference_id=1):
    rf = MagicMock()
    rf.display_name = name
    rf.file_class = file_class
    rf.file_extension = ext
    rf.referencefile_id = rf_id
    rf.reference_id = reference_id
    rf.md5sum = f"md5-{rf_id}"
    return rf


PDF = _rf("table_s1", "supplement", "pdf", 1)
XLSX = _rf("table_s1", "supplement", "xlsx", 2)
DOCX = _rf("table_s1", "supplement", "docx", 3)
PDF_MD = _rf("table_s1_merged", "converted_merged_supplement", "md", 11)
XLSX_MD = _rf("table_s1_xlsx", "converted_merged_supplement", "md", 12)
DOCX_MD = _rf("table_s1_docx", "converted_merged_supplement", "md", 13)
ALL = [PDF, XLSX, DOCX, PDF_MD, XLSX_MD, DOCX_MD]


def _db_returning(rows):
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = rows
    return db


def _ids(derived):
    return [d["referencefile_id"] for d in derived]


def test_each_source_links_only_its_own_markdown():
    db = _db_returning([PDF_MD, XLSX_MD, DOCX_MD])
    assert _ids(_find_converted_derived_for_source(db, PDF)) == [11]
    assert _ids(_find_converted_derived_for_source(db, XLSX)) == [12]
    assert _ids(_find_converted_derived_for_source(db, DOCX)) == [13]


def test_office_source_ignores_pdfx_and_tei_suffixes():
    tei = _rf("table_s1_tei", "converted_merged_supplement", "md", 14)
    db = _db_returning([PDF_MD, tei])
    assert _find_converted_derived_for_source(db, XLSX) == []
    assert _ids(_find_converted_derived_for_source(db, PDF)) == [11, 14]


def test_derived_resolves_back_to_the_source_with_matching_extension():
    assert _find_source_for_derived(XLSX_MD, ALL, {})["referencefile_id"] == 2
    assert _find_source_for_derived(DOCX_MD, ALL, {})["referencefile_id"] == 3
    assert _find_source_for_derived(PDF_MD, ALL, {})["referencefile_id"] == 1


def test_derived_office_markdown_without_its_source_is_unresolved():
    assert _find_source_for_derived(XLSX_MD, [PDF, DOCX], {}) is None
