"""
Simplified tests for bulk upload utility functions.
"""
import io
import os
import tarfile
import zipfile
from unittest.mock import Mock, patch

import pytest

from agr_literature_service.api.utils.bulk_upload_utils import (
    parse_filename_by_mod,
    parse_supplement_file,
    classify_and_parse_file,
    extract_and_classify_files,
    validate_archive_structure,
    process_single_file
)


# === parse_filename_by_mod ===
@pytest.mark.parametrize(
    "filename,mod,exp_curie,exp_author,exp_status,exp_type",
    [
        ("12345_Author2023.pdf", "WB", "WB:WBPaper12345", "Author2023", "final", None),
        ("12345_Author2023_temp.pdf", "WB", "WB:WBPaper12345", "Author2023", "temp", None),
        ("12345678_Author2023.html", "FB", "PMID:12345678", "Author2023", "final", None),
        ("123456789012345_Author2023.pdf", "MGI", "AGRKB:123456789012345", "Author2023", "final", None),
    ]
)
def test_parse_filename_patterns(filename, mod, exp_curie, exp_author, exp_status, exp_type):
    res = parse_filename_by_mod(filename, mod)
    assert res["reference_curie"] == exp_curie
    assert res["author_and_year"] == exp_author
    assert res["file_publication_status"] == exp_status
    assert res.get("pdf_type") == exp_type


def test_parse_filename_invalid_raises():
    with pytest.raises(ValueError):
        parse_filename_by_mod("no_numbers_here.pdf", "WB")


# === parse_supplement_file ===
def test_parse_supplement_file():
    res = parse_supplement_file("supp.txt", "12345", "FB")
    assert res["reference_curie"] == "PMID:12345"
    assert res["display_name"] == "supp"


# === classify_and_parse_file ===
def test_classify_and_parse_file_main(tmp_path):
    root = tmp_path
    file = root / "12345_A.pdf"
    file.write_text("")
    res = classify_and_parse_file(str(file), str(root), "WB")
    assert res["file_class"] == "main"
    assert res["reference_curie"] == "WB:WBPaper12345"


def test_classify_and_parse_file_supp(tmp_path):
    root = tmp_path
    sub = root / "12345"
    sub.mkdir()
    file = sub / "s.txt"
    file.write_text("")
    res = classify_and_parse_file(str(file), str(root), "FB")
    assert res["file_class"] == "supplement"
    assert res["reference_curie"] == "PMID:12345"


# === extract_and_classify_files & validate_archive_structure ===
def make_tar_archive(files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, content in files:
            info = tarfile.TarInfo(name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    buf.seek(0)
    return buf


def make_zip_archive(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        for name, content in files:
            zf.writestr(name, content)
    buf.seek(0)
    return buf


@pytest.mark.parametrize("maker", [make_tar_archive, make_zip_archive])
def test_extract_and_validate(maker, tmp_path):
    files = [("123_A.pdf", b"a"), ("123/x.txt", b"b")]
    archive = maker(files)
    # validate
    info = validate_archive_structure(archive)
    assert info['valid'] is True
    assert info['total_files'] == 2
    assert info['main_files'] == 1
    assert info['supplement_files'] == 1
    # extract
    archive.seek(0)
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    extracted = extract_and_classify_files(archive, str(out_dir))
    assert len(extracted) == 2
    for path, is_main in extracted:
        assert os.path.exists(path)
        assert isinstance(is_main, bool)


def test_validate_invalid_archive():
    bad = io.BytesIO(b"not an archive")
    res = validate_archive_structure(bad)
    assert res['valid'] is False


# === process_single_file ===
def test_process_single_file_success(tmp_path):
    temp = tmp_path / "f.txt"
    temp.write_bytes(b"data")
    metadata = {
        'reference_curie': 'WB:WBPaper1',
        'display_name': 'f',
        'file_class': 'main',
        'file_extension': 'txt',
        'file_publication_status': 'final',
        'pdf_type': None,
        'mod_abbreviation': 'WB'
    }
    mock_db = Mock()
    with patch('agr_literature_service.api.crud.referencefile_crud.file_upload') as fu:
        fu.return_value = None
        res = process_single_file(str(temp), metadata, mock_db)
    assert res['status'] == 'success'


def test_process_single_file_error(tmp_path):
    temp = tmp_path / "f.txt"
    temp.write_bytes(b"data")
    metadata = {'reference_curie': 'FB:1', 'display_name': 'f', 'file_class': 'main'}
    mock_db = Mock()
    with patch('agr_literature_service.api.crud.referencefile_crud.file_upload', side_effect=Exception("fail")):
        res = process_single_file(str(temp), metadata, mock_db)
    assert res['status'] == 'error'

    nofile = str(tmp_path / "nofile.txt")
    res2 = process_single_file(nofile, metadata, mock_db)
    assert res2['status'] == 'error'
