"""
Simplified tests for filename parsing in bulk_upload_utils.parse_filename_by_mod
"""
import pytest

from agr_literature_service.api.utils.bulk_upload_utils import parse_filename_by_mod

# Common test cases: (filename, mod, expected_curie_prefix, expected_author, expected_status, expected_pdf_type)
COMMON_CASES = [
    # WB basic
    ("12345_Author2023.pdf", "WB", "WB:WBPaper12345", "Author2023", "final", None),
    # FB basic
    ("67890_Author2023.pdf", "FB", "PMID:67890", "Author2023", "final", None),
    # Other MOD uses PMID fallback
    ("11111_Author2023.pdf", "SGD", "PMID:11111", "Author2023", "final", None),
    # AGRKB 15-digit ID across mods
    ("123456789012345_Author2023.pdf", "MGI", "AGRKB:123456789012345", "Author2023", "final", None),
]


@pytest.mark.parametrize(
    "filename,mod,exp_curie,exp_auth,exp_status,exp_type",
    COMMON_CASES
)
def test_parse_basic_patterns(filename, mod, exp_curie, exp_auth, exp_status, exp_type):
    res = parse_filename_by_mod(filename, mod)
    assert res["reference_curie"] == exp_curie
    assert res["author_and_year"] == exp_auth
    assert res["file_publication_status"] == exp_status
    assert res.get("pdf_type") == exp_type
    assert res["mod_abbreviation"] == mod


@pytest.mark.parametrize(
    "filename,exp_status,exp_type",
    [
        ("123_Author2023_temp.pdf", "temp", None),
        ("456_Author2023_aut.pdf", "final", "aut"),
        ("789_Author2023_HTML.htm", "final", "html"),
    ]
)
def test_parse_status_and_pdf_types(filename, exp_status, exp_type):
    res = parse_filename_by_mod(filename, "FB")
    assert res["file_publication_status"] == exp_status
    assert res.get("pdf_type") == exp_type


@pytest.mark.parametrize(
    "invalid",
    [
        "no_numbers.pdf",
        "123-invalid.pdf",
        "_startunderscore.pdf",
        "",
    ]
)
def test_invalid_patterns_raise(invalid):
    with pytest.raises(ValueError):
        parse_filename_by_mod(invalid, "WB")
