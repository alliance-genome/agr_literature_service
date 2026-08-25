"""Unit tests for the date-selectable QC report readers in check_crud.

These drive the crud functions directly against a temporary LOG_PATH, so they
need no database and no running API.
"""
import pytest
from fastapi import HTTPException, status

from agr_literature_service.api.crud import check_crud


def _write(directory, name, datestamp, rows):
    """Write one QC log: the '#!date-produced' header plus tab-delimited rows."""
    body = f"#!date-produced: {datestamp}\n" + "".join(f"{row}\n" for row in rows)
    (directory / name).write_text(body)


@pytest.fixture
def qc_tree(tmp_path, monkeypatch):
    qc = tmp_path / "QC"
    qc.mkdir()

    # Two archived runs of the obsolete-entity report, plus the stable "latest".
    for datestamp in ("20260607", "20260707"):
        _write(qc, f"obsolete_entity_report_{datestamp}.log", datestamp,
               ["WB\tgene\tObsolete\tWB:WBGene0001\tunc-1\t2\tAGRKB:101,AGRKB:102\tNCBITaxon:6239",
                "FB\tallele\tDeleted\tFB:FBal0001\t\t1\tAGRKB:103\tNCBITaxon:7227"])
    _write(qc, "obsolete_entity_report.log", "20260707",
           ["WB\tgene\tObsolete\tWB:WBGene0001\tunc-1\t2\tAGRKB:101,AGRKB:102\tNCBITaxon:6239",
            "FB\tallele\tDeleted\tFB:FBal0001\t\t1\tAGRKB:103\tNCBITaxon:7227"])

    _write(qc, "duplicate_orcid_report_20260618.log", "20260618",
           ["SGD\tAGRKB:201\t0000-0001-0000-0001\tA Curator"])
    _write(qc, "obsolete_pmid_report_20260618.log", "20260618", ["MGI\t12345678"])
    _write(qc, "redacted_references_with_tags_20260620.log", "20260620",
           ["AGRKB:301\tZFIN\tretracted"])

    # Neither a datestamp nor a run: must be ignored by the listing.
    _write(qc, "obsolete_entity_report_final.log", "20260707", [])

    monkeypatch.setenv("LOG_PATH", str(tmp_path))
    return tmp_path


@pytest.fixture
def current_run_is_undated(tmp_path, monkeypatch):
    """The real convention: the current run keeps the plain name.

    duplicate_orcid_report.log is the latest data, and only the superseded run
    carries a datestamp in its filename.
    """
    qc = tmp_path / "QC"
    qc.mkdir()
    _write(qc, "duplicate_orcid_report.log", "20260818",
           ["SGD\tAGRKB:999\t0000-0002-9999-9999\tCurrent Curator"])
    _write(qc, "duplicate_orcid_report_20250714.log", "20250714",
           ["SGD\tAGRKB:111\t0000-0001-1111-1111\tOld Curator"])
    monkeypatch.setenv("LOG_PATH", str(tmp_path))
    return tmp_path


class TestTheCurrentUndatedRun:
    """The newest run has no datestamp in its filename until it is superseded."""

    def test_the_undated_file_is_still_readable(self, current_run_is_undated):
        result = check_crud.check_duplicate_orcids()
        assert result["date-produced"] == "20260818"
        assert result["duplicate_orcids"]["SGD"][0]["author_names"] == "Current Curator"

    def test_the_current_run_is_listed_alongside_the_archive(self, current_run_is_undated):
        assert check_crud.list_qc_report_dates("duplicate_orcids") == ["20260818", "20250714"]

    def test_the_newest_listed_date_is_the_current_run(self, current_run_is_undated):
        # What a caller defaulting to the first listed date must get: the newest
        # data, not the stale archive.
        newest = check_crud.list_qc_report_dates("duplicate_orcids")[0]
        assert check_crud.check_duplicate_orcids(newest)["duplicate_orcids"]["SGD"][0][
            "author_names"] == "Current Curator"

    def test_the_archived_run_still_reads_from_its_dated_file(self, current_run_is_undated):
        result = check_crud.check_duplicate_orcids("20250714")
        assert result["date-produced"] == "20250714"
        assert result["duplicate_orcids"]["SGD"][0]["author_names"] == "Old Curator"

    def test_a_datestamp_the_undated_file_does_not_claim_is_not_found(self, current_run_is_undated):
        with pytest.raises(HTTPException) as excinfo:
            check_crud.check_duplicate_orcids("20260819")
        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND

    def test_an_undated_file_without_a_header_adds_no_date(self, tmp_path, monkeypatch):
        qc = tmp_path / "QC"
        qc.mkdir()
        (qc / "obsolete_pmid_report.log").write_text("MGI\t12345678\n")
        monkeypatch.setenv("LOG_PATH", str(tmp_path))
        assert check_crud.list_qc_report_dates("obsolete_pmids") == []
        # ...and it is still readable without a datestamp.
        assert check_crud.check_obsolete_pmids()["obsolete_pmids"] == {"MGI": ["12345678"]}

    def test_names_the_date_belonging_to_the_current_run(self, current_run_is_undated):
        assert check_crud.qc_latest_datestamp("duplicate_orcids") == "20260818"

    def test_no_current_date_when_the_undated_file_is_absent(self, qc_tree):
        # qc_tree archives duplicate_orcid runs but has no undated file.
        assert check_crud.qc_latest_datestamp("duplicate_orcids") is None

    def test_no_current_date_when_the_undated_file_has_no_header(self, tmp_path, monkeypatch):
        qc = tmp_path / "QC"
        qc.mkdir()
        (qc / "obsolete_pmid_report.log").write_text("MGI\t12345678\n")
        monkeypatch.setenv("LOG_PATH", str(tmp_path))
        assert check_crud.qc_latest_datestamp("obsolete_pmids") is None

    def test_an_undatable_current_run_still_counts_as_present(self, tmp_path, monkeypatch):
        # A hand-written log with no header cannot be dated, but it is still the
        # current run and must still be offered.
        qc = tmp_path / "QC"
        qc.mkdir()
        (qc / "obsolete_pmid_report.log").write_text("MGI\t12345678\n")
        monkeypatch.setenv("LOG_PATH", str(tmp_path))
        assert check_crud.qc_latest_exists("obsolete_pmids") is True
        assert check_crud.qc_latest_datestamp("obsolete_pmids") is None

    def test_current_run_is_present_when_it_has_a_header(self, current_run_is_undated):
        assert check_crud.qc_latest_exists("duplicate_orcids") is True

    def test_no_current_run_when_only_archives_exist(self, qc_tree):
        assert check_crud.qc_latest_exists("duplicate_orcids") is False

    def test_a_dated_copy_of_the_current_run_is_not_listed_twice(self, current_run_is_undated):
        _write(current_run_is_undated / "QC", "duplicate_orcid_report_20260818.log", "20260818",
               ["SGD\tAGRKB:999\t0000-0002-9999-9999\tCurrent Curator"])
        assert check_crud.list_qc_report_dates("duplicate_orcids") == ["20260818", "20250714"]


class TestListQcReportDates:

    def test_lists_archived_runs_newest_first(self, qc_tree):
        assert check_crud.list_qc_report_dates("obsolete_entities") == ["20260707", "20260607"]

    def test_ignores_suffixes_that_are_not_datestamps(self, qc_tree):
        assert "final" not in check_crud.list_qc_report_dates("obsolete_entities")

    def test_does_not_list_the_undatestamped_latest_file(self, qc_tree):
        assert all(date.isdigit() for date in check_crud.list_qc_report_dates("obsolete_entities"))

    def test_each_report_key_lists_only_its_own_runs(self, qc_tree):
        assert check_crud.list_qc_report_dates("duplicate_orcids") == ["20260618"]
        assert check_crud.list_qc_report_dates("obsolete_pmids") == ["20260618"]
        assert check_crud.list_qc_report_dates("redacted_references") == ["20260620"]

    def test_missing_qc_directory_lists_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("LOG_PATH", str(tmp_path))
        assert check_crud.list_qc_report_dates("obsolete_entities") == []

    def test_unknown_report_key_is_rejected(self, qc_tree):
        with pytest.raises(HTTPException) as excinfo:
            check_crud.list_qc_report_dates("../../etc/passwd")
        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST


class TestReadingASpecificRun:

    def test_datestamp_selects_that_run(self, qc_tree):
        result = check_crud.check_obsolete_entities("20260607")
        assert result["date-produced"] == "20260607"
        assert set(result["obsolete_entities"]) == {"WB", "FB"}

    def test_omitting_the_datestamp_reads_the_latest_file(self, qc_tree):
        assert check_crud.check_obsolete_entities()["date-produced"] == "20260707"

    def test_empty_datestamp_reads_the_latest_file(self, qc_tree):
        assert check_crud.check_obsolete_entities("")["date-produced"] == "20260707"

    def test_response_shape_is_unchanged(self, qc_tree):
        row = check_crud.check_obsolete_entities("20260607")["obsolete_entities"]["WB"][0]
        assert row == {
            "entity_type": "gene",
            "entity_status": "Obsolete",
            "entity_curie": "WB:WBGene0001",
            "entity_name": "unc-1",
            "reference_count": "2",
            "reference_curies": "AGRKB:101, AGRKB:102",
            "species": "NCBITaxon:6239",
        }

    def test_the_other_three_readers_accept_a_datestamp(self, qc_tree):
        orcids = check_crud.check_duplicate_orcids("20260618")
        assert orcids["duplicate_orcids"]["SGD"] == [
            {"reference_curie": "AGRKB:201", "orcid": "0000-0001-0000-0001", "author_names": "A Curator"}
        ]
        assert check_crud.check_obsolete_pmids("20260618")["obsolete_pmids"] == {"MGI": ["12345678"]}
        redacted = check_crud.check_redacted_references_with_tags("20260620")
        assert redacted["redacted-references"] == {"ZFIN": [{"reference_id": "AGRKB:301"}]}


class TestRejectedAndMissingRequests:

    @pytest.mark.parametrize("datestamp", [
        "2026-06-07",          # separators
        "202606",              # too short
        "202606077",           # too long
        "abcdefgh",            # not digits
        "../../../etc/passwd",  # traversal
        "20260607/../../etc",  # traversal after a valid prefix
    ])
    def test_malformed_datestamps_are_rejected(self, qc_tree, datestamp):
        with pytest.raises(HTTPException) as excinfo:
            check_crud.check_obsolete_entities(datestamp)
        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST

    def test_a_run_that_does_not_exist_is_not_found(self, qc_tree):
        with pytest.raises(HTTPException) as excinfo:
            check_crud.check_obsolete_entities("19990101")
        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND

    def test_a_missing_latest_file_is_not_found(self, qc_tree):
        # duplicate_orcids has archived runs in the fixture but no stable file.
        with pytest.raises(HTTPException) as excinfo:
            check_crud.check_duplicate_orcids()
        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND
