import json
import logging
from unittest.mock import MagicMock, patch

from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import check_handle_duplicate

logger = logging.getLogger(__name__)

MODULE = "agr_literature_service.lit_processing.data_ingest.utils.db_write_utils"


def _write_pubmed_json(json_dir, pmid, doi):
    with open(json_dir / f"{pmid}.json", "w") as fh:
        json.dump({"crossReferences": [{"id": f"PMID:{pmid}"}, {"id": f"DOI:{doi}"}]}, fh)


def _run(tmp_path, monkeypatch, pmids, doi_records, pmid_dois):
    """doi_records: {doi: (agr_curie, [pmids already on that record])}; pmid_dois: {pmid: doi}."""
    json_dir = tmp_path / "pubmed_json"
    json_dir.mkdir()
    for pmid, doi in pmid_dois.items():
        _write_pubmed_json(json_dir, pmid, doi)
    monkeypatch.setenv("XML_PATH", f"{tmp_path}/")
    monkeypatch.setenv("LOG_PATH", str(tmp_path / "logs"))
    monkeypatch.delenv("LOG_URL", raising=False)
    xref_ref = {"DOI": {doi: agr for doi, (agr, _) in doi_records.items()}, "PMCID": {}}
    ref_xref_valid = {agr: ({"DOI": doi, "PMID": set(found)} if found else {"DOI": doi})
                      for doi, (agr, found) in doi_records.items()}
    db_session = MagicMock()
    with patch(f"{MODULE}.get_reference_id_by_curie", side_effect=lambda db, curie: curie):
        _, _, not_loaded = check_handle_duplicate(db_session, "WB", pmids, xref_ref, ref_xref_valid, logger)
    attached = {(xref.curie, xref.reference_id) for xref in (c.args[0] for c in db_session.add.call_args_list)}
    return attached, not_loaded


def test_consecutive_pmids_matching_doi_records_are_all_attached(tmp_path, monkeypatch):
    """SCRUM-6864: removing a matched PMID from the list being iterated skipped the next PMID, so every
    second micropub in a run of DOI matches was created as a duplicate of its DOI record."""
    pmid_dois = {str(42438710 + i): f"10.17912/micropub.biology.00{2190 + i}" for i in range(4)}
    doi_records = {doi: (f"AGRKB:10100000130099{i}", []) for i, doi in enumerate(pmid_dois.values())}
    pmids = sorted(pmid_dois)

    attached, not_loaded = _run(tmp_path, monkeypatch, pmids, doi_records, pmid_dois)

    assert pmids == []
    assert attached == {(f"PMID:{pmid}", doi_records[doi][0]) for pmid, doi in pmid_dois.items()}
    assert not_loaded == []


def test_unmatched_pmids_stay_to_be_created(tmp_path, monkeypatch):
    pmid_dois = {"42438718": "10.17912/micropub.biology.002193",
                 "42438719": "10.1000/not.in.db",
                 "42438720": "10.17912/micropub.biology.002194"}
    doi_records = {"10.17912/micropub.biology.002193": ("AGRKB:101000001300992", []),
                   "10.17912/micropub.biology.002194": ("AGRKB:101000001300993", [])}
    pmids = sorted(pmid_dois)

    attached, _ = _run(tmp_path, monkeypatch, pmids, doi_records, pmid_dois)

    assert pmids == ["42438719"]
    assert attached == {("PMID:42438718", "AGRKB:101000001300992"),
                        ("PMID:42438720", "AGRKB:101000001300993")}


def test_doi_record_with_another_pmid_is_reported_not_attached(tmp_path, monkeypatch):
    pmid_dois = {"42438718": "10.17912/micropub.biology.002193"}
    doi_records = {"10.17912/micropub.biology.002193": ("AGRKB:101000001300992", ["40000000"])}
    pmids = ["42438718"]

    attached, not_loaded = _run(tmp_path, monkeypatch, pmids, doi_records, pmid_dois)

    assert pmids == []
    assert attached == set()
    assert not_loaded == [("42438718", "10.17912/micropub.biology.002193", "40000000")]


def test_doi_matches_regardless_of_case(tmp_path, monkeypatch):
    """SCRUM-6864: DOI keys are lower-cased when the lookup is loaded (xref_lookup_key), so a PubMed DOI in a
    different case still finds its record."""
    pmid_dois = {"24318535": "10.1534/GENETICS.113.157685"}
    doi_records = {"10.1534/genetics.113.157685": ("AGRKB:101000000953000", [])}
    pmids = ["24318535"]

    attached, _ = _run(tmp_path, monkeypatch, pmids, doi_records, pmid_dois)

    assert pmids == []
    assert attached == {("PMID:24318535", "AGRKB:101000000953000")}
