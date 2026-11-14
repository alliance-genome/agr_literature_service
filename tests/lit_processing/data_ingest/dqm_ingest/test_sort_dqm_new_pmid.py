from agr_literature_service.lit_processing.data_ingest.dqm_ingest.sort_dqm_json_reference_updates import \
    new_pmid_needs_new_reference

AGR = "AGRKB:101000001300992"


def test_no_new_pmid_creates_nothing():
    assert new_pmid_needs_new_reference(None, AGR, {}) is False


def test_new_pmid_attached_to_the_doi_matched_reference_is_not_created():
    """SCRUM-6864: a DQM entry whose DOI matched one reference and whose PMID is new used to create a
    second reference for the PMID as well as attaching the PMID to the matched one."""
    xrefs_to_add = {AGR: {"PMID": {"42438718": {"REFERENCE_WB.json"}},
                          "XB": {"XB-ART-59722": {"REFERENCE_WB.json"}}}}
    assert new_pmid_needs_new_reference("42438718", AGR, xrefs_to_add) is False


def test_new_pmid_not_attached_is_created():
    # e.g. the matched reference already has a different PMID, so the new one is a conflict, not queued
    assert new_pmid_needs_new_reference("42438718", AGR, {}) is True
    assert new_pmid_needs_new_reference("42438718", AGR, {AGR: {"XB": {"XB-ART-59722": set()}}}) is True
    assert new_pmid_needs_new_reference("42438718", AGR, {"AGRKB:101000000000001": {"PMID": {"42438718": set()}}}) is True
