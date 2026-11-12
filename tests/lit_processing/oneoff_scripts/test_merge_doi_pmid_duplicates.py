"""SCRUM-6864: papers in ABC twice, as a MOD DQM record with the DOI and a PubMed record with the PMID, are
merged into the DOI record, and pairs whose records disagree are held for curators."""
import importlib.util
from pathlib import Path
from typing import Any, cast

from agr_literature_service.api.crud import reference_crud
from agr_literature_service.api.models import CrossReferenceModel, ModCorpusAssociationModel, ModModel, \
    ReferenceModel, WorkflowTagModel
from agr_literature_service.api.user import set_global_user_id
from ..data_ingest.utils.test_author import _make_reference
from ...fixtures import db  # noqa

# ticket-prefixed file name, so load it by path (see test_cleanup_automated_author_history)
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6864_merge_doi_pmid_duplicates.py"
)
_spec = importlib.util.spec_from_file_location("merge_doi_pmid_duplicates", _SCRIPT)
assert _spec is not None and _spec.loader is not None
script = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(script)
Side = cast(Any, script).Side
find_conflicts = cast(Any, script).find_conflicts

WB, FB = "WB", "FB"


def _side(xrefs=None, corpus=None, workflow_mods=(), curation_status_mods=(), indexing_priority_mods=()):
    return Side("AGRKB:x", xrefs or {}, corpus or {}, set(workflow_mods), set(curation_status_mods),
                set(indexing_priority_mods))


def test_typical_micropub_pair_merges():
    doi_side = _side({"DOI": "DOI:10.17912/micropub.biology.002193", "WB": "WB:WBPaper00069799"},
                     {WB: True}, workflow_mods=[WB])
    pmid_side = _side({"PMID": "PMID:42438718", "PMCID": "PMCID:PMC13356707"}, {WB: None, FB: True},
                      workflow_mods=[FB])
    assert find_conflicts(doi_side, pmid_side) == []


def test_case_variant_doi_is_not_a_conflict():
    doi_side = _side({"DOI": "DOI:10.1534/GENETICS.113.157685"})
    pmid_side = _side({"DOI": "DOI:10.1534/genetics.113.157685", "PMID": "PMID:24318535"})
    assert find_conflicts(doi_side, pmid_side) == []


def test_disagreeing_records_are_held():
    doi_side = _side({"WB": "WB:WBPaper00068719"}, {WB: False}, workflow_mods=[WB], curation_status_mods=[WB],
                     indexing_priority_mods=[WB])
    pmid_side = _side({"WB": "WB:WBPaper00068726", "PMID": "PMID:41127995"}, {WB: True}, workflow_mods=[WB],
                      curation_status_mods=[WB], indexing_priority_mods=[WB])
    assert find_conflicts(doi_side, pmid_side) == [
        "two WB IDs: WB:WBPaper00068719 and WB:WBPaper00068726",
        "WB corpus False and True",
        "WB workflow tags on both",
        "WB curation status on both",
        "WB indexing priority on both",
    ]


def test_merge_pair_moves_the_pmid_record_onto_the_doi_record(db, monkeypatch):  # noqa
    set_global_user_id(db, "SCRUM-6864_test")
    wb = ModModel(abbreviation="WB", short_name="WB", full_name="WormBase")
    fb = ModModel(abbreviation="FB", short_name="FB", full_name="FlyBase")
    db.add_all([wb, fb])
    db.commit()
    doi_ref = _make_reference(db, "AGRKB:SCRUM6864-DOI")
    pmid_ref = _make_reference(db, "AGRKB:SCRUM6864-PMID")
    db.add_all([
        CrossReferenceModel(curie="DOI:10.17912/micropub.biology.002193", curie_prefix="DOI",
                            reference_id=doi_ref.reference_id),
        CrossReferenceModel(curie="WB:WBPaper00069799", curie_prefix="WB", reference_id=doi_ref.reference_id),
        CrossReferenceModel(curie="PMID:42438718", curie_prefix="PMID", reference_id=pmid_ref.reference_id),
        CrossReferenceModel(curie="PMCID:PMC13356707", curie_prefix="PMCID", reference_id=pmid_ref.reference_id),
        ModCorpusAssociationModel(reference_id=doi_ref.reference_id, mod_id=wb.mod_id, corpus=None,
                                  mod_corpus_sort_source="Dqm_files"),
        ModCorpusAssociationModel(reference_id=pmid_ref.reference_id, mod_id=wb.mod_id, corpus=True,
                                  mod_corpus_sort_source="Mod_pubmed_search"),
        ModCorpusAssociationModel(reference_id=pmid_ref.reference_id, mod_id=fb.mod_id, corpus=True,
                                  mod_corpus_sort_source="Mod_pubmed_search"),
        WorkflowTagModel(reference_id=pmid_ref.reference_id, mod_id=fb.mod_id, workflow_tag_id="ATP:0000141"),
    ])
    db.commit()
    # no PubMed or ontology calls, no merge e-mail
    monkeypatch.setattr(reference_crud, "update_data", lambda mod, pmids: None)
    monkeypatch.setattr(reference_crud, "revalidate_all_tags", lambda **kwargs: None)
    monkeypatch.setattr(reference_crud, "send_report", lambda subject, message: None)

    doi_side = cast(Any, script).load_side(db, doi_ref)
    assert find_conflicts(doi_side, cast(Any, script).load_side(db, pmid_ref)) == []
    cast(Any, script).merge_pair(db, doi_ref, pmid_ref, doi_side)

    assert db.query(ReferenceModel).filter(ReferenceModel.curie == "AGRKB:SCRUM6864-PMID").one_or_none() is None
    rid = doi_ref.reference_id
    xrefs = {x.curie for x in db.query(CrossReferenceModel).filter(CrossReferenceModel.reference_id == rid).all()}
    assert xrefs == {"DOI:10.17912/micropub.biology.002193", "WB:WBPaper00069799", "PMID:42438718",
                     "PMCID:PMC13356707"}
    corpus = {(m.mod_id, m.corpus) for m in db.query(ModCorpusAssociationModel).filter(
        ModCorpusAssociationModel.reference_id == rid).all()}
    assert corpus == {(wb.mod_id, True), (fb.mod_id, True)}
    tags = {(t.mod_id, t.workflow_tag_id) for t in db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == rid).all()}
    assert tags == {(fb.mod_id, "ATP:0000141")}
