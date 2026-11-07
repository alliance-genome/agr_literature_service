"""load_sgd_created_references makes ABC agree with the papers SGD created
itself: same SGD ID, in the SGD corpus (SCRUM-6830)."""
from unittest.mock import patch

from sqlalchemy import text

from agr_literature_service.api.models import CrossReferenceModel, ModCorpusAssociationModel, ModModel
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.dqm_ingest import load_sgd_created_references as loader
from ..utils.test_author import _make_reference
from ....fixtures import db  # noqa

MODULE = "agr_literature_service.lit_processing.data_ingest.dqm_ingest.load_sgd_created_references"


def _setup(db):  # noqa
    set_global_user_id(db, "load_sgd_created_references_test")
    sgd = ModModel(abbreviation="SGD", short_name="SGD", full_name="Saccharomyces Genome Database")
    db.add(sgd)
    db.commit()
    return sgd.mod_id


def _paper(db, n, pmid=None, doi=None, sgd_ids=(), sgd_mod_id=None, corpus=None):  # noqa
    """An ABC paper with the given PMID/DOI and SGD IDs ((curie, obsolete) pairs).
    The SGD association is added with a raw UPDATE to corpus True, so the
    listener does not mint an ID for it (the state the loaders left)."""
    ref = _make_reference(db, f"AGRKB:SGDCREATED-{n}")
    rid = ref.reference_id
    if pmid:
        db.add(CrossReferenceModel(curie=pmid, curie_prefix="PMID", reference_id=rid))
    if doi:
        db.add(CrossReferenceModel(curie=f"DOI:{doi}", curie_prefix="DOI", reference_id=rid))
    for curie, obsolete in sgd_ids:
        db.add(CrossReferenceModel(curie=curie, curie_prefix="SGD", reference_id=rid, pages=["reference"],
                                   is_obsolete=obsolete))
    if corpus is not None:
        db.add(ModCorpusAssociationModel(reference_id=rid, mod_id=sgd_mod_id, corpus=False,
                                         mod_corpus_sort_source="Mod_pubmed_search"))
    db.commit()
    if corpus:
        db.execute(text("UPDATE mod_corpus_association SET corpus = TRUE WHERE reference_id = :rid"), {"rid": rid})
        db.commit()
    return rid


def _sgd(sgdid, pmid=None, doi=None):
    return {"sgdid": sgdid, "pmid": pmid, "doi": doi, "display_name": "Smith J, et al. (2025)",
            "date_created": "2026-03-01 16:13:06", "created_by": "OTTO"}


def _sgd_ids(db, rid):  # noqa
    db.expire_all()
    return sorted((x.curie, x.is_obsolete) for x in db.query(CrossReferenceModel).filter_by(
        reference_id=rid, curie_prefix="SGD"))


def _in_corpus(db, rid, sgd_mod_id):  # noqa
    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=rid, mod_id=sgd_mod_id).one_or_none()
    return mca is not None and mca.corpus is True


def _run(db, papers, apply=True):  # noqa
    sgd_mod_id = db.query(ModModel.mod_id).filter_by(abbreviation="SGD").scalar()
    return loader.reconcile(db, sgd_mod_id, papers, apply=apply)


def test_paper_with_an_abc_minted_id_gets_sgds_id(db):  # noqa
    # the 13-paper case: ABC minted S100..., SGD already had the paper as S000...
    sgd = _setup(db)
    rid = _paper(db, 1, pmid="PMID:39322276", sgd_ids=[("SGD:S100006889", False)], sgd_mod_id=sgd, corpus=True)
    results = _run(db, [_sgd("SGD:S000382006", "PMID:39322276")])
    assert results[loader.ID_REPLACED] == ["SGD:S100006889 -> SGD:S000382006 (PMID:39322276)"]
    assert _sgd_ids(db, rid) == [("SGD:S000382006", False)]
    assert _in_corpus(db, rid, sgd)


def test_paper_out_of_corpus_without_an_id_gets_sgds_id_and_no_minted_one(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, pmid="PMID:40327034", sgd_mod_id=sgd, corpus=False)
    results = _run(db, [_sgd("SGD:S000382011", "PMID:40327034")])
    assert list(results) == [loader.ID_ADDED]
    # the listener saw SGD's ID in the same flush and did not mint an S100 ID
    assert _sgd_ids(db, rid) == [("SGD:S000382011", False)]
    assert _in_corpus(db, rid, sgd)


def test_paper_with_no_sgd_association_is_added_to_the_corpus(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, pmid="PMID:111")
    _run(db, [_sgd("SGD:S000390001", "PMID:111")])
    assert _sgd_ids(db, rid) == [("SGD:S000390001", False)]
    mca = db.query(ModCorpusAssociationModel).filter_by(reference_id=rid, mod_id=sgd).one()
    assert (mca.corpus, mca.mod_corpus_sort_source) == (True, loader.SORT_SOURCE)


def test_obsolete_copy_of_sgds_id_is_reactivated(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, pmid="PMID:222", sgd_ids=[("SGD:S000350202", True), ("SGD:S100000700", False)],
                 sgd_mod_id=sgd, corpus=True)
    results = _run(db, [_sgd("SGD:S000350202", "PMID:222")])
    assert list(results) == [loader.ID_REPLACED]
    assert _sgd_ids(db, rid) == [("SGD:S000350202", False), ("SGD:S100000700", True)]


def test_matching_paper_is_left_alone_and_out_of_corpus_one_moved_in(db):  # noqa
    sgd = _setup(db)
    same = _paper(db, 1, pmid="PMID:333", sgd_ids=[("SGD:S000350203", False)], sgd_mod_id=sgd, corpus=True)
    outside = _paper(db, 2, pmid="PMID:444", sgd_ids=[("SGD:S000350204", False)], sgd_mod_id=sgd, corpus=False)
    results = _run(db, [_sgd("SGD:S000350203", "PMID:333"), _sgd("SGD:S000350204", "PMID:444")])
    assert len(results[loader.OK]) == 1 and len(results[loader.MOVED_INTO_CORPUS]) == 1
    assert _sgd_ids(db, same) == [("SGD:S000350203", False)]
    assert _in_corpus(db, outside, sgd)


def test_doi_is_used_when_there_is_no_pmid(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, doi="10.1000/abc")
    _run(db, [_sgd("SGD:S000390002", doi="10.1000/abc")])
    assert _sgd_ids(db, rid) == [("SGD:S000390002", False)]
    assert _in_corpus(db, rid, sgd)


def test_reported_not_changed(db):  # noqa
    sgd = _setup(db)
    holder = _paper(db, 1, pmid="PMID:555", sgd_ids=[("SGD:S000390003", False)], sgd_mod_id=sgd, corpus=True)
    other = _paper(db, 2, pmid="PMID:666")
    results = _run(db, [
        _sgd("SGD:S000390003", "PMID:666"),            # SGD's ID is on another paper than the PMID
        _sgd("SGD:S000390004", "PMID:777"),            # PMID not in ABC
        _sgd("SGD:S000390005", doi="10.1000/none"),    # nothing to match
    ])
    assert len(results[loader.CONFLICT]) == 1
    assert results[loader.NEW_PMID] == ["PMID:777"]
    assert len(results[loader.NEEDS_CURATOR]) == 1
    assert _sgd_ids(db, holder) == [("SGD:S000390003", False)]
    assert _sgd_ids(db, other) == []


def test_an_sgd_made_id_is_never_replaced(db):  # noqa
    # the paper's ID is an S000 one from SGD too: two SGD papers for one PMID,
    # so a curator decides; nothing is renamed
    sgd = _setup(db)
    rid = _paper(db, 1, pmid="PMID:123", sgd_ids=[("SGD:S000100001", False)], sgd_mod_id=sgd, corpus=True)
    results = _run(db, [_sgd("SGD:S000390010", "PMID:123")])
    assert results[loader.CONFLICT] == ["PMID:123 has SGD:S000100001 in ABC, SGD lists SGD:S000390010"]
    assert _sgd_ids(db, rid) == [("SGD:S000100001", False)]


def test_same_pmid_twice_renames_only_once(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, pmid="PMID:124", sgd_ids=[("SGD:S100009001", False)], sgd_mod_id=sgd, corpus=True)
    listed = [_sgd("SGD:S000390011", "PMID:124"), _sgd("SGD:S000390012", "PMID:124")]
    # the dry run reports what the real run does
    dry = _run(db, listed, apply=False)
    assert len(dry[loader.ID_REPLACED]) == 1 and len(dry[loader.CONFLICT]) == 1
    results = _run(db, listed)
    assert len(results[loader.ID_REPLACED]) == 1 and len(results[loader.CONFLICT]) == 1
    assert _sgd_ids(db, rid) == [("SGD:S000390011", False)]


def test_doi_is_used_when_abc_lacks_the_pmid(db):  # noqa
    # loading the PMID would duplicate the paper ABC already has by DOI
    sgd = _setup(db)
    rid = _paper(db, 1, doi="10.1000/preprint")
    results = _run(db, [_sgd("SGD:S000390013", "PMID:125", doi="10.1000/preprint")])
    assert loader.NEW_PMID not in results
    assert _sgd_ids(db, rid) == [("SGD:S000390013", False)]
    assert _in_corpus(db, rid, sgd)


def test_dry_run_changes_nothing(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, pmid="PMID:39322276", sgd_ids=[("SGD:S100006889", False)], sgd_mod_id=sgd, corpus=False)
    results = _run(db, [_sgd("SGD:S000382006", "PMID:39322276")], apply=False)
    assert list(results) == [loader.ID_REPLACED]
    assert _sgd_ids(db, rid) == [("SGD:S100006889", False)]
    assert not _in_corpus(db, rid, sgd)


def test_new_pmids_are_loaded_then_given_sgds_id(db):  # noqa
    sgd = _setup(db)

    def fake_load(session, pmids):
        # stands in for the PubMed load: the paper now exists in ABC
        _paper(session, 9, pmid="PMID:888")
        return {"PMID:888"}

    with patch(f"{MODULE}.fetch_sgd_created_references",
               return_value=[_sgd("SGD:S000390006", "PMID:888"), _sgd("SGD:S000390007", "PMID:999")]), \
            patch(f"{MODULE}.create_postgres_session", return_value=db), \
            patch.object(db, "close"), \
            patch(f"{MODULE}.load_new_pmids", side_effect=fake_load):
        results = loader.load_sgd_created_references(days=2, send=False)
    assert results[loader.ID_ADDED] == ["SGD:S000390006 (PMID:888)"]
    assert results[loader.NOT_LOADED] == ["PMID:999"]
    rid = db.query(CrossReferenceModel.reference_id).filter_by(curie="PMID:888").scalar()
    assert _sgd_ids(db, rid) == [("SGD:S000390006", False)]
    assert _in_corpus(db, rid, sgd)


def test_report_lists_the_changes():
    results = {loader.OK: [""], loader.ID_REPLACED: ["SGD:S1 -> SGD:S0 (PMID:1)"],
               loader.NEEDS_CURATOR: ["SGD:S000390005  doi=10.1000/none"]}
    message = loader.compose_report(2, 3, results, False)
    assert "SGD-created papers from the last 2 day(s): 3" in message
    assert "<li>SGD:S1 -> SGD:S0 (PMID:1)" in message
    assert loader.NEEDS_CURATOR in message


def test_fetch_reads_the_created_in_sgd_list():
    with patch(f"{MODULE}.requests.get") as get:
        get.return_value.json.return_value = {"days": 2, "references": [_sgd("SGD:S000390020", "PMID:1")]}
        assert loader.fetch_sgd_created_references(2) == [_sgd("SGD:S000390020", "PMID:1")]
        assert get.call_args.kwargs["params"] == {"days": 2}
        assert get.call_args.args[0].endswith("/references/created_in_sgd")
        # another SGD endpoint (e.g. the entity lookup) answers with a list
        get.return_value.json.return_value = [{"modEntityId": "SGD:S000001855"}]
        try:
            loader.fetch_sgd_created_references(2)
            raise AssertionError("expected ValueError")
        except ValueError as e:
            assert "did not return SGD's created_in_sgd list" in str(e)
