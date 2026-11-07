"""SCRUM-6830: every paper put into the SGD corpus gets an active SGD curie,
an obsolete one is reactivated rather than replaced, the loaders' move into
the corpus is versioned, and the one-off backfill closes the gaps left from
before."""
import importlib.util
from pathlib import Path
from typing import Any, cast

from sqlalchemy import text

from agr_literature_service.api.models import CrossReferenceModel, ModCorpusAssociationModel, ModModel
from agr_literature_service.api.models.sgd_curie_listener import ensure_active_sgd_curie
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.lit_processing.data_ingest.utils.alliance_paper_utils import \
    update_sgd_corpus_flag_to_true
from ..data_ingest.utils.test_author import _make_reference
from ...fixtures import db  # noqa

# ticket-prefixed file name, so load it by path (see test_mark_article_plus_supplement_main)
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6830_backfill_sgd_curies.py"
)
_spec = importlib.util.spec_from_file_location("backfill_sgd_curies", _SCRIPT)
assert _spec is not None and _spec.loader is not None
script = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(script)
backfill_sgd_curies = cast(Any, script).backfill_sgd_curies


def _setup(db):  # noqa
    set_global_user_id(db, "SCRUM-6830_test")
    sgd = ModModel(abbreviation="SGD", short_name="SGD", full_name="Saccharomyces Genome Database")
    db.add(sgd)
    db.commit()
    return sgd.mod_id


def _paper(db, n, mod_id, corpus=False, sort_source="Mod_pubmed_search", pmid=None):  # noqa
    ref = _make_reference(db, f"AGRKB:SCRUM6830-{n}")
    mca = ModCorpusAssociationModel(reference_id=ref.reference_id, mod_id=mod_id, corpus=corpus,
                                    mod_corpus_sort_source=sort_source)
    db.add(mca)
    if pmid:
        db.add(CrossReferenceModel(curie=f"PMID:{pmid}", curie_prefix="PMID", reference_id=ref.reference_id))
    db.commit()
    return ref.reference_id


def _legacy_in_corpus(db, n, mod_id, sort_source="Mod_pubmed_search"):  # noqa
    """A paper put into the corpus the way the loaders did before SCRUM-6830:
    a raw UPDATE, which the ORM listener never sees, so it has no SGD curie."""
    rid = _paper(db, n, mod_id, corpus=False, sort_source=sort_source)
    db.execute(text("UPDATE mod_corpus_association SET corpus = TRUE WHERE reference_id = :rid"), {"rid": rid})
    db.commit()
    return rid


def _xref(db, reference_id, curie, obsolete):  # noqa
    db.add(CrossReferenceModel(curie=curie, curie_prefix="SGD", reference_id=reference_id,
                               pages=["reference"], is_obsolete=obsolete))
    db.commit()


def _sgd_curies(db, reference_id):  # noqa
    db.expire_all()
    return {(x.curie, x.is_obsolete) for x in db.query(CrossReferenceModel).filter(
        CrossReferenceModel.reference_id == reference_id, CrossReferenceModel.curie_prefix == "SGD")}


def _active(db, reference_id):  # noqa
    return [curie for curie, obsolete in _sgd_curies(db, reference_id) if not obsolete]


def _mca(db, reference_id, mod_id):  # noqa
    return db.query(ModCorpusAssociationModel).filter_by(reference_id=reference_id, mod_id=mod_id).one()


# --- the listener: every way into the SGD corpus ---------------------------------------------


def test_paper_added_in_corpus_gets_a_new_curie(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, sgd, corpus=True)
    active = _active(db, rid)
    assert len(active) == 1 and active[0].startswith("SGD:S")
    # the new curie is versioned like any other cross reference
    assert db.execute(text("SELECT count(*) FROM cross_reference_version WHERE curie = :c"),
                      {"c": active[0]}).scalar() == 1


def test_paper_moved_back_into_corpus_gets_its_old_curie_back(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, sgd, corpus=False)
    _xref(db, rid, "SGD:S000350202", obsolete=True)
    _mca(db, rid, sgd).corpus = True
    db.commit()
    assert _sgd_curies(db, rid) == {("SGD:S000350202", False)}


def test_curie_already_held_by_another_paper_is_not_reactivated(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, sgd, corpus=False)
    other = _legacy_in_corpus(db, 2, sgd)
    _xref(db, rid, "SGD:S000350202", obsolete=True)
    _xref(db, other, "SGD:S000350202", obsolete=False)
    _mca(db, rid, sgd).corpus = True
    db.commit()
    active = _active(db, rid)
    assert len(active) == 1 and active != ["SGD:S000350202"]


def test_curie_added_in_the_same_flush_is_kept(db):  # noqa
    # the DQM loaders add the MOD's own curie and the association together
    sgd = _setup(db)
    rid = _paper(db, 1, sgd, corpus=False)
    db.add(CrossReferenceModel(curie="SGD:S000350299", curie_prefix="SGD", reference_id=rid, pages=["reference"]))
    _mca(db, rid, sgd).corpus = True
    db.commit()
    assert _sgd_curies(db, rid) == {("SGD:S000350299", False)}


def test_other_mods_and_out_of_corpus_papers_get_no_sgd_curie(db):  # noqa
    sgd = _setup(db)
    fb = ModModel(abbreviation="FB", short_name="FB", full_name="FlyBase")
    db.add(fb)
    db.commit()
    assert _sgd_curies(db, _paper(db, 1, fb.mod_id, corpus=True)) == set()
    assert _sgd_curies(db, _paper(db, 2, sgd, corpus=False)) == set()


def test_ensure_does_nothing_when_a_curie_is_active(db):  # noqa
    sgd = _setup(db)
    rid = _legacy_in_corpus(db, 1, sgd)
    _xref(db, rid, "SGD:S000350202", obsolete=True)
    assert ensure_active_sgd_curie(db, rid)
    db.commit()
    assert _sgd_curies(db, rid) == {("SGD:S000350202", False)}
    assert not ensure_active_sgd_curie(db, rid)


# --- the loaders ------------------------------------------------------------------------------


def test_loader_move_into_corpus_is_versioned_and_gets_a_curie(db):  # noqa
    sgd = _setup(db)
    rid = _paper(db, 1, sgd, corpus=False, pmid="20740007")
    count, pmids = update_sgd_corpus_flag_to_true(db, {"20740007"})
    assert (count, pmids) == (1, {"20740007"})
    mca = _mca(db, rid, sgd)
    assert mca.corpus is True
    history = db.execute(text("SELECT corpus FROM mod_corpus_association_version WHERE id = :id "
                              "ORDER BY transaction_id"), {"id": mca.mod_corpus_association_id}).scalars().all()
    assert history == [False, True]
    assert len(_active(db, rid)) == 1


# --- the one-off backfill ---------------------------------------------------------------------


def test_backfill_covers_every_sort_source(db):  # noqa
    sgd = _setup(db)
    pubmed_search = _legacy_in_corpus(db, 1, sgd)
    manual = _legacy_in_corpus(db, 2, sgd, sort_source="Manual_creation")
    returned = _legacy_in_corpus(db, 3, sgd, sort_source="Manual_creation")
    _xref(db, returned, "SGD:S000350202", obsolete=True)
    has_one = _legacy_in_corpus(db, 4, sgd, sort_source="Gaf")
    _xref(db, has_one, "SGD:S000350203", obsolete=False)
    outside = _paper(db, 5, sgd, corpus=False)

    # a dry run changes nothing
    assert backfill_sgd_curies(db, dry_run=True) == 3
    assert _sgd_curies(db, pubmed_search) == set()

    assert backfill_sgd_curies(db) == 3
    for rid in (pubmed_search, manual):
        assert len(_active(db, rid)) == 1
    assert _sgd_curies(db, returned) == {("SGD:S000350202", False)}
    assert _sgd_curies(db, has_one) == {("SGD:S000350203", False)}
    assert _sgd_curies(db, outside) == set()
    assert backfill_sgd_curies(db) == 0
