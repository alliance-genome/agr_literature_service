"""The Alliance category comes from the MOD reference types, the reference
relations and the PubMed types, the first in CATEGORY_PRIORITY winning."""
import csv
import importlib.util
from pathlib import Path
from typing import Any, cast

import pytest

from agr_literature_service.api.crud import mod_reference_type_crud
from agr_literature_service.api.models import ModModel, ModReferencetypeAssociationModel, PersonModel, \
    ReferenceModel, ReferencetypeModel, UserModel
from agr_literature_service.api.schemas import ModReferenceTypeSchemaPost
from agr_literature_service.api.user import _set_current_user, get_current_agent_id, get_global_user_id, \
    set_global_user_id
from agr_literature_service.lit_processing.data_ingest.utils.alliance_category_utils import \
    CATEGORY_PRIORITY, CategoryInputs, category_name, load_mapping, resolve_category, set_reference_category
from .test_author import _make_curator_user
from ....fixtures import db, populate_test_mod_reference_types  # noqa

_SCRIPT = (Path(__file__).resolve().parents[4] / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
           / "backfill_alliance_category_from_mod_reference_types.py")
_spec = importlib.util.spec_from_file_location("backfill_alliance_category", _SCRIPT)
assert _spec is not None and _spec.loader is not None
backfill_module = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(backfill_module)
backfill = cast(Any, backfill_module).backfill


def test_mapping():
    mapping = load_mapping()
    assert set(c for c in mapping.values() if c) <= set(CATEGORY_PRIORITY)
    assert mapping[("FB", "review")] == "Review_Article"
    assert mapping[("ZFIN", "review")] == "Review_Article"
    assert mapping[("WB", "comment")] == "Comment"
    assert mapping[("WB", "letter")] == "Comment"
    assert mapping[("FB", "erratum")] == "Correction"
    assert mapping[("WB", "journal_article")] == "Research_Article"
    # flags and generic article types give no category
    assert mapping[("WB", "experimental")] is None
    assert mapping[("MGI", "peer reviewed article")] is None
    # unclear until curators confirm
    assert mapping[("WB", "wormbook")] == "Other"
    assert mapping[("SGD", "chapter")] == "Other"


def _inputs(*types, relations=(), curator=False):
    return CategoryInputs(mod_reference_types=list(types), relation_types=set(relations), set_by_curator=curator)


@pytest.mark.parametrize("inputs, pubmed, current, expected", [
    # a MOD's review outranks PubMed's research article
    (_inputs(("FB", "review")), "Research_Article", "Research_Article", "Review_Article"),
    # review wins over another MOD's comment, and over a generic type
    (_inputs(("FB", "review"), ("WB", "Comment"), ("WB", "Journal_article")), "Research_Article", None,
     "Review_Article"),
    # WB comments are comments, whatever category WB's DQM file sends
    (_inputs(("WB", "Comment"), ("WB", "Not_experimental")), "Personal_Communication", "Personal_Communication",
     "Comment"),
    # flags and generic types leave it to PubMed
    (_inputs(("WB", "Experimental"), ("WB", "Journal_article")), "Review_Article", "Research_Article",
     "Review_Article"),
    (_inputs(("WB", "Experimental")), "Research_Article", "Review_Article", "Research_Article"),
    # WB Journal_article is a research article, below any other category
    (_inputs(("WB", "Journal_article"), ("WB", "Experimental")), None, "Personal_Communication",
     "Research_Article"),
    (_inputs(("WB", "Journal_article")), "Comment", "Research_Article", "Comment"),
    # a retraction notice outranks everything
    (_inputs(("FB", "review"), relations=["RetractionOf"]), "Research_Article", None, "Retraction"),
    (_inputs(relations=["CommentOn"]), "Research_Article", None, "Comment"),
    # unclear types are Other until confirmed, still below a review
    (_inputs(("WB", "WormBook")), "Research_Article", None, "Other"),
    (_inputs(("WB", "WormBook"), ("WB", "Review")), "Research_Article", None, "Review_Article"),
    # a paper in PubMed is not a personal communication, whatever the MOD type
    (_inputs(("MGI", "Personal Communication")), "Research_Article", "Personal_Communication",
     "Research_Article"),
    (_inputs(("SGD", "Personal Communication to SGD")), None, "Other", "Personal_Communication"),
    (_inputs(("WB", "Email")), None, "Other", "Personal_Communication"),
    # nothing gives a category: keep the current one (e.g. the DQM category of a paper with no PMID)
    (_inputs(("WB", "Experimental")), None, "Personal_Communication", "Personal_Communication"),
    # obsolete references and a curator's category are left alone
    (_inputs(("FB", "review")), "Research_Article", "Obsolete", "Obsolete"),
    (_inputs(("FB", "review"), curator=True), "Research_Article", "Research_Article", "Research_Article"),
    # categories in any of their spellings
    (_inputs(("ZFIN", "review")), "research_article", "ReferenceCategory.Research_Article", "Review_Article"),
])
def test_resolve_category(inputs, pubmed, current, expected):
    assert resolve_category(inputs, pubmed, current) == expected


def _reference(db, curie, category="Research_Article", pubmed_types=("Journal Article",)):  # noqa
    ref = ReferenceModel(curie=curie, category=category, pubmed_types=list(pubmed_types))
    db.add(ref)
    db.commit()
    return ref.reference_id


class TestSetReferenceCategory:

    def setup_method(self):
        self._user = (get_global_user_id(), get_current_agent_id())

    def teardown_method(self):
        _set_current_user(*self._user)

    def test_mod_reference_type_api(self, db, populate_test_mod_reference_types):  # noqa
        set_global_user_id(db, "alliance_category_test")
        reference_id = _reference(db, "AGRKB:CAT-TEST-1")
        curie = "AGRKB:CAT-TEST-1"
        mrt_id = mod_reference_type_crud.create(db, ModReferenceTypeSchemaPost(
            reference_curie=curie, reference_type="Review", mod_abbreviation="ZFIN"))
        assert category_name(db.get(ReferenceModel, reference_id).category) == "Review_Article"
        # without the review, PubMed's category again
        mod_reference_type_crud.destroy(db, mrt_id)
        assert category_name(db.get(ReferenceModel, reference_id).category) == "Research_Article"

    def test_dqm_category_of_paper_without_pmid(self, db, populate_test_mod_reference_types):  # noqa
        """The MOD's DQM category is one of the sources: a higher one wins over the
        types, and a second run (the next nightly load) changes nothing."""
        set_global_user_id(db, "alliance_category_test")
        reference_id = _reference(db, "AGRKB:CAT-TEST-7", pubmed_types=())
        mod_reference_type_crud.insert_mod_reference_type_into_db(db, [], "WB", "Journal_article", reference_id)
        assert set_reference_category(db, reference_id, "Review Article") == ("Research_Article", "Review_Article")
        assert set_reference_category(db, reference_id, "Review Article") is None
        # without it (a paper with a PMID), the types and PubMed decide
        assert set_reference_category(db, reference_id) == ("Review_Article", "Research_Article")
        db.commit()

    def test_curator_category_kept(self, db, populate_test_mod_reference_types):  # noqa
        set_global_user_id(db, "alliance_category_test")
        reference_id = _reference(db, "AGRKB:CAT-TEST-2")
        curator = _make_curator_user(db, "AGR:CAT-TEST-2")
        _set_current_user(curator, db.query(UserModel).join(
            PersonModel, PersonModel.person_id == UserModel.person_id).filter(
            PersonModel.curie == curator).one().user_id)
        db.get(ReferenceModel, reference_id).category = "Comment"
        db.commit()
        set_global_user_id(db, "alliance_category_test")
        mod_reference_type_crud.insert_mod_reference_type_into_db(db, [], "ZFIN", "Review", reference_id)
        assert set_reference_category(db, reference_id) is None
        assert category_name(db.get(ReferenceModel, reference_id).category) == "Comment"

    def test_backfill(self, db, populate_test_mod_reference_types, tmp_path):  # noqa
        set_global_user_id(db, "alliance_category_test")
        review = _reference(db, "AGRKB:CAT-TEST-3")
        book = _reference(db, "AGRKB:CAT-TEST-4", pubmed_types=())
        unchanged = _reference(db, "AGRKB:CAT-TEST-5")
        # PubMed alone would make it a research article: left to the PubMed update
        pubmed_only = _reference(db, "AGRKB:CAT-TEST-6", category="Comment")
        for mod, label, reference_id in (("ZFIN", "Review", review), ("FB", "book", book),
                                         ("WB", "Journal_article", unchanged)):
            mod_reference_type_crud.insert_mod_reference_type_into_db(db, [], mod, label, reference_id)

        report = tmp_path / "report.tsv"
        counts = backfill(db, apply=False, report_file=str(report))
        assert counts == {("Research_Article", "Review_Article"): 1, ("Research_Article", "Book"): 1}
        assert category_name(db.get(ReferenceModel, review).category) == "Research_Article"
        rows = list(csv.DictReader(open(report), delimiter="\t"))
        assert {(r["curie"], r["new_category"]) for r in rows} == {
            ("AGRKB:CAT-TEST-3", "Review_Article"), ("AGRKB:CAT-TEST-4", "Book")}

        backfill(db, apply=True, report_file=str(report))
        db.expire_all()
        assert category_name(db.get(ReferenceModel, review).category) == "Review_Article"
        assert category_name(db.get(ReferenceModel, book).category) == "Book"
        assert category_name(db.get(ReferenceModel, unchanged).category) == "Research_Article"
        assert category_name(db.get(ReferenceModel, pubmed_only).category) == "Comment"
        assert backfill(db, apply=False, report_file=str(report)) == {}

    def test_personal_communication_in_pubmed(self, db, populate_test_mod_reference_types):  # noqa
        set_global_user_id(db, "alliance_category_test")
        mgi = db.query(ModModel).filter(ModModel.abbreviation == "MGI").one()
        db.add(ModReferencetypeAssociationModel(mod=mgi, referencetype=ReferencetypeModel(
            label="Personal Communication"), display_order=10))
        db.commit()
        published = _reference(db, "AGRKB:CAT-TEST-8")
        unpublished = _reference(db, "AGRKB:CAT-TEST-9", category="Other", pubmed_types=())
        for reference_id in (published, unpublished):
            mod_reference_type_crud.insert_mod_reference_type_into_db(
                db, [], "MGI", "Personal Communication", reference_id)
        assert set_reference_category(db, published) is None
        assert set_reference_category(db, unpublished) == ("Other", "Personal_Communication")
        db.commit()
