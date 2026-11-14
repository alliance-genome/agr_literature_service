"""Tests for link_authors_to_persons (SCRUM-5744)."""
from unittest.mock import patch

from agr_literature_service.api.models import (
    AuthorModel,
    ModCorpusAssociationModel,
    ModModel,
    PersonCrossReferenceModel,
    PersonEmailModel,
    PersonModel,
    PersonNameModel,
    ReferenceModel,
    WorkflowTagModel,
    WorkflowTransitionModel,
)
from agr_literature_service.lit_processing.data_ingest.pubmed_ingest import (
    link_authors_to_persons as mod,
)
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ....fixtures import db, load_name_to_atp_and_relationships_mock  # noqa


def _person(db, curie, last_name, orcid=None, emails=()):  # noqa
    person = PersonModel(display_name=f"Test {last_name}", curie=curie)
    db.add(person)
    db.flush()
    db.add(PersonNameModel(person_id=person.person_id, first_name="Test",
                           last_name=last_name, is_primary=True))
    if orcid:
        db.add(PersonCrossReferenceModel(person_id=person.person_id, curie=orcid,
                                         curie_prefix="ORCID"))
    for email in emails:
        db.add(PersonEmailModel(person_id=person.person_id, email_address=email))
    db.commit()
    return person.person_id


def _reference(db, curie):  # noqa
    ref = ReferenceModel(curie=curie, category="research_article")
    db.add(ref)
    db.commit()
    return ref.reference_id


def _author(db, reference_id, order, last_name, orcid=None, email=None):  # noqa
    author = AuthorModel(reference_id=reference_id, author_order=order,
                         name=f"Test {last_name}", last_name=last_name,
                         orcid=orcid, email_address=email)
    db.add(author)
    db.commit()
    return author.author_id


def _linked(db, author_id):  # noqa
    db.expire_all()
    return db.query(AuthorModel).filter(AuthorModel.author_id == author_id).one()


ORCID_1 = "ORCID:0000-0001-0000-0001"
ORCID_2 = "ORCID:0000-0001-0000-0002"


class TestNormalizeName:

    def test_folds_case_accents_and_punctuation(self):
        assert mod.normalize_name("García-López") == mod.normalize_name("garcia lopez")
        assert mod.normalize_name("O'Brien") == "obrien"
        assert mod.normalize_name(None) == ""


class TestOrcidPass:

    def test_links_on_orcid_and_last_name(self, db):  # noqa
        person_id = _person(db, "AGRKB:P5744-1", "Smith", orcid=ORCID_1)
        ref_id = _reference(db, "AGRKB:R5744-1")
        author_id = _author(db, ref_id, 1, "Smith", orcid=ORCID_1)

        counts = mod.link_authors_to_persons(db)

        author = _linked(db, author_id)
        assert author.person_id == person_id
        assert author.updated_by == mod.ORCID_USER
        assert counts[f"{mod.ORCID_USER}_linked"] == 1

    def test_orcid_match_is_case_and_accent_insensitive(self, db):  # noqa
        person_id = _person(db, "AGRKB:P5744-2", "Müller", orcid="ORCID:0000-0001-0000-000x")
        ref_id = _reference(db, "AGRKB:R5744-2")
        author_id = _author(db, ref_id, 1, "Muller", orcid="ORCID:0000-0001-0000-000X")

        mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id == person_id

    def test_no_link_when_last_name_differs(self, db):  # noqa
        _person(db, "AGRKB:P5744-3", "Smith", orcid=ORCID_1)
        ref_id = _reference(db, "AGRKB:R5744-3")
        author_id = _author(db, ref_id, 1, "Jones", orcid=ORCID_1)

        counts = mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id is None
        assert counts["orcid_last_name_mismatch"] == 1

    def test_no_link_when_orcid_duplicated_on_paper(self, db):  # noqa
        _person(db, "AGRKB:P5744-4", "Smith", orcid=ORCID_1)
        ref_id = _reference(db, "AGRKB:R5744-4")
        a1 = _author(db, ref_id, 1, "Smith", orcid=ORCID_1)
        a2 = _author(db, ref_id, 2, "Smith", orcid=ORCID_1)

        mod.link_authors_to_persons(db)

        assert _linked(db, a1).person_id is None
        assert _linked(db, a2).person_id is None

    def test_obsolete_person_orcid_is_ignored(self, db):  # noqa
        person_id = _person(db, "AGRKB:P5744-5", "Smith")
        db.add(PersonCrossReferenceModel(person_id=person_id, curie=ORCID_1,
                                         curie_prefix="ORCID", is_obsolete=True))
        db.commit()
        ref_id = _reference(db, "AGRKB:R5744-5")
        author_id = _author(db, ref_id, 1, "Smith", orcid=ORCID_1)

        mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id is None

    def test_skips_when_person_already_an_author_on_paper(self, db):  # noqa
        person_id = _person(db, "AGRKB:P5744-6", "Smith", orcid=ORCID_1)
        ref_id = _reference(db, "AGRKB:R5744-6")
        already = AuthorModel(reference_id=ref_id, author_order=1, name="Smith J",
                              last_name="Smith", person_id=person_id)
        db.add(already)
        db.commit()
        author_id = _author(db, ref_id, 2, "Smith", orcid=ORCID_1)

        counts = mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id is None
        assert counts[f"{mod.ORCID_USER}_skipped_person_on_paper"] == 1

    def test_absorbs_person_only_stub(self, db):  # noqa
        person_id = _person(db, "AGRKB:P5744-7", "Smith", orcid=ORCID_1)
        ref_id = _reference(db, "AGRKB:R5744-7")
        stub = AuthorModel(reference_id=ref_id, person_id=person_id)
        db.add(stub)
        db.commit()
        stub_id = stub.author_id
        author_id = _author(db, ref_id, 1, "Smith", orcid=ORCID_1)

        mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id == person_id
        assert db.query(AuthorModel).filter(AuthorModel.author_id == stub_id).one_or_none() is None

    def test_does_not_relink_after_curator_unlinked(self, db):  # noqa
        person_id = _person(db, "AGRKB:P5744-8", "Smith", orcid=ORCID_1)
        ref_id = _reference(db, "AGRKB:R5744-8")
        author_id = _author(db, ref_id, 1, "Smith", orcid=ORCID_1)
        mod.link_authors_to_persons(db)
        author = _linked(db, author_id)
        assert author.person_id == person_id
        author.person_id = None
        db.commit()

        counts = mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id is None
        assert counts[f"{mod.ORCID_USER}_skipped_previously_unlinked"] == 1

    def test_dry_run_changes_nothing(self, db):  # noqa
        _person(db, "AGRKB:P5744-9", "Smith", orcid=ORCID_1)
        ref_id = _reference(db, "AGRKB:R5744-9")
        author_id = _author(db, ref_id, 1, "Smith", orcid=ORCID_1)

        counts = mod.link_authors_to_persons(db, dry_run=True)

        assert counts[f"{mod.ORCID_USER}_linked"] == 1
        assert _linked(db, author_id).person_id is None


class TestEmailPass:

    def test_links_on_email_and_last_name(self, db):  # noqa
        person_id = _person(db, "AGRKB:P5744-11", "Smith", emails=["J.Smith@Example.org"])
        ref_id = _reference(db, "AGRKB:R5744-11")
        author_id = _author(db, ref_id, 1, "Smith", email=" j.smith@example.org")

        counts = mod.link_authors_to_persons(db)

        author = _linked(db, author_id)
        assert author.person_id == person_id
        assert author.updated_by == mod.EMAIL_USER
        assert counts[f"{mod.EMAIL_USER}_linked"] == 1

    def test_no_link_when_last_name_differs(self, db):  # noqa
        _person(db, "AGRKB:P5744-12", "Smith", emails=["lab@example.org"])
        ref_id = _reference(db, "AGRKB:R5744-12")
        author_id = _author(db, ref_id, 1, "Jones", email="lab@example.org")

        mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id is None

    def test_no_link_when_email_belongs_to_two_persons(self, db):  # noqa
        _person(db, "AGRKB:P5744-13a", "Smith", emails=["lab@example.org"])
        _person(db, "AGRKB:P5744-13b", "Smith", emails=["lab@example.org"])
        ref_id = _reference(db, "AGRKB:R5744-13")
        author_id = _author(db, ref_id, 1, "Smith", email="lab@example.org")

        counts = mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id is None
        assert counts["email_ambiguous_person"] == 1

    def test_no_link_when_email_shared_by_two_authors_on_paper(self, db):  # noqa
        _person(db, "AGRKB:P5744-14", "Smith", emails=["lab@example.org"])
        ref_id = _reference(db, "AGRKB:R5744-14")
        a1 = _author(db, ref_id, 1, "Smith", email="lab@example.org")
        a2 = _author(db, ref_id, 2, "Smith", email="lab@example.org")

        mod.link_authors_to_persons(db)

        assert _linked(db, a1).person_id is None
        assert _linked(db, a2).person_id is None

    def test_no_email_link_when_author_orcid_matches_another_person(self, db):  # noqa
        # ORCID resolves to a person (last name differs, so no ORCID link); the
        # email pass must not then link the author to someone else.
        _person(db, "AGRKB:P5744-15a", "Jones", orcid=ORCID_1)
        _person(db, "AGRKB:P5744-15b", "Smith", emails=["smith@example.org"])
        ref_id = _reference(db, "AGRKB:R5744-15")
        author_id = _author(db, ref_id, 1, "Smith", orcid=ORCID_1, email="smith@example.org")

        mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id is None

    def test_no_email_link_when_person_has_a_different_orcid(self, db):  # noqa
        _person(db, "AGRKB:P5744-16", "Smith", orcid=ORCID_2, emails=["smith@example.org"])
        ref_id = _reference(db, "AGRKB:R5744-16")
        author_id = _author(db, ref_id, 1, "Smith", orcid=ORCID_1, email="smith@example.org")

        counts = mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id is None
        assert counts["email_orcid_conflict"] == 1

    def test_email_link_when_author_orcid_unknown_and_person_has_none(self, db):  # noqa
        person_id = _person(db, "AGRKB:P5744-17", "Smith", emails=["smith@example.org"])
        ref_id = _reference(db, "AGRKB:R5744-17")
        author_id = _author(db, ref_id, 1, "Smith", orcid=ORCID_1, email="smith@example.org")

        mod.link_authors_to_persons(db)

        assert _linked(db, author_id).person_id == person_id

    def test_orcid_pass_wins_over_email_pass(self, db):  # noqa
        person_id = _person(db, "AGRKB:P5744-18", "Smith", orcid=ORCID_1,
                            emails=["smith@example.org"])
        ref_id = _reference(db, "AGRKB:R5744-18")
        author_id = _author(db, ref_id, 1, "Smith", orcid=ORCID_1, email="smith@example.org")

        counts = mod.link_authors_to_persons(db)

        author = _linked(db, author_id)
        assert author.person_id == person_id
        assert author.updated_by == mod.ORCID_USER
        assert counts.get(f"{mod.EMAIL_USER}_linked", 0) == 0


AUTHOR_PERSON_STATES = ["ATP:0000109", "ATP:0000377", "ATP:0000376", "ATP:0000378"]


def _author_person_status(db, ref_id):  # noqa
    db.expire_all()
    row = db.query(WorkflowTagModel).filter(
        WorkflowTagModel.reference_id == ref_id,
        WorkflowTagModel.workflow_tag_id.in_(AUTHOR_PERSON_STATES)).one()
    return row.workflow_tag_id


@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       load_name_to_atp_and_relationships_mock)
class TestAuthorPersonStatus:
    """SCRUM-6827: a paper whose last unlinked author the linker links moves to
    author-person curation complete."""

    def _wb_paper(self, db, curie, status):  # noqa
        populate_test_mods()
        mod = db.query(ModModel).filter_by(abbreviation="WB").one()
        if not db.query(WorkflowTransitionModel).filter_by(mod_id=mod.mod_id).first():
            for a in AUTHOR_PERSON_STATES:
                for b in AUTHOR_PERSON_STATES:
                    if a != b:
                        db.add(WorkflowTransitionModel(mod_id=mod.mod_id, transition_from=a,
                                                       transition_to=b, transition_type="any", actions=[]))
        ref_id = _reference(db, curie)
        db.add(ModCorpusAssociationModel(reference_id=ref_id, mod_id=mod.mod_id, corpus=True,
                                         mod_corpus_sort_source="manual_creation"))
        db.add(WorkflowTagModel(reference_id=ref_id, mod_id=mod.mod_id, workflow_tag_id=status))
        db.commit()
        return ref_id

    def test_linking_the_last_author_completes(self, db):  # noqa
        load_name_to_atp_and_relationships_mock()
        _person(db, "AGRKB:P6827-1", "Smith", orcid=ORCID_1)
        ref_id = self._wb_paper(db, "AGRKB:R6827-1", "ATP:0000376")
        _author(db, ref_id, 1, "Smith", orcid=ORCID_1)
        counts = mod.link_authors_to_persons(db)
        assert counts["author_person_status_to_ATP:0000378"] == 1
        assert _author_person_status(db, ref_id) == "ATP:0000378"

    def test_an_author_left_unlinked_keeps_the_status(self, db):  # noqa
        load_name_to_atp_and_relationships_mock()
        _person(db, "AGRKB:P6827-2", "Smith", orcid=ORCID_2)
        ref_id = self._wb_paper(db, "AGRKB:R6827-2", "ATP:0000109")
        _author(db, ref_id, 1, "Smith", orcid=ORCID_2)
        _author(db, ref_id, 2, "Jones")
        mod.link_authors_to_persons(db)
        assert _author_person_status(db, ref_id) == "ATP:0000109"
