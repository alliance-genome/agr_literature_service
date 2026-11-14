"""SCRUM-6827: author-person curation status kept in step with automation."""
from unittest.mock import patch

import pytest
from sqlalchemy import text

from agr_literature_service.api.crud.utils import author_person_status as aps
from agr_literature_service.lit_processing.data_ingest.utils import author_person_status_updates as apsu
from agr_literature_service.api.models import (
    AuthorModel,
    ModCorpusAssociationModel,
    ModModel,
    PersonModel,
    ReferenceModel,
    UserModel,
    WorkflowTagModel,
    WorkflowTransitionModel,
)
from agr_literature_service.api.user import (
    _set_current_user,
    get_current_agent_id,
    get_global_user_id,
    set_global_user_id,
)
from agr_literature_service.lit_processing.data_ingest.utils.db_write_utils import update_authors
from agr_literature_service.lit_processing.tests.mod_populate_load import populate_test_mods
from ....fixtures import db, load_name_to_atp_and_relationships_mock  # noqa

NEEDED = "ATP:0000109"
IN_PROGRESS = "ATP:0000377"
BLOCKED = "ATP:0000376"
COMPLETE = "ATP:0000378"
STATES = [NEEDED, IN_PROGRESS, BLOCKED, COMPLETE]
AUTOMATION = "author_person_status_test"


@pytest.fixture
def wb(db):  # noqa
    """WB with the 12 author-person transitions, all of type 'any'."""
    populate_test_mods()
    mod = db.query(ModModel).filter_by(abbreviation="WB").one()
    for from_atp in STATES:
        for to_atp in STATES:
            if from_atp != to_atp:
                db.add(WorkflowTransitionModel(mod_id=mod.mod_id, transition_from=from_atp,
                                               transition_to=to_atp, transition_type="any", actions=[]))
    db.commit()
    saved = (get_global_user_id(), get_current_agent_id())
    set_global_user_id(db, AUTOMATION)
    yield mod
    _set_current_user(*saved)


def _reference(db, mod, n, status=None, corpus=True):  # noqa
    ref = ReferenceModel(curie=f"AGRKB:APS-{n}", category="research_article")
    db.add(ref)
    db.commit()
    db.add(ModCorpusAssociationModel(reference_id=ref.reference_id, mod_id=mod.mod_id,
                                     corpus=corpus, mod_corpus_sort_source="manual_creation"))
    if status:
        db.add(WorkflowTagModel(reference_id=ref.reference_id, mod_id=mod.mod_id, workflow_tag_id=status))
    db.commit()
    return ref.reference_id


def _person(db, n):  # noqa
    person = PersonModel(display_name=f"Person {n}", curie=f"AGRKB:APSP-{n}")
    db.add(person)
    db.commit()
    return person.person_id


def _curator(db, n):  # noqa
    """A person-backed user; returns the label created_by / updated_by take."""
    curie = f"AGRKB:APSC-{n}"
    person = PersonModel(display_name="Curator", curie=curie)
    db.add(person)
    db.commit()
    db.add(UserModel(person_id=person.person_id))
    db.commit()
    return curie


def _author(db, reference_id, order, person_id=None, by=None):  # noqa
    extra = {"created_by": by, "updated_by": by} if by else {}
    if order is None:
        # a person who is not an author: ck_person_only_link_only allows nothing else
        db.add(AuthorModel(reference_id=reference_id, person_id=person_id, **extra))
    else:
        db.add(AuthorModel(reference_id=reference_id, author_order=order, name=f"Author {order}",
                           last_name=f"Author{order}", person_id=person_id, **extra))
    db.commit()


def _status(db, reference_id):  # noqa
    db.expire_all()
    row = db.query(WorkflowTagModel).filter(WorkflowTagModel.reference_id == reference_id,
                                            WorkflowTagModel.workflow_tag_id.in_(STATES)).one_or_none()
    return row.workflow_tag_id if row else None


def _history(db, reference_id, atp):  # noqa
    return db.execute(text("SELECT count(*) FROM workflow_tag_version "
                           "WHERE reference_id = :r AND workflow_tag_id = :a"),
                      {"r": reference_id, "a": atp}).scalar()


@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       load_name_to_atp_and_relationships_mock)
class TestChecks:

    def test_all_authors_connected(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        empty = _reference(db, wb, 1)
        assert aps.all_authors_connected(db, empty) is True
        ref = _reference(db, wb, 2)
        _author(db, ref, 1, _person(db, 1))
        assert aps.all_authors_connected(db, ref) is True
        _author(db, ref, 2)
        assert aps.all_authors_connected(db, ref) is False

    def test_a_person_who_is_not_an_author_does_not_count(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 3)
        _author(db, ref, 1, _person(db, 3))
        _author(db, ref, None, _person(db, 4))
        assert aps.all_authors_connected(db, ref) is True

    def test_curator_connected(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        curator = _curator(db, 1)
        automated = _reference(db, wb, 4)
        _author(db, automated, 1, _person(db, 5))
        assert apsu.curator_connected(db, automated) is False
        # a curator edited an author but connected nobody
        edited = _reference(db, wb, 5)
        _author(db, edited, 1, by=curator)
        assert apsu.curator_connected(db, edited) is False
        linked = _reference(db, wb, 6)
        _author(db, linked, 1, _person(db, 6), by=curator)
        assert apsu.curator_connected(db, linked) is True

    def test_sort_grants_complete_only_when_all_connected(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 7)
        _author(db, ref, 1)
        assert [t[1] for t in aps.author_person_corpus_entry_tags(db, ref, "WB")] == [NEEDED]
        empty = _reference(db, wb, 8)
        assert [t[1] for t in aps.author_person_corpus_entry_tags(db, empty, "WB")] == [COMPLETE]


@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       load_name_to_atp_and_relationships_mock)
class TestCompleteWhenAllConnected:

    def test_complete_from_any_state_without_history(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        refs = []
        for n, state in enumerate([NEEDED, IN_PROGRESS, BLOCKED], start=10):
            ref = _reference(db, wb, n, status=state)
            _author(db, ref, 1, _person(db, n))
            refs.append(ref)
        before = [_history(db, ref, COMPLETE) for ref in refs]
        apsu.complete_when_all_connected(db, refs)
        assert [_status(db, ref) for ref in refs] == [COMPLETE] * 3
        assert [_history(db, ref, COMPLETE) for ref in refs] == before
        assert 'versioning' not in WorkflowTagModel.__versioned__

    def test_history_written_when_a_curator_connected_an_author(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        curator = _curator(db, 2)
        ref = _reference(db, wb, 20, status=NEEDED)
        _author(db, ref, 1, _person(db, 20), by=curator)
        _author(db, ref, 2, _person(db, 21))
        apsu.complete_when_all_connected(db, [ref])
        assert _status(db, ref) == COMPLETE
        assert _history(db, ref, COMPLETE) == 1

    def test_not_all_connected_or_out_of_corpus_left_alone(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        partial = _reference(db, wb, 30, status=NEEDED)
        _author(db, partial, 1, _person(db, 30))
        _author(db, partial, 2)
        outside = _reference(db, wb, 31, status=NEEDED, corpus=False)
        _author(db, outside, 1, _person(db, 31))
        untagged = _reference(db, wb, 32)
        _author(db, untagged, 1, _person(db, 32))
        apsu.complete_when_all_connected(db, [partial, outside, untagged])
        assert [_status(db, partial), _status(db, outside), _status(db, untagged)] == [NEEDED, NEEDED, None]

    def test_dry_run_changes_nothing(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 40, status=NEEDED)
        _author(db, ref, 1, _person(db, 40))
        counts = apsu.complete_when_all_connected(db, [ref], dry_run=True)
        assert counts == {f"author_person_status_to_{COMPLETE}": 1}
        assert _status(db, ref) == NEEDED

    def test_refuses_uncommitted_changes(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 41, status=NEEDED)
        db.add(WorkflowTagModel(reference_id=ref, mod_id=wb.mod_id, workflow_tag_id="ATP:0000141"))
        with pytest.raises(RuntimeError):
            apsu.set_author_person_status(db, ref, "WB", COMPLETE, record_history=False)
        db.rollback()

    def test_only_workflow_tag_history_is_switched_off(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 42, status=NEEDED)
        # a pending author change is not refused: it keeps its history
        db.add(AuthorModel(reference_id=ref, author_order=1, name="Pending", person_id=_person(db, 42)))
        apsu.set_author_person_status(db, ref, "WB", COMPLETE, record_history=False)
        assert _status(db, ref) == COMPLETE
        assert _history(db, ref, COMPLETE) == 0
        assert db.execute(text("SELECT count(*) FROM author_version WHERE reference_id = :r"),
                          {"r": ref}).scalar() == 1
        assert 'versioning' not in WorkflowTagModel.__versioned__


def _json_authors(*names):
    return [{"name": name, "firstname": name.split()[0], "lastname": name.split()[1],
             "firstinit": name[0], "authorRank": rank} for rank, name in enumerate(names, start=1)]


def _db_authors(db, reference_id):  # noqa
    return [{"name": row.name, "first_name": row.first_name, "last_name": row.last_name,
             "first_initial": row.first_initial, "author_order": row.author_order}
            for row in db.query(AuthorModel).filter_by(reference_id=reference_id)
            .order_by(AuthorModel.author_order)]


@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       load_name_to_atp_and_relationships_mock)
class TestAuthorReload:

    def _reload(self, db, reference_id, *names, record_history=False):  # noqa
        update_authors(db, reference_id, _db_authors(db, reference_id), _json_authors(*names),
                       "x", {}, None, None, None, None)
        db.commit()
        return apsu.apply_author_reload_statuses(db, record_history=record_history)

    def test_complete_goes_back_to_needed_without_history(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 50, status=COMPLETE)
        _author(db, ref, 1, _person(db, 50), by=AUTOMATION)
        self._reload(db, ref, "Xx X", "Yy Y")
        assert _status(db, ref) == NEEDED
        assert _history(db, ref, NEEDED) == 0

    def test_curator_action_keeps_history(self, db, wb):  # noqa
        # a reference merge in the API reloads with record_history=True
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 55, status=COMPLETE)
        _author(db, ref, 1, _person(db, 55), by=AUTOMATION)
        self._reload(db, ref, "Xx X", record_history=True)
        assert _status(db, ref) == NEEDED
        assert _history(db, ref, NEEDED) == 1

    def test_other_states_are_left_for_the_curator(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 51, status=BLOCKED)
        _author(db, ref, 1, by=AUTOMATION)
        self._reload(db, ref, "Xx X")
        assert _status(db, ref) == BLOCKED

    def test_reload_to_no_authors_completes(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 52, status=IN_PROGRESS)
        _author(db, ref, 1, by=AUTOMATION)
        self._reload(db, ref)
        assert _status(db, ref) == COMPLETE

    def test_no_reload_no_change(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        curator = _curator(db, 3)
        ref = _reference(db, wb, 53, status=COMPLETE)
        _author(db, ref, 1, _person(db, 53), by=curator)
        # a curator connected this author, so the update does not reload the paper
        self._reload(db, ref, "Xx X")
        assert _status(db, ref) == COMPLETE

    def test_rolled_back_reload_is_discarded(self, db, wb):  # noqa
        load_name_to_atp_and_relationships_mock()
        ref = _reference(db, wb, 54, status=COMPLETE)
        _author(db, ref, 1, _person(db, 54), by=AUTOMATION)
        update_authors(db, ref, _db_authors(db, ref), _json_authors("Xx X"), "x", {}, None, None, None, None)
        db.rollback()
        apsu.discard_author_reload_marks(db)
        assert apsu.apply_author_reload_statuses(db) == {}
        assert _status(db, ref) == COMPLETE
