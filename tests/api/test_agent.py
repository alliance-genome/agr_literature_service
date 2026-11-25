"""The agent table and the current-user layer (SCRUM-6719: users -> agent, integer id;
SCRUM-6995: agent is the parent table of person and automation, which share its id).

An agent's label (the curie of a person, or an automation's display_name) is what the API
shows as created_by / updated_by; resolve_agent_id turns a label back into agent.id, creating
an automation agent for a label it has never seen.
"""
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from agr_literature_service.api import user as user_mod
from agr_literature_service.api.models import CopyrightLicenseModel, PersonModel, PersonNoteModel
from agr_literature_service.api.models.user_model import AutomationModel, UserModel
from agr_literature_service.api.user import (
    get_current_agent_id, get_current_user_pk, get_global_user_id, resolve_agent_id, set_global_user_id)

from ..fixtures import db  # noqa: F401


@pytest.fixture(autouse=True)
def _clear_global_user():
    user_token = user_mod._current_user_id.set(None)
    agent_token = user_mod._current_agent_id.set(None)
    try:
        yield
    finally:
        user_mod._current_agent_id.reset(agent_token)
        user_mod._current_user_id.reset(user_token)


def _person(db, curie):  # noqa
    person = PersonModel(display_name=f"Agent Test {curie}", curie=curie)
    db.add(person)
    db.flush()
    return person


def test_agent_table_has_integer_id(db):  # noqa
    assert UserModel.__tablename__ == "agent"
    assert [column.name for column in UserModel.__table__.primary_key] == ["id"]
    assert [column.name for column in PersonModel.__table__.primary_key] == ["id"]
    assert [column.name for column in AutomationModel.__table__.primary_key] == ["id"]


def test_person_is_an_agent(db):  # noqa
    person = _person(db, "AGRKB:AGENT-TEST-IS-AGENT")
    db.expire_all()
    agent = db.get(UserModel, person.person_id)
    assert isinstance(agent, PersonModel)
    assert agent.agent_type == "person"
    assert agent.user_id == agent.person_id == agent.id
    assert agent.display_name == "Agent Test AGRKB:AGENT-TEST-IS-AGENT"


def test_automation_agent(db):  # noqa
    automation = AutomationModel(display_name="agent_test_pipeline_x")
    db.add(automation)
    db.flush()
    db.expire_all()
    agent = db.get(UserModel, automation.id)
    assert isinstance(agent, AutomationModel)
    assert agent.agent_type == "automation"
    assert resolve_agent_id(db.connection(), "agent_test_pipeline_x") == automation.id


def test_person_label_is_curie(db):  # noqa
    person = _person(db, "AGRKB:AGENT-TEST-LABEL")
    note = PersonNoteModel(person_id=person.person_id, note="label test", created_by="AGRKB:AGENT-TEST-LABEL")
    db.add(note)
    db.flush()
    db.expire_all()
    note = db.get(PersonNoteModel, note.person_note_id)
    assert note.created_by_id == person.person_id
    assert note.created_by == "AGRKB:AGENT-TEST-LABEL"


def test_automation_display_name_unique(db):  # noqa
    db.add(AutomationModel(display_name="agent_test_dup"))
    db.flush()
    db.add(AutomationModel(display_name="agent_test_dup"))
    with pytest.raises(IntegrityError):
        db.flush()
    db.rollback()


def test_person_display_name_not_unique(db):  # noqa
    first = PersonModel(display_name="Agent Test Same Name", curie="AGRKB:AGENT-TEST-SAME-1")
    second = PersonModel(display_name="Agent Test Same Name", curie="AGRKB:AGENT-TEST-SAME-2")
    db.add_all([first, second])
    db.flush()
    assert first.id != second.id


def test_child_agent_type_fixed(db):  # noqa
    person = _person(db, "AGRKB:AGENT-TEST-FIXED-TYPE")
    db.commit()
    with pytest.raises(IntegrityError):
        db.execute(text("INSERT INTO automation (id) VALUES (:i)"), {"i": person.person_id})
    db.rollback()


def test_person_versioning_records_display_name_on_agent(db):  # noqa
    person = _person(db, "AGRKB:AGENT-TEST-VERSION")
    db.commit()
    person.display_name = "Agent Test Renamed"
    db.commit()
    names = db.execute(text("SELECT display_name FROM agent_version WHERE id = :i ORDER BY transaction_id"),
                       {"i": person.person_id}).scalars().all()
    assert names == ["Agent Test AGRKB:AGENT-TEST-VERSION", "Agent Test Renamed"]
    person_version_columns = db.execute(text(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'person_version'")).scalars().all()
    assert "display_name" not in person_version_columns
    assert "created_by_id" not in person_version_columns


def test_person_delete_blocked_by_audit_reference(db):  # noqa
    person = _person(db, "AGRKB:AGENT-TEST-DELETE")
    other = _person(db, "AGRKB:AGENT-TEST-DELETE-2")
    # a note about another person, created by person: person's agent is referenced by created_by_id
    db.add(PersonNoteModel(person_id=other.person_id, note="audit ref", created_by="AGRKB:AGENT-TEST-DELETE"))
    db.commit()
    db.delete(person)
    with pytest.raises(IntegrityError):
        db.flush()
    db.rollback()


def test_resolve_agent_id_returns_existing_automation_agent(db):  # noqa
    first = resolve_agent_id(db.connection(), "AGENT_TEST_SCRIPT_A")
    assert resolve_agent_id(db.connection(), "AGENT_TEST_SCRIPT_A") == first
    assert db.query(AutomationModel).filter_by(display_name="AGENT_TEST_SCRIPT_A").count() == 1


def test_resolve_agent_id_creates_unknown_label(db):  # noqa
    agent_id = resolve_agent_id(db.connection(), "AGENT_TEST_NEVER_SEEN")
    row = db.execute(text("SELECT a.agent_type, a.display_name, m.id FROM agent a "
                          "JOIN automation m ON m.id = a.id WHERE a.id = :i"), {"i": agent_id}).one()
    assert tuple(row) == ("automation", "AGENT_TEST_NEVER_SEEN", agent_id)


def test_resolve_agent_id_finds_person_by_curie(db):  # noqa
    person = _person(db, "AGRKB:AGENT-TEST-1")
    agent_id = resolve_agent_id(db.connection(), "AGRKB:AGENT-TEST-1")
    assert agent_id == person.person_id
    assert db.query(UserModel).count() == db.query(UserModel).filter(UserModel.id != person.id).count() + 1


def test_resolve_agent_id_is_case_sensitive(db):  # noqa
    lower = resolve_agent_id(db.connection(), "agent_test_case")
    assert resolve_agent_id(db.connection(), "AGENT_TEST_CASE") != lower


def test_set_global_user_id_records_label_and_agent_id(db):  # noqa
    set_global_user_id(db, "AGENT_TEST_GLOBAL")
    assert get_global_user_id() == "AGENT_TEST_GLOBAL"
    agent = db.query(AutomationModel).filter_by(display_name="AGENT_TEST_GLOBAL").one()
    assert get_current_agent_id() == agent.user_id
    assert get_current_user_pk(db) == agent.user_id


def test_current_user_pk_is_none_without_user(db):  # noqa
    assert get_current_user_pk(db) is None
    assert get_current_agent_id() is None


def test_transaction_user_id_set_on_write(db):  # noqa
    set_global_user_id(db, "AGENT_TEST_TX")
    db.add(CopyrightLicenseModel(name="AGENT_TEST_TX_LICENSE", url="https://example.org/license"))
    db.commit()
    last = db.execute(text("SELECT user_id FROM transaction ORDER BY id DESC LIMIT 1")).scalar()
    assert last is not None
    assert last == get_current_agent_id()


def test_register_cognito_user_creates_one_person_agent(db):  # noqa
    cognito_user = {"email": "agent.test.cognito@example.org", "name": "Agent Test Cognito",
                    "cognito:groups": ["SGDCurator"]}
    first = user_mod._register_cognito_user(db, cognito_user, "agent.test.cognito@example.org")
    again = user_mod._register_cognito_user(db, cognito_user, "Agent.Test.Cognito@example.org")
    assert first == again
    person = db.query(PersonModel).filter_by(curie=first[1]).one()
    assert person.id == first[0]
    assert person.agent_type == "person"
    assert person.mod_roles == ["SGDCurator"]
    assert db.query(PersonModel).filter_by(display_name="Agent Test Cognito").count() == 1


def test_register_cognito_user_records_roles_of_preregistered_person(db):  # noqa
    from agr_literature_service.api.models import PersonEmailModel
    person = _person(db, "AGRKB:AGENT-TEST-PREREG")
    db.add(PersonEmailModel(person_id=person.person_id, email_address="agent.test.prereg@example.org"))
    db.commit()
    cognito_user = {"email": "agent.test.prereg@example.org", "cognito:groups": ["WBCurator"]}
    assert user_mod._register_cognito_user(db, cognito_user, "agent.test.prereg@example.org") == \
        (person.person_id, "AGRKB:AGENT-TEST-PREREG")
    db.expire_all()
    assert db.get(PersonModel, person.person_id).mod_roles == ["WBCurator"]


def test_merge_agent_into(db):  # noqa
    from agr_literature_service.api.crud.agent_crud import merge_agent_into
    person = _person(db, "AGRKB:AGENT-TEST-MERGE")
    subject = _person(db, "AGRKB:AGENT-TEST-MERGE-SUBJECT")
    placeholder = AutomationModel(display_name="AGENT_TEST_MERGE_PLACEHOLDER")
    db.add(placeholder)
    db.commit()
    placeholder_id = placeholder.id
    note = PersonNoteModel(person_id=subject.person_id, note="first", created_by="AGENT_TEST_MERGE_PLACEHOLDER")
    db.add(note)
    db.commit()
    note.note = "second"  # a second history row
    note.updated_by = "AGENT_TEST_MERGE_PLACEHOLDER"
    db.commit()
    note_id = note.person_note_id

    counts = merge_agent_into(db, placeholder_id, person.person_id)
    db.commit()
    assert counts["person_note"] == 1
    assert counts["person_note_version"] >= 2
    assert db.get(UserModel, placeholder_id) is None
    assert db.execute(text("SELECT count(*) FROM automation WHERE id = :i"), {"i": placeholder_id}).scalar() == 0
    note = db.get(PersonNoteModel, note_id)
    assert (note.created_by_id, note.updated_by_id) == (person.person_id, person.person_id)
    assert note.created_by == "AGRKB:AGENT-TEST-MERGE"
    leftovers = db.execute(text("SELECT count(*) FROM person_note_version "
                                "WHERE created_by_id = :i OR updated_by_id = :i"), {"i": placeholder_id}).scalar()
    assert leftovers == 0


def test_merge_agent_into_refuses_a_person(db):  # noqa
    from agr_literature_service.api.crud.agent_crud import merge_agent_into
    first = _person(db, "AGRKB:AGENT-TEST-MERGE-P1")
    second = _person(db, "AGRKB:AGENT-TEST-MERGE-P2")
    with pytest.raises(ValueError):
        merge_agent_into(db, first.person_id, second.person_id)


def test_promote_automation_to_person(db):  # noqa
    from agr_literature_service.api.crud.agent_crud import promote_automation_to_person
    automation = AutomationModel(display_name="agent_test_promote_me")
    subject = _person(db, "AGRKB:AGENT-TEST-PROMOTE-SUBJECT")
    db.add(automation)
    db.commit()
    agent_id = automation.id
    note = PersonNoteModel(person_id=subject.person_id, note="by automation", created_by="agent_test_promote_me")
    db.add(note)
    db.commit()
    note_id = note.person_note_id

    person = promote_automation_to_person(db, agent_id, "Promoted Person", "AGRKB:AGENT-TEST-PROMOTED",
                                          mod_roles=["WBCurator"])
    db.commit()
    assert isinstance(person, PersonModel)
    assert (person.id, person.agent_type, person.display_name) == (agent_id, "person", "Promoted Person")
    assert person.mod_roles == ["WBCurator"]
    assert db.execute(text("SELECT count(*) FROM automation WHERE id = :i"), {"i": agent_id}).scalar() == 0
    assert db.get(PersonNoteModel, note_id).created_by == "AGRKB:AGENT-TEST-PROMOTED"
    with pytest.raises(ValueError):
        promote_automation_to_person(db, agent_id, "Again", "AGRKB:AGENT-TEST-PROMOTED-AGAIN")


def test_link_user_to_person_merges_the_automation(db):  # noqa
    from agr_literature_service.api.user import link_user_to_person
    person = _person(db, "AGRKB:AGENT-TEST-LINK")
    automation_id = resolve_agent_id(db.connection(), "AGENT_TEST_LINK_LOGIN")
    db.commit()
    link_user_to_person(db, "AGENT_TEST_LINK_LOGIN", person.person_id)
    assert db.get(UserModel, automation_id) is None
    assert resolve_agent_id(db.connection(), "AGRKB:AGENT-TEST-LINK") == person.person_id
