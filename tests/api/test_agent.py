"""The agent table and the current-user layer (SCRUM-6719: users -> agent, integer id).

An agent's label (automation_username, or the curie of its person) is what the API shows as
created_by / updated_by; resolve_agent_id turns a label back into agent.id, creating an
automation agent for a label it has never seen.
"""
import pytest
from sqlalchemy import text

from agr_literature_service.api import user as user_mod
from agr_literature_service.api.models import CopyrightLicenseModel, PersonModel
from agr_literature_service.api.models.user_model import UserModel
from agr_literature_service.api.user import (
    get_current_agent_id, get_current_user_pk, get_global_user_id, link_user_to_person, resolve_agent_id,
    set_global_user_id)

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
    assert not hasattr(UserModel, "id")


def test_resolve_agent_id_returns_existing_automation_agent(db):  # noqa
    first = resolve_agent_id(db.connection(), "AGENT_TEST_SCRIPT_A")
    assert resolve_agent_id(db.connection(), "AGENT_TEST_SCRIPT_A") == first
    assert db.query(UserModel).filter_by(automation_username="AGENT_TEST_SCRIPT_A").count() == 1


def test_resolve_agent_id_creates_unknown_label(db):  # noqa
    agent_id = resolve_agent_id(db.connection(), "AGENT_TEST_NEVER_SEEN")
    row = db.execute(text("SELECT automation_username, person_id FROM agent WHERE id = :i"), {"i": agent_id}).one()
    assert tuple(row) == ("AGENT_TEST_NEVER_SEEN", None)


def test_resolve_agent_id_finds_person_by_curie(db):  # noqa
    person = _person(db, "AGRKB:AGENT-TEST-1")
    link_user_to_person(db, "AGENT_TEST_CURATOR", person.person_id)
    agent_id = resolve_agent_id(db.connection(), "AGRKB:AGENT-TEST-1")
    assert db.get(UserModel, agent_id).person_id == person.person_id
    assert db.query(UserModel).filter_by(person_id=person.person_id).count() == 1


def test_resolve_agent_id_is_case_sensitive(db):  # noqa
    lower = resolve_agent_id(db.connection(), "agent_test_case")
    assert resolve_agent_id(db.connection(), "AGENT_TEST_CASE") != lower


def test_set_global_user_id_records_label_and_agent_id(db):  # noqa
    set_global_user_id(db, "AGENT_TEST_GLOBAL")
    assert get_global_user_id() == "AGENT_TEST_GLOBAL"
    agent = db.query(UserModel).filter_by(automation_username="AGENT_TEST_GLOBAL").one()
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
