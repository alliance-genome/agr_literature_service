"""SCRUM-6729: created_by / updated_by identifiers are resolved by their shape. Person curies and cross
references (WB:WBPerson..., bare WBPerson..., ORCID:...) resolve to the person; a WB person not in ABC yet keeps
one placeholder automation agent; an unknown person curie or email is a 422, never a made-up agent."""
import importlib.util
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import HTTPException

from agr_literature_service.api.crud.user_utils import claim_wb_placeholder, map_to_existing_user_id, \
    map_to_user_id, resolve_named_agent, wb_person_placeholder
from agr_literature_service.api.models import PersonCrossReferenceModel, PersonEmailModel, PersonModel
from agr_literature_service.api.models.user_model import UserModel
from ..fixtures import db  # noqa

_SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6729_claim_wb_person_placeholders.py"
)
_spec = importlib.util.spec_from_file_location("claim_wb_person_placeholders", _SCRIPT)
assert _spec is not None and _spec.loader is not None
script = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(script)


def _person(db, curie, name, xref=None, email=None, with_agent=False):  # noqa
    """Get-or-create a person, optionally with a cross reference, an email and an agent; persons and agents
    survive the per-test cleanup."""
    person = db.query(PersonModel).filter(PersonModel.curie == curie).one_or_none()
    if person is None:
        person = PersonModel(display_name=name, curie=curie)
        db.add(person)
        db.flush()
        if xref:
            db.add(PersonCrossReferenceModel(curie=xref, curie_prefix=xref.split(":")[0],
                                             person_id=person.person_id))
        if email:
            db.add(PersonEmailModel(person_id=person.person_id, email_address=email))
        if with_agent:
            db.add(UserModel(person_id=person.person_id))
        db.commit()
    return person.person_id


def _placeholder(db, name):  # noqa
    agent = db.query(UserModel).filter(UserModel.automation_username == name).one_or_none()
    if agent is None:
        agent = UserModel(automation_username=name)
        db.add(agent)
        db.commit()
    return agent.user_id


def _person_agents(db, person_id):  # noqa
    return sorted(agent_id for (agent_id,) in db.query(UserModel.user_id).filter(UserModel.person_id == person_id))


def _named(db, name):  # noqa
    return db.query(UserModel).filter(UserModel.automation_username.ilike(name)).count()


def test_wb_person_placeholder():
    assert wb_person_placeholder("WBPerson1843") == "WBPerson1843"
    assert wb_person_placeholder(" wb:wbperson1843 ") == "WBPerson1843"
    assert wb_person_placeholder("WBPersonundefined") is None
    assert wb_person_placeholder("ORCID:0000-0001-6729-0001") is None


def test_wb_person_without_agent_claims_the_placeholder(db):  # noqa
    person_id = _person(db, "AGRKB:105006729001", "Wb Curatorone", xref="WB:WBPerson672901")
    placeholder_id = _placeholder(db, "WBPerson672901")
    agents_before = db.query(UserModel).count()

    assert map_to_user_id("WBPerson672901", db) == "AGRKB:105006729001"
    db.commit()
    # the placeholder became the person's agent: no new agent, no name left
    assert _person_agents(db, person_id) == [placeholder_id]
    assert _named(db, "WBPerson672901") == 0
    assert db.query(UserModel).count() == agents_before
    # the prefixed form and later uses resolve to the same agent
    assert map_to_user_id("WB:WBPerson672901", db) == "AGRKB:105006729001"
    assert _person_agents(db, person_id) == [placeholder_id]


def test_wb_person_with_agent_keeps_the_placeholder_as_a_second_agent(db):  # noqa
    person_id = _person(db, "AGRKB:105006729002", "Wb Curatortwo", xref="WB:WBPerson672902", with_agent=True)
    (person_agent_id,) = _person_agents(db, person_id)
    placeholder_id = _placeholder(db, "WBPerson672902")
    agents_before = db.query(UserModel).count()

    assert map_to_user_id("WB:WBPerson672902", db) == "AGRKB:105006729002"
    db.commit()
    assert _person_agents(db, person_id) == sorted([person_agent_id, placeholder_id])
    assert _named(db, "WBPerson672902") == 0
    assert db.query(UserModel).count() == agents_before
    # new writes go to the person's first agent
    agent_id, label = resolve_named_agent("WBPerson672902", db)
    assert (agent_id, label) == (person_agent_id, "AGRKB:105006729002")


def test_wb_person_not_in_abc_reuses_one_placeholder(db):  # noqa
    _placeholder(db, "WBPerson672903")
    # an existing placeholder is reused, whatever the case or prefix
    assert map_to_user_id("wbperson672903", db) == "WBPerson672903"
    assert map_to_user_id("WB:WBPerson672903", db) == "WBPerson672903"
    assert map_to_existing_user_id("WBPerson672903", db) == "WBPerson672903"
    assert _named(db, "WBPerson672903") == 1
    # a WB person id never seen before gets its bare form, which the audit listener creates
    assert map_to_user_id("WB:wbperson672904", db) == "WBPerson672904"
    assert map_to_existing_user_id("WBPerson672904", db) is None


def test_person_curies_and_cross_references(db):  # noqa
    person_id = _person(db, "AGRKB:105006729005", "Orcid Persone", xref="ORCID:0000-0001-6729-0005")
    assert _person_agents(db, person_id) == []
    # by cross reference, any case: the person gets one agent, reused afterwards
    assert map_to_user_id("orcid:0000-0001-6729-0005", db) == "AGRKB:105006729005"
    assert map_to_user_id("AGRKB:105006729005", db) == "AGRKB:105006729005"
    db.commit()
    assert len(_person_agents(db, person_id)) == 1
    # an unknown person curie or cross reference is refused, never made an agent
    for unknown in ("ORCID:0000-0001-6729-9999", "AGRKB:105006729999", "SGD:Colleague_6729"):
        with pytest.raises(HTTPException) as exc:
            map_to_user_id(unknown, db)
        assert exc.value.status_code == 422
        assert map_to_existing_user_id(unknown, db) is None
        assert _named(db, unknown) == 0


def test_email(db):  # noqa
    person_id = _person(db, "AGRKB:105006729006", "Email Persone", email="email.6729a@example.org")
    assert map_to_user_id("EMAIL.6729a@example.org", db) == "AGRKB:105006729006"
    db.commit()
    assert len(_person_agents(db, person_id)) == 1
    _person(db, "AGRKB:105006729007", "Shared Emailone", email="shared.6729@example.org")
    _person(db, "AGRKB:105006729008", "Shared Emailtwo", email="shared.6729@example.org")
    for email in ("shared.6729@example.org", "nobody.6729@example.org"):
        with pytest.raises(HTTPException) as exc:
            map_to_user_id(email, db)
        assert exc.value.status_code == 422


def test_automation_names_unchanged(db):  # noqa
    _placeholder(db, "load_something_6729")
    assert map_to_user_id("LOAD_SOMETHING_6729", db) == "load_something_6729"
    # a new script name is still echoed back for the audit listener to create
    assert map_to_user_id("new_script_6729", db) == "new_script_6729"
    assert map_to_user_id("WBPersonundefined", db) == "WBPersonundefined"


def test_claim_script(db):  # noqa
    person_id = _person(db, "AGRKB:105006729009", "Wb Curatornine", xref="WB:WBPerson672909")
    placeholder_id = _placeholder(db, "WBPerson672909")
    _placeholder(db, "WBPerson672910")  # not in ABC
    assert script.form_rows_left(db) == 0

    # dry run: listed, nothing written
    result = script.claim_placeholders(db)
    db.rollback()
    assert "WBPerson672909 -> AGRKB:105006729009" in result["claimed"]
    assert "WBPerson672910" in result["not in ABC"]
    assert _named(db, "WBPerson672909") == 1

    result = script.claim_placeholders(db, apply=True)
    assert "WBPerson672909 -> AGRKB:105006729009" in result["claimed"]
    assert _person_agents(db, person_id) == [placeholder_id]
    assert _named(db, "WBPerson672910") == 1
    # nothing left to claim
    assert "WBPerson672909 -> AGRKB:105006729009" not in script.claim_placeholders(db)["claimed"]
    assert claim_wb_placeholder(db, "WBPerson672910") is None
