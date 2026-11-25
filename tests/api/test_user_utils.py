"""SCRUM-6729: created_by / updated_by identifiers are resolved by their shape. Person curies and cross
references (WB:WBPerson..., bare WBPerson..., ORCID:...) resolve to the person; a WB person not in ABC yet keeps
one placeholder automation agent; an unknown person curie or email is a 422, never a made-up agent.
SCRUM-6995: a person is its own agent, and the claim script merges a placeholder into its person."""
import importlib.util
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi import HTTPException

from agr_literature_service.api.crud.user_utils import claim_wb_placeholder, map_to_existing_user_id, \
    map_to_user_id, resolve_named_agent, wb_person_placeholder
from agr_literature_service.api.models import PersonCrossReferenceModel, PersonEmailModel, PersonModel, \
    PersonNoteModel
from agr_literature_service.api.models.user_model import AutomationModel, UserModel
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


def _person(db, curie, name, xref=None, email=None):  # noqa
    """Get-or-create a person, optionally with a cross reference and an email; a person is an agent
    (SCRUM-6995) and its agent row survives the per-test cleanup."""
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
        db.commit()
    return person.person_id


def _placeholder(db, name):  # noqa
    agent = db.query(AutomationModel).filter(AutomationModel.display_name == name).one_or_none()
    if agent is None:
        agent = AutomationModel(display_name=name)
        db.add(agent)
        db.commit()
    return agent.id


def _named(db, name):  # noqa
    return db.query(AutomationModel).filter(AutomationModel.display_name.ilike(name)).count()


def test_wb_person_placeholder():
    assert wb_person_placeholder("WBPerson1843") == "WBPerson1843"
    assert wb_person_placeholder(" wb:wbperson1843 ") == "WBPerson1843"
    assert wb_person_placeholder("WBPersonundefined") is None
    assert wb_person_placeholder("ORCID:0000-0001-6729-0001") is None


def test_wb_person_resolves_to_the_person_and_leaves_the_placeholder(db):  # noqa
    person_id = _person(db, "AGRKB:105006729001", "Wb Curatorone", xref="WB:WBPerson672901")
    _placeholder(db, "WBPerson672901")
    agents_before = db.query(UserModel).count()

    # any form of the WB person id resolves to the person, which is an agent itself
    assert map_to_user_id("WBPerson672901", db) == "AGRKB:105006729001"
    assert map_to_user_id("WB:WBPerson672901", db) == "AGRKB:105006729001"
    assert resolve_named_agent("WBPerson672901", db) == (person_id, "AGRKB:105006729001")
    db.commit()
    # no agent is created, and the placeholder keeps its records until the claim script merges it
    assert db.query(UserModel).count() == agents_before
    assert _named(db, "WBPerson672901") == 1


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
    _person(db, "AGRKB:105006729005", "Orcid Persone", xref="ORCID:0000-0001-6729-0005")
    agents_before = db.query(UserModel).count()
    # by cross reference, any case: the person, with no agent created
    assert map_to_user_id("orcid:0000-0001-6729-0005", db) == "AGRKB:105006729005"
    assert map_to_user_id("AGRKB:105006729005", db) == "AGRKB:105006729005"
    db.commit()
    assert db.query(UserModel).count() == agents_before
    # an unknown person curie or cross reference is refused, never made an agent
    for unknown in ("ORCID:0000-0001-6729-9999", "AGRKB:105006729999", "SGD:Colleague_6729"):
        with pytest.raises(HTTPException) as exc:
            map_to_user_id(unknown, db)
        assert exc.value.status_code == 422
        assert map_to_existing_user_id(unknown, db) is None
        assert _named(db, unknown) == 0


def test_email(db):  # noqa
    _person(db, "AGRKB:105006729006", "Email Persone", email="email.6729a@example.org")
    assert map_to_user_id("EMAIL.6729a@example.org", db) == "AGRKB:105006729006"
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
    # a record the placeholder made, about another person
    note = PersonNoteModel(person_id=_person(db, "AGRKB:105006729011", "Note Subject"), note="by placeholder",
                           created_by="WBPerson672909")
    db.add(note)
    db.commit()
    note_id = note.person_note_id
    assert note.created_by_id == placeholder_id
    assert script.form_rows_left(db) == 0

    # dry run: listed, nothing written
    result = script.claim_placeholders(db)
    db.rollback()
    assert "WBPerson672909 -> AGRKB:105006729009" in result["claimed"]
    assert "WBPerson672910" in result["not in ABC"]
    assert _named(db, "WBPerson672909") == 1

    result = script.claim_placeholders(db, apply=True)
    assert "WBPerson672909 -> AGRKB:105006729009" in result["claimed"]
    # the placeholder is merged into the person: gone, its record now the person's
    assert _named(db, "WBPerson672909") == 0
    assert db.get(UserModel, placeholder_id) is None
    note = db.get(PersonNoteModel, note_id)
    assert (note.created_by_id, note.created_by) == (person_id, "AGRKB:105006729009")
    assert _named(db, "WBPerson672910") == 1
    # nothing left to claim
    assert "WBPerson672909 -> AGRKB:105006729009" not in script.claim_placeholders(db)["claimed"]
    assert claim_wb_placeholder(db, "WBPerson672910") is None


def test_display_name_matches_only_one_person(db):  # noqa
    _person(db, "AGRKB:105006729012", "Uniquename Persontwelve")
    assert map_to_user_id("Uniquename Persontwelve", db) == "AGRKB:105006729012"
    assert map_to_user_id("U. Persontwelve", db) == "AGRKB:105006729012"
    # a name two persons share is no match: it is echoed back like any unknown name
    _person(db, "AGRKB:105006729013", "Twin Namesake")
    _person(db, "AGRKB:105006729014", "Twin Namesake")
    assert map_to_user_id("Twin Namesake", db) == "Twin Namesake"
