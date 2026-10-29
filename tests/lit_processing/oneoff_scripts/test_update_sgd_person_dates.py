"""update_sgd_person_dates sets the SGD colleague date on the rows
load_sgd_colleagues created, and keeps the update dates of rows edited since
and of persons that were in ABC before the SGD load."""
from datetime import datetime

from sqlalchemy import text

from agr_literature_service.api.models import LaboratoryCrossReferenceModel, LaboratoryModel, \
    LaboratoryPersonModel, PersonCrossReferenceModel, PersonModel, PersonNameModel
from agr_literature_service.lit_processing.oneoff_scripts.update_sgd_person_dates import update_dates
from ...fixtures import db  # noqa

LOADER = "load_sgd_colleagues_test"
CURATOR = "curator_sgd_dates_test"
D1 = datetime(2000, 5, 19)
D2 = datetime(2007, 3, 1)


def _audited(model, **kwargs):
    return model(created_by=kwargs.pop("created_by", LOADER), updated_by=kwargs.pop("updated_by", LOADER),
                 **kwargs)


def _person(db, n, colleague_id, created_by=LOADER):  # noqa
    person = _audited(PersonModel, display_name=f"Colleague {n}", curie=f"AGRKB:SGD-DATES-{n}",
                      created_by=created_by, updated_by=created_by)
    db.add(person)
    db.flush()
    db.add(_audited(PersonCrossReferenceModel, person_id=person.person_id, curie_prefix="SGD",
                    curie=f"SGD:Colleague_{colleague_id}"))
    db.flush()
    return person.person_id


def _dates(db, table, row_id):  # noqa
    return tuple(db.execute(text(
        f"SELECT date_created, db_date_created, date_updated, db_date_updated FROM {table} WHERE id = :id"),
        {"id": row_id}).one())


def test_update_dates(db):  # noqa
    loaded = _person(db, 1, 9100001)
    pre_existing = _person(db, 2, 9100002, created_by=CURATOR)
    member = _person(db, 3, 9100003)
    edited_name = _audited(PersonNameModel, person_id=loaded, last_name="Edited", updated_by=CURATOR)
    db.add(edited_name)
    lab = _audited(LaboratoryModel, curie="AGRKB:SGD-DATES-LAB", name="Lab 1", status="active",
                   lab_is_open=False)
    db.add(lab)
    db.flush()
    db.add(_audited(LaboratoryCrossReferenceModel, laboratory_id=lab.laboratory_id, curie_prefix="SGD",
                    curie="SGD:Lab_9100001"))
    membership = _audited(LaboratoryPersonModel, laboratory_id=lab.laboratory_id, person_id=member)
    db.add(membership)
    # an obsolete colleague xref of the member (e.g. after a merge) is ignored
    db.add(_audited(PersonCrossReferenceModel, person_id=member, curie_prefix="SGD",
                    curie="SGD:Colleague_9100004", is_obsolete=True))
    db.flush()
    pre_existing_dates = _dates(db, "person", pre_existing)
    name_updated = _dates(db, "person_name", edited_name.person_name_id)[2:]

    counts = update_dates(db, [(9100001, D1), (9100002, D1), (9100003, D2), (9100004, D1), (9199999, D1)],
                          loader_user=LOADER)

    assert _dates(db, "person", loaded) == (D1, D1, D1, D1)
    # in ABC before the SGD load: the colleague's date_created, its own update dates;
    # its SGD xref is the loader's
    assert _dates(db, "person", pre_existing) == (D1, D1) + pre_existing_dates[2:]
    xref_id = db.execute(text("SELECT id FROM person_cross_reference WHERE person_id = :p"),
                         {"p": pre_existing}).scalar()
    assert _dates(db, "person_cross_reference", xref_id) == (D1, D1, D1, D1)
    # updated by a curator since: created dates move, update dates stay
    assert _dates(db, "person_name", edited_name.person_name_id) == (D1, D1) + name_updated
    assert _dates(db, "laboratory", lab.laboratory_id) == (D1, D1, D1, D1)
    # the later of the member's (D2) and the lab head's (D1) dates
    assert _dates(db, "laboratory_person", membership.laboratory_person_id) == (D2, D2, D2, D2)
    assert _dates(db, "person", member) == (D2, D2, D2, D2)
    assert counts["unmatched colleague_ids"] == 1
    assert counts["person"] == 3
    assert counts["person (date_created only)"] == 1
