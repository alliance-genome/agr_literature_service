from collections import namedtuple

import pytest
from sqlalchemy.exc import IntegrityError
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import AuthorModel, PersonModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa
from .test_reference import test_reference  # noqa


AuthorPersonTestData = namedtuple(
    'AuthorPersonTestData', ['new_author_id', 'related_ref_curie', 'related_ref_id'])


@pytest.fixture
def test_author(db, auth_headers, test_reference):  # noqa
    with TestClient(app) as client:
        new_author = {
            "author_order": 1,
            "first_name": "string",
            "last_name": "string",
            "first_initial": "FI",
            "name": "003_TCU",
            "reference_curie": test_reference.new_ref_curie
        }
        response = client.post(url="/author/", json=new_author, headers=auth_headers)
        yield AuthorPersonTestData(response.json()['author_id'],
                                   test_reference.new_ref_curie,
                                   test_reference.related_ref_id)


def _person(db):  # noqa
    p = PersonModel(display_name="Test Person", curie="AGR:AP-TEST-1")
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


class TestAuthorPersonConstraints:
    def test_neither_person_nor_order_rejected(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        db.add(AuthorModel(reference_id=ref_id))  # no person_id, no author_order
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_person_only_with_author_metadata_rejected(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        p = _person(db)
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id, name="Should Not Be Here"))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_person_only_link_only_allowed(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        p = _person(db)
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id))
        db.commit()  # no error

    def test_duplicate_person_on_reference_rejected(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        p = _person(db)
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id))
        db.commit()
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id, author_order=3))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_duplicate_author_order_on_reference_rejected(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        db.add(AuthorModel(reference_id=ref_id, author_order=5, name="A"))
        db.commit()
        db.add(AuthorModel(reference_id=ref_id, author_order=5, name="B"))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_null_reference_id_rejected(self, db, test_reference):  # noqa
        # every author row must belong to a reference: reference_id is NOT NULL
        db.add(AuthorModel(author_order=1, name="No Reference"))  # no reference_id
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()

    def test_many_pure_author_and_person_only_rows_allowed(self, db, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        db.add(AuthorModel(reference_id=ref_id, author_order=1, name="A1"))
        db.add(AuthorModel(reference_id=ref_id, author_order=2, name="A2"))
        p1 = PersonModel(display_name="P1", curie="AGR:AP-TEST-2")
        p2 = PersonModel(display_name="P2", curie="AGR:AP-TEST-3")
        db.add_all([p1, p2])
        db.commit()
        db.add(AuthorModel(reference_id=ref_id, person_id=p1.person_id))
        db.add(AuthorModel(reference_id=ref_id, person_id=p2.person_id))
        db.commit()  # no error


class TestPersonLinkMerge:
    def test_patch_links_person_no_stub(self, db, auth_headers, test_author):  # noqa
        p = _person(db)
        with TestClient(app) as client:
            r = client.patch(url=f"/author/{test_author.new_author_id}",
                             json={"person_curie": p.curie,
                                   "reference_curie": test_author.related_ref_curie},
                             headers=auth_headers)
        assert r.status_code == status.HTTP_200_OK
        db.expire_all()
        a = db.query(AuthorModel).filter(AuthorModel.author_id == test_author.new_author_id).one()
        assert a.person_id == p.person_id

    def test_patch_absorbs_link_only_stub(self, db, auth_headers, test_author):  # noqa
        # a link-only stub for person P already exists on the same reference
        ref_id = test_author.related_ref_id
        p = _person(db)
        stub = AuthorModel(reference_id=ref_id, person_id=p.person_id)
        db.add(stub)
        db.commit()
        stub_id = stub.author_id
        with TestClient(app) as client:
            r = client.patch(url=f"/author/{test_author.new_author_id}",
                             json={"person_curie": p.curie,
                                   "reference_curie": test_author.related_ref_curie},
                             headers=auth_headers)
        assert r.status_code == status.HTTP_200_OK
        db.expire_all()
        # stub deleted, author row now carries the person
        assert db.query(AuthorModel).filter(AuthorModel.author_id == stub_id).one_or_none() is None
        a = db.query(AuthorModel).filter(AuthorModel.author_id == test_author.new_author_id).one()
        assert a.person_id == p.person_id
        assert a.author_order == 1

    def test_patch_person_already_full_author_errors(self, db, auth_headers, test_author):  # noqa
        # person P is already a *full* author (order set) on the reference
        ref_id = test_author.related_ref_id
        p = _person(db)
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id, author_order=2, name="Other"))
        db.commit()
        with TestClient(app) as client:
            r = client.patch(url=f"/author/{test_author.new_author_id}",
                             json={"person_curie": p.curie,
                                   "reference_curie": test_author.related_ref_curie},
                             headers=auth_headers)
        assert r.status_code == status.HTTP_409_CONFLICT

    def test_create_absorbs_link_only_stub(self, db, auth_headers, test_reference):  # noqa
        # POST /author for a person that already has a link-only stub on the reference
        # should absorb the stub (like PATCH), not 500 on the uniqueness constraint.
        ref_id = test_reference.related_ref_id
        p = _person(db)
        stub = AuthorModel(reference_id=ref_id, person_id=p.person_id)
        db.add(stub)
        db.commit()
        stub_id = stub.author_id
        with TestClient(app) as client:
            r = client.post(url="/author/",
                            json={"author_order": 1, "name": "Real",
                                  "person_curie": p.curie,
                                  "reference_curie": test_reference.new_ref_curie},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_201_CREATED
        db.expire_all()
        assert db.query(AuthorModel).filter(AuthorModel.author_id == stub_id).one_or_none() is None
        new_id = r.json()["author_id"]
        a = db.query(AuthorModel).filter(AuthorModel.author_id == new_id).one()
        assert a.person_id == p.person_id
        assert a.author_order == 1

    def test_create_person_already_full_author_errors(self, db, auth_headers, test_reference):  # noqa
        # POST /author for a person already a full author on the reference -> 409, not 500.
        ref_id = test_reference.related_ref_id
        p = _person(db)
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id, author_order=2, name="Other"))
        db.commit()
        with TestClient(app) as client:
            r = client.post(url="/author/",
                            json={"author_order": 1, "name": "Real",
                                  "person_curie": p.curie,
                                  "reference_curie": test_reference.new_ref_curie},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_409_CONFLICT

    def test_create_person_only_link_stub(self, db, auth_headers, test_reference):  # noqa
        # POST /author with only person_curie (+ reference), no author_order and no
        # prior collision -> 201 and a valid link-only stub (person set, order NULL).
        p = _person(db)
        with TestClient(app) as client:
            r = client.post(url="/author/",
                            json={"person_curie": p.curie,
                                  "reference_curie": test_reference.new_ref_curie},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_201_CREATED
        new_id = r.json()["author_id"]
        db.expire_all()
        a = db.query(AuthorModel).filter(AuthorModel.author_id == new_id).one()
        assert a.person_id == p.person_id
        assert a.author_order is None

    def test_create_person_only_duplicate_stub_errors(self, db, auth_headers, test_reference):  # noqa
        # a second person-only POST for the same person/reference -> 409, not 500.
        ref_id = test_reference.related_ref_id
        p = _person(db)
        db.add(AuthorModel(reference_id=ref_id, person_id=p.person_id))
        db.commit()
        with TestClient(app) as client:
            r = client.post(url="/author/",
                            json={"person_curie": p.curie,
                                  "reference_curie": test_reference.new_ref_curie},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_409_CONFLICT
