from collections import namedtuple

import pytest
from sqlalchemy.exc import IntegrityError
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import AuthorModel, PersonModel, ReferenceModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa
from .test_reference import test_reference  # noqa
from .test_resource import test_resource  # noqa


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


class TestReachable500Hardening:
    """The author CHECK / NOT NULL constraints must surface as clean 422s (never a
    raw IntegrityError 500) through both the POST /author and POST /reference paths."""

    def test_reference_create_later_author_person_curie(self, db, auth_headers, test_reference):  # noqa
        # POST /reference with 2+ authors where a LATER author has person_curie must
        # not 500 (earlier pending authors autoflushing with reference_id NULL).
        p = _person(db)
        with TestClient(app) as client:
            r = client.post(url="/reference/",
                            json={"title": "Two authors", "category": "thesis",
                                  "authors": [{"author_order": 1, "name": "A"},
                                              {"author_order": 2, "name": "B",
                                               "person_curie": p.curie}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_201_CREATED
        ref_id = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == r.json()["curie"]).scalar()
        authors = db.query(AuthorModel).filter(AuthorModel.reference_id == ref_id).all()
        assert len(authors) == 2
        author_b = db.query(AuthorModel).filter(
            AuthorModel.reference_id == ref_id, AuthorModel.name == "B").one()
        assert author_b.person_id == p.person_id

    def test_reference_create_embedded_author_no_order_no_person(self, db, auth_headers):  # noqa
        # an embedded author with neither author_order nor a person violates
        # ck_author_person_or_order -> 422, not 500.
        with TestClient(app) as client:
            r = client.post(url="/reference/",
                            json={"title": "Bad author", "category": "thesis",
                                  "authors": [{"name": "No Order No Person"}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_author_no_order_no_person(self, db, auth_headers, test_reference):  # noqa
        # POST /author with a reference but neither author_order nor person_curie
        # violates ck_author_person_or_order -> 422.
        with TestClient(app) as client:
            r = client.post(url="/author/",
                            json={"name": "Nameless order",
                                  "reference_curie": test_reference.new_ref_curie},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_author_person_link_with_metadata_no_order(self, db, auth_headers, test_reference):  # noqa
        # person_curie + author metadata but no author_order violates
        # ck_person_only_link_only -> 422.
        p = _person(db)
        with TestClient(app) as client:
            r = client.post(url="/author/",
                            json={"name": "Has Name",
                                  "person_curie": p.curie,
                                  "reference_curie": test_reference.new_ref_curie},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_create_person_only_empty_affiliations_coerced(self, db, auth_headers, test_reference):  # noqa
        # a person-only POST carrying affiliations: [] (a UI's "no affiliations")
        # must not trip ck_person_only_link_only -> 201, affiliations stored NULL.
        p = _person(db)
        with TestClient(app) as client:
            r = client.post(url="/author/",
                            json={"person_curie": p.curie,
                                  "affiliations": [],
                                  "reference_curie": test_reference.new_ref_curie},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_201_CREATED
        new_id = r.json()["author_id"]
        db.expire_all()
        a = db.query(AuthorModel).filter(AuthorModel.author_id == new_id).one()
        assert a.person_id == p.person_id
        assert a.author_order is None
        assert a.affiliations is None

    def test_create_person_only_empty_name_coerced(self, db, auth_headers, test_reference):  # noqa
        # a person-only POST carrying name: "" must not trip ck_person_only_link_only
        # -> 201, name stored NULL.
        p = _person(db)
        with TestClient(app) as client:
            r = client.post(url="/author/",
                            json={"person_curie": p.curie,
                                  "name": "",
                                  "reference_curie": test_reference.new_ref_curie},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_201_CREATED
        new_id = r.json()["author_id"]
        db.expire_all()
        a = db.query(AuthorModel).filter(AuthorModel.author_id == new_id).one()
        assert a.person_id == p.person_id
        assert a.author_order is None
        assert a.name is None

    def test_reference_create_embedded_person_only_empty_affiliations(self, db, auth_headers):  # noqa
        # an embedded person-only author with affiliations: [] (no author_order) must
        # not trip ck_person_only_link_only through the reference-create path -> 201.
        p = _person(db)
        with TestClient(app) as client:
            r = client.post(url="/reference/",
                            json={"title": "Embedded stub", "category": "thesis",
                                  "authors": [{"person_curie": p.curie,
                                               "affiliations": []}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_201_CREATED
        ref_id = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == r.json()["curie"]).scalar()
        a = db.query(AuthorModel).filter(AuthorModel.reference_id == ref_id).one()
        assert a.person_id == p.person_id
        assert a.author_order is None
        assert a.affiliations is None

    def test_create_duplicate_author_order_409(self, db, auth_headers, test_reference):  # noqa
        # POST /author with an author_order already taken on the reference must be a
        # clean 409 (uq_author_ref_order is DEFERRABLE, else a raw 500 at commit).
        with TestClient(app) as client:
            first = client.post(url="/author/",
                                json={"author_order": 3, "name": "First",
                                      "reference_curie": test_reference.new_ref_curie},
                                headers=auth_headers)
            assert first.status_code == status.HTTP_201_CREATED
            second = client.post(url="/author/",
                                 json={"author_order": 3, "name": "Second",
                                       "reference_curie": test_reference.new_ref_curie},
                                 headers=auth_headers)
        assert second.status_code == status.HTTP_409_CONFLICT

    def test_reference_create_duplicate_embedded_author_order_409(self, db, auth_headers):  # noqa
        # POST /reference with two embedded authors sharing author_order must be a
        # clean 409 (uq_author_ref_order would otherwise raise a raw 500 at commit).
        with TestClient(app) as client:
            r = client.post(url="/reference/",
                            json={"title": "Dup order", "category": "thesis",
                                  "authors": [{"author_order": 1, "name": "A"},
                                              {"author_order": 1, "name": "B"}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_409_CONFLICT

    def test_patch_metadata_onto_person_only_stub_rejected(self, db, auth_headers, test_reference):  # noqa
        # PATCHing real metadata onto a person-only stub (author_order NULL, which
        # PATCH cannot set) would violate ck_person_only_link_only -> must 422.
        ref_id = test_reference.related_ref_id
        p = _person(db)
        stub = AuthorModel(reference_id=ref_id, person_id=p.person_id)
        db.add(stub)
        db.commit()
        stub_id = stub.author_id
        with TestClient(app) as client:
            r = client.patch(url=f"/author/{stub_id}",
                             json={"name": "X"},
                             headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
