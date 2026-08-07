from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import AuthorModel, ReferenceModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa
from .test_reference import test_reference  # noqa


def _mk_authors(db, ref_id, n):  # noqa
    ids = []
    for i in range(1, n + 1):
        a = AuthorModel(reference_id=ref_id, author_order=i, name=f"A{i}")
        db.add(a)
        db.commit()
        ids.append(a.author_id)
    return ids


class TestReorder:
    def test_reorder_swaps_in_one_statement(self, db, auth_headers, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        a1, a2, a3 = _mk_authors(db, ref_id, 3)
        with TestClient(app) as client:
            r = client.post(url="/author/reorder",
                            json={"reference_curie": test_reference.new_ref_curie,
                                  "ordering": [{"author_id": a1, "author_order": 3},
                                               {"author_id": a2, "author_order": 1},
                                               {"author_id": a3, "author_order": 2}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_200_OK
        db.expire_all()
        orders = {a.author_id: a.author_order
                  for a in db.query(AuthorModel).filter(AuthorModel.reference_id == ref_id)}
        assert orders == {a1: 3, a2: 1, a3: 2}

    def test_patch_rejects_author_order(self, db, auth_headers, test_reference):  # noqa
        ref_id = test_reference.related_ref_id
        (a1,) = _mk_authors(db, ref_id, 1)
        with TestClient(app) as client:
            r = client.patch(url=f"/author/{a1}",
                             json={"author_order": 5, "reference_curie": test_reference.new_ref_curie},
                             headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_reorder_foreign_author_id_rejected(self, db, auth_headers, test_reference):  # noqa
        # an author_id belonging to a DIFFERENT reference must be rejected (422),
        # not silently skipped with a false 200.
        ref_id = test_reference.related_ref_id
        (a1,) = _mk_authors(db, ref_id, 1)
        with TestClient(app) as client:
            other = client.post(url="/reference/",
                                json={"title": "Other", "category": "thesis"},
                                headers=auth_headers)
            other_ref_id = db.query(ReferenceModel.reference_id).filter(
                ReferenceModel.curie == other.json()["curie"]).scalar()
            foreign = AuthorModel(reference_id=other_ref_id, author_order=1, name="Foreign")
            db.add(foreign)
            db.commit()
            foreign_id = foreign.author_id
            r = client.post(url="/author/reorder",
                            json={"reference_curie": test_reference.new_ref_curie,
                                  "ordering": [{"author_id": a1, "author_order": 2},
                                               {"author_id": foreign_id, "author_order": 1}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_reorder_duplicate_target_orders_rejected(self, db, auth_headers, test_reference):  # noqa
        # two authors assigned the same target order must be rejected (422), not a
        # deferred-constraint 500 at commit.
        ref_id = test_reference.related_ref_id
        a1, a2 = _mk_authors(db, ref_id, 2)
        with TestClient(app) as client:
            r = client.post(url="/author/reorder",
                            json={"reference_curie": test_reference.new_ref_curie,
                                  "ordering": [{"author_id": a1, "author_order": 1},
                                               {"author_id": a2, "author_order": 1}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_reorder_duplicate_author_id_rejected(self, db, auth_headers, test_reference):  # noqa
        # the same author_id twice makes the UPDATE winner nondeterministic -> 422.
        ref_id = test_reference.related_ref_id
        (a1,) = _mk_authors(db, ref_id, 1)
        with TestClient(app) as client:
            r = client.post(url="/author/reorder",
                            json={"reference_curie": test_reference.new_ref_curie,
                                  "ordering": [{"author_id": a1, "author_order": 1},
                                               {"author_id": a1, "author_order": 2}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_reorder_partial_collides_with_absent_author(self, db, auth_headers, test_reference):  # noqa
        # a1=1, a2=2; a partial reorder [{a1: 2}] leaves a2 at order 2, so a1's new
        # order collides with the absent a2 -> deferred-constraint 500 at commit
        # unless caught. Expect 422.
        ref_id = test_reference.related_ref_id
        a1, a2 = _mk_authors(db, ref_id, 2)
        with TestClient(app) as client:
            r = client.post(url="/author/reorder",
                            json={"reference_curie": test_reference.new_ref_curie,
                                  "ordering": [{"author_id": a1, "author_order": 2}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_reorder_zero_author_order_rejected(self, db, auth_headers, test_reference):  # noqa
        # author_order must be >= 1; 0 is a schema validation 422, not a 500.
        ref_id = test_reference.related_ref_id
        (a1,) = _mk_authors(db, ref_id, 1)
        with TestClient(app) as client:
            r = client.post(url="/author/reorder",
                            json={"reference_curie": test_reference.new_ref_curie,
                                  "ordering": [{"author_id": a1, "author_order": 0}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_reorder_missing_key_is_422(self, db, auth_headers, test_reference):  # noqa
        # a malformed ordering item (missing author_order) is a 422 from schema
        # validation, not a KeyError/500.
        ref_id = test_reference.related_ref_id
        (a1,) = _mk_authors(db, ref_id, 1)
        with TestClient(app) as client:
            r = client.post(url="/author/reorder",
                            json={"reference_curie": test_reference.new_ref_curie,
                                  "ordering": [{"author_id": a1}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
