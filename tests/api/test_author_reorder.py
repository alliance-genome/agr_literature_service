from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import AuthorModel, PersonModel, ReferenceModel, UserModel
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

    def test_reorder_stamps_updated_by(self, db, auth_headers, test_reference):  # noqa
        # A reorder must stamp updated_by/date_updated with the acting user so (1) the
        # change is visible in history and (2) the ingest curator-gate sees the
        # reference as touched (when the actor is a curator) and won't renumber the
        # authors back. Seed the rows with a DIFFERENT user so the acting user's stamp
        # is detectable; pre-fix the raw UPDATE never touched updated_by -> stays the
        # seeded value and this must fail.
        ref_id = test_reference.related_ref_id
        seed_uid = "reorder-seed-user"
        # users rows survive the per-test cleanup, so get-or-create this seed user.
        if db.query(UserModel).filter_by(id=seed_uid).one_or_none() is None:
            db.add(UserModel(id=seed_uid, automation_username="reorderSeed"))
            db.commit()
        a1 = AuthorModel(reference_id=ref_id, author_order=1, name="A1",
                         created_by=seed_uid, updated_by=seed_uid)
        a2 = AuthorModel(reference_id=ref_id, author_order=2, name="A2",
                         created_by=seed_uid, updated_by=seed_uid)
        db.add_all([a1, a2])
        db.commit()
        a1_id, a2_id = a1.author_id, a2.author_id
        assert a1.updated_by == seed_uid and a2.updated_by == seed_uid
        with TestClient(app) as client:
            r = client.post(url="/author/reorder",
                            json={"reference_curie": test_reference.new_ref_curie,
                                  "ordering": [{"author_id": a1_id, "author_order": 2},
                                               {"author_id": a2_id, "author_order": 1}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_200_OK
        db.expire_all()
        after = {a.author_id: a.updated_by
                 for a in db.query(AuthorModel).filter(AuthorModel.reference_id == ref_id)}
        # the reorder stamped the acting user (no longer the seeded user) on both rows,
        # so the reference now carries the actor's audit trail on its author rows.
        assert after[a1_id] is not None and after[a1_id] != seed_uid
        assert after[a2_id] is not None and after[a2_id] != seed_uid

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

    def test_reorder_person_only_stub_rejected(self, db, auth_headers, test_reference):  # noqa
        # a person-only stub (author_order IS NULL, person_id set) is not an ordered
        # author; including it in a reorder payload must be rejected (422), not silently
        # promoted to a nameless ordered author.
        ref_id = test_reference.related_ref_id
        (a1,) = _mk_authors(db, ref_id, 1)
        p = PersonModel(display_name="Reorder Stub Person", curie="AGR:AP-REORDER-1")
        db.add(p)
        db.commit()
        db.refresh(p)
        stub = AuthorModel(reference_id=ref_id, person_id=p.person_id)
        db.add(stub)
        db.commit()
        stub_id = stub.author_id
        with TestClient(app) as client:
            r = client.post(url="/author/reorder",
                            json={"reference_curie": test_reference.new_ref_curie,
                                  "ordering": [{"author_id": a1, "author_order": 1},
                                               {"author_id": stub_id, "author_order": 2}]},
                            headers=auth_headers)
        assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        assert "person-only" in r.json()["detail"]

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
