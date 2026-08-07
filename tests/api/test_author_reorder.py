from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import AuthorModel
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
