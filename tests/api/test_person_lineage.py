# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import PersonLineageModel, PersonModel
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


LineageTestData = namedtuple("LineageTestData", ["response", "new_id", "person_subject_id", "person_object_id"])


@pytest.fixture
def two_people(db):  # noqa
    p1 = PersonModel(display_name="Canon One", curie="AGRKB:test-canon-1")
    p2 = PersonModel(display_name="Canon Two", curie="AGRKB:test-canon-2")
    db.add(p1)
    db.add(p2)
    db.commit()
    db.refresh(p1)
    db.refresh(p2)
    return {"person_subject_id": p1.person_id, "person_object_id": p2.person_id}


@pytest.fixture
def test_lineage(db, auth_headers, two_people):  # noqa
    with TestClient(app) as client:
        payload = {
            "person_subject_curie_or_id": two_people["person_subject_id"],
            "person_object_curie_or_id": two_people["person_object_id"],
            "relationship": "phd_supervisor_of",
        }
        response = client.post("/person_lineage/", json=payload, headers=auth_headers)
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield LineageTestData(
            response=response,
            new_id=body.get("person_lineage_id"),
            person_subject_id=two_people["person_subject_id"],
            person_object_id=two_people["person_object_id"],
        )


class TestPersonLineage:

    def test_create_lineage(self, db, test_lineage):  # noqa
        assert test_lineage.response.status_code == status.HTTP_201_CREATED
        obj = (
            db.query(PersonLineageModel)
            .filter(PersonLineageModel.person_lineage_id == test_lineage.new_id)
            .one()
        )
        assert obj.person_subject_id == test_lineage.person_subject_id
        assert obj.person_object_id == test_lineage.person_object_id
        assert obj.relationship == "phd_supervisor_of"

    def test_duplicate_rejected(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={
                    "person_subject_curie_or_id": test_lineage.person_subject_id,
                    "person_object_curie_or_id": test_lineage.person_object_id,
                    "relationship": "phd_supervisor_of",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_reversed_pair_allowed(self, auth_headers, test_lineage):  # noqa
        # Directional: B->A is a different fact from A->B.
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={
                    "person_subject_curie_or_id": test_lineage.person_object_id,
                    "person_object_curie_or_id": test_lineage.person_subject_id,
                    "relationship": "phd_supervisor_of",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_201_CREATED

    def test_invalid_person_rejected(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={"person_subject_curie_or_id": 9999999, "person_object_curie_or_id": 9999998, "relationship": "phd_supervisor_of"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_self_pair_rejected(self, auth_headers, two_people):  # noqa
        # A person cannot be in a relationship with themselves.
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage/",
                json={
                    "person_subject_curie_or_id": two_people["person_subject_id"],
                    "person_object_curie_or_id": two_people["person_subject_id"],
                    "relationship": "phd_supervisor_of",
                },
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_symmetric_collaborator_reversed_rejected(self, db, auth_headers, two_people):  # noqa
        # collaborator_of is non-directional: (A,B) and (B,A) normalize to the same
        # canonical row, so the reverse insert hits the unique constraint -> 422.
        with TestClient(app) as client:
            c1 = client.post(
                "/person_lineage/",
                json={
                    "person_subject_curie_or_id": two_people["person_subject_id"],
                    "person_object_curie_or_id": two_people["person_object_id"],
                    "relationship": "collaborator_of",
                },
                headers=auth_headers,
            )
            assert c1.status_code == status.HTTP_201_CREATED
            c2 = client.post(
                "/person_lineage/",
                json={
                    "person_subject_curie_or_id": two_people["person_object_id"],
                    "person_object_curie_or_id": two_people["person_subject_id"],
                    "relationship": "collaborator_of",
                },
                headers=auth_headers,
            )
            assert c2.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

        # exactly one canonical row regardless of submitted direction
        count = (
            db.query(PersonLineageModel)
            .filter(PersonLineageModel.relationship == "collaborator_of")
            .count()
        )
        assert count == 1

    def test_show_and_patch(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.get(f"/person_lineage/{test_lineage.new_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_200_OK

            res = client.patch(
                f"/person_lineage/{test_lineage.new_id}",
                json={"relationship": "postdoc_supervisor_of"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            assert res.json()["relationship"] == "postdoc_supervisor_of"

    def test_patch_to_symmetric_normalizes_ids(self, auth_headers, two_people):  # noqa
        lo = min(two_people["person_subject_id"], two_people["person_object_id"])
        hi = max(two_people["person_subject_id"], two_people["person_object_id"])
        with TestClient(app) as client:
            # directional row deliberately stored in reverse (hi, lo) order
            r = client.post(
                "/person_lineage/",
                json={"person_subject_curie_or_id": hi, "person_object_curie_or_id": lo, "relationship": "phd_supervisor_of"},
                headers=auth_headers,
            )
            pid = r.json()["person_lineage_id"]
            # patch to the symmetric relationship -> ids must re-normalize to (lo, hi)
            p = client.patch(
                f"/person_lineage/{pid}",
                json={"relationship": "collaborator_of"},
                headers=auth_headers,
            )
            assert p.status_code == status.HTTP_200_OK
            g = client.get(f"/person_lineage/{pid}", headers=auth_headers).json()
            assert g["person_subject_id"] == lo and g["person_object_id"] == hi

    def test_patch_to_symmetric_collision_rejected(self, auth_headers, two_people):  # noqa
        lo = min(two_people["person_subject_id"], two_people["person_object_id"])
        hi = max(two_people["person_subject_id"], two_people["person_object_id"])
        with TestClient(app) as client:
            # existing normalized collaborator_of (lo, hi)
            client.post(
                "/person_lineage/",
                json={"person_subject_curie_or_id": lo, "person_object_curie_or_id": hi, "relationship": "collaborator_of"},
                headers=auth_headers,
            )
            # directional (hi, lo) phd row; patching it to collaborator_of would
            # normalize to (lo, hi) and collide with the row above -> 422
            r = client.post(
                "/person_lineage/",
                json={"person_subject_curie_or_id": hi, "person_object_curie_or_id": lo, "relationship": "phd_supervisor_of"},
                headers=auth_headers,
            )
            pid = r.json()["person_lineage_id"]
            p = client.patch(
                f"/person_lineage/{pid}",
                json={"relationship": "collaborator_of"},
                headers=auth_headers,
            )
            assert p.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_list_for_person_matches_either_side(self, db, auth_headers, two_people):  # noqa
        # A person appearing as subject in one PPR and object in another is returned
        # by both; GET /person_lineage/person/{id} matches either side.
        s_id = two_people["person_subject_id"]
        o_id = two_people["person_object_id"]
        with TestClient(app) as client:
            # person s is the subject here
            client.post(
                "/person_lineage/",
                json={"person_subject_curie_or_id": s_id, "person_object_curie_or_id": o_id,
                      "relationship": "phd_supervisor_of"},
                headers=auth_headers,
            )
            # person s is the object here (reverse direction is a distinct fact)
            client.post(
                "/person_lineage/",
                json={"person_subject_curie_or_id": o_id, "person_object_curie_or_id": s_id,
                      "relationship": "postdoc_supervisor_of"},
                headers=auth_headers,
            )

            by_id = client.get(f"/person_lineage/person/{s_id}", headers=auth_headers)
            assert by_id.status_code == status.HTTP_200_OK
            rows = by_id.json()
            assert len(rows) == 2
            # s appears once on each side
            assert {r["person_subject_id"] for r in rows} == {s_id, o_id}

            # by curie resolves to the same person -> same rows
            curie = db.query(PersonModel).filter(PersonModel.person_id == s_id).one().curie
            by_curie = client.get(f"/person_lineage/person/{curie}", headers=auth_headers)
            assert by_curie.status_code == status.HTTP_200_OK
            assert len(by_curie.json()) == 2

    def test_list_for_person_unknown_404(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get("/person_lineage/person/9999999", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_destroy(self, auth_headers, test_lineage):  # noqa
        with TestClient(app) as client:
            res = client.delete(f"/person_lineage/{test_lineage.new_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_204_NO_CONTENT
            res = client.get(f"/person_lineage/{test_lineage.new_id}", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND
