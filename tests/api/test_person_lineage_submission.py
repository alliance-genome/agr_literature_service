# flake8: noqa: F811
from collections import namedtuple

import pytest
from starlette.testclient import TestClient
from fastapi import status

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    PersonLineageSubmissionModel,
    PersonLineageModel,
    PersonModel,
)
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa


SubmissionTestData = namedtuple("SubmissionTestData", ["response", "new_id"])


@pytest.fixture
def two_people(db):  # noqa
    p1 = PersonModel(display_name="Sub One", curie="AGRKB:test-sub-1")
    p2 = PersonModel(display_name="Sub Two", curie="AGRKB:test-sub-2")
    db.add(p1)
    db.add(p2)
    db.commit()
    db.refresh(p1)
    db.refresh(p2)
    return {"person_subject_id": p1.person_id, "person_object_id": p2.person_id}


@pytest.fixture
def test_submission(db, auth_headers):  # noqa
    with TestClient(app) as client:
        # Names only — no ids, no status.
        payload = {
            "person_subject_name": "Alice Advisor",
            "person_object_name": "Bob Trainee",
            "relationship": "phd_supervisor_of",
            "who_sent_this": "curator1",
        }
        response = client.post("/person_lineage_submission/", json=payload, headers=auth_headers)
        body = response.json() if response.status_code == status.HTTP_201_CREATED else {}
        yield SubmissionTestData(response=response, new_id=body.get("person_lineage_submission_id"))


class TestPersonLineageSubmission:

    def test_create_names_only(self, db, test_submission):  # noqa
        assert test_submission.response.status_code == status.HTTP_201_CREATED
        obj = (
            db.query(PersonLineageSubmissionModel)
            .filter(PersonLineageSubmissionModel.person_lineage_submission_id == test_submission.new_id)
            .one()
        )
        assert obj.person_subject_name == "Alice Advisor"
        assert obj.person_subject_id is None and obj.person_object_id is None
        assert obj.status == "pending"
        assert obj.person_lineage_id is None

    def test_missing_required_rejected(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage_submission/",
                json={"person_subject_name": "A", "person_object_name": "B", "relationship": "phd_supervisor_of"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_bad_relationship_rejected(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.post(
                "/person_lineage_submission/",
                json={"person_subject_name": "A", "person_object_name": "B",
                      "relationship": "bogus", "who_sent_this": "x"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_duplicate_name_only_submissions_allowed(self, auth_headers):  # noqa
        # No constraint on submissions — identical name-only claims all succeed.
        payload = {"person_subject_name": "Dup A", "person_object_name": "Dup B",
                   "relationship": "phd_supervisor_of", "who_sent_this": "x"}
        with TestClient(app) as client:
            for _ in range(3):
                res = client.post("/person_lineage_submission/", json=payload, headers=auth_headers)
                assert res.status_code == status.HTTP_201_CREATED

    def test_resolve_one_side(self, auth_headers, test_submission, two_people):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person_lineage_submission/{test_submission.new_id}",
                json={"person_subject_curie_or_id": two_people["person_subject_id"], "status": "partially_resolved"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            body = res.json()
            assert body["person_subject_id"] == two_people["person_subject_id"]
            assert body["person_object_id"] is None
            assert body["status"] == "partially_resolved"

    def test_bad_status_rejected(self, auth_headers, test_submission):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person_lineage_submission/{test_submission.new_id}",
                json={"status": "nonsense"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_invalid_person_id_rejected(self, auth_headers, test_submission):  # noqa
        with TestClient(app) as client:
            res = client.patch(
                f"/person_lineage_submission/{test_submission.new_id}",
                json={"person_subject_curie_or_id": 9999999},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_validate_requires_both_ids(self, auth_headers, test_submission, two_people):  # noqa
        with TestClient(app) as client:
            client.patch(
                f"/person_lineage_submission/{test_submission.new_id}",
                json={"person_subject_curie_or_id": two_people["person_subject_id"]},
                headers=auth_headers,
            )
            res = client.post(
                f"/person_lineage_submission/{test_submission.new_id}/validate",
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_validate_creates_canonical_then_dedups(self, db, auth_headers, two_people):  # noqa
        with TestClient(app) as client:
            # First submission, fully resolved, validated -> creates canonical.
            payload = {
                "person_subject_name": "Alice", "person_object_name": "Bob",
                "relationship": "phd_supervisor_of", "who_sent_this": "curator1",
                "person_subject_curie_or_id": two_people["person_subject_id"],
                "person_object_curie_or_id": two_people["person_object_id"],
            }
            r1 = client.post("/person_lineage_submission/", json=payload, headers=auth_headers)
            assert r1.status_code == status.HTTP_201_CREATED
            sub1 = r1.json()["person_lineage_submission_id"]

            v1 = client.post(f"/person_lineage_submission/{sub1}/validate", headers=auth_headers)
            assert v1.status_code == status.HTTP_200_OK
            assert v1.json()["status"] == "validated"
            canonical_id = v1.json()["person_lineage_id"]
            assert canonical_id is not None

            # Second submission, same resolved pair + relationship, validated -> duplicate.
            payload2 = dict(payload, who_sent_this="curator2")
            r2 = client.post("/person_lineage_submission/", json=payload2, headers=auth_headers)
            sub2 = r2.json()["person_lineage_submission_id"]
            v2 = client.post(f"/person_lineage_submission/{sub2}/validate", headers=auth_headers)
            assert v2.status_code == status.HTTP_200_OK
            assert v2.json()["status"] == "duplicate"
            # Linked to the SAME canonical row; no second canonical created.
            assert v2.json()["person_lineage_id"] == canonical_id

        count = (
            db.query(PersonLineageModel)
            .filter(
                PersonLineageModel.person_subject_id == two_people["person_subject_id"],
                PersonLineageModel.person_object_id == two_people["person_object_id"],
                PersonLineageModel.relationship == "phd_supervisor_of",
            )
            .count()
        )
        assert count == 1

    def test_reject_submission(self, db, auth_headers, test_submission):  # noqa
        # A curator can reject a submission; it is not linked to any canonical row.
        with TestClient(app) as client:
            res = client.patch(
                f"/person_lineage_submission/{test_submission.new_id}",
                json={"status": "rejected"},
                headers=auth_headers,
            )
            assert res.status_code == status.HTTP_200_OK
            body = res.json()
            assert body["status"] == "rejected"
            assert body["person_lineage_id"] is None

    def test_same_people_different_relationships(self, db, auth_headers, two_people):  # noqa
        # Same pair + different relationship => two distinct canonical rows
        # (the unique constraint is on the full triple, not just the pair).
        with TestClient(app) as client:
            ids = {}
            for rel in ("phd_supervisor_of", "postdoc_supervisor_of"):
                r = client.post(
                    "/person_lineage_submission/",
                    json={
                        "person_subject_name": "Alice", "person_object_name": "Bob",
                        "relationship": rel, "who_sent_this": "cur",
                        "person_subject_curie_or_id": two_people["person_subject_id"],
                        "person_object_curie_or_id": two_people["person_object_id"],
                    },
                    headers=auth_headers,
                )
                sub_id = r.json()["person_lineage_submission_id"]
                v = client.post(f"/person_lineage_submission/{sub_id}/validate", headers=auth_headers)
                assert v.status_code == status.HTTP_200_OK
                # each is a brand-new canonical fact, not a duplicate
                assert v.json()["status"] == "validated"
                ids[rel] = v.json()["person_lineage_id"]

            assert ids["phd_supervisor_of"] != ids["postdoc_supervisor_of"]

        canon = (
            db.query(PersonLineageModel)
            .filter(
                PersonLineageModel.person_subject_id == two_people["person_subject_id"],
                PersonLineageModel.person_object_id == two_people["person_object_id"],
            )
            .count()
        )
        assert canon == 2

    def test_validate_links_to_preexisting_canonical(self, db, auth_headers, two_people):  # noqa
        # A canonical PPR created independently; a later submission that resolves to
        # the same triple validates as a 'duplicate' linked to that existing row.
        with TestClient(app) as client:
            c = client.post(
                "/person_lineage/",
                json={
                    "person_subject_curie_or_id": two_people["person_subject_id"],
                    "person_object_curie_or_id": two_people["person_object_id"],
                    "relationship": "phd_supervisor_of",
                },
                headers=auth_headers,
            )
            assert c.status_code == status.HTTP_201_CREATED
            canonical_id = c.json()["person_lineage_id"]

            r = client.post(
                "/person_lineage_submission/",
                json={
                    "person_subject_name": "Alice", "person_object_name": "Bob",
                    "relationship": "phd_supervisor_of", "who_sent_this": "cur",
                    "person_subject_curie_or_id": two_people["person_subject_id"],
                    "person_object_curie_or_id": two_people["person_object_id"],
                },
                headers=auth_headers,
            )
            sub_id = r.json()["person_lineage_submission_id"]
            v = client.post(f"/person_lineage_submission/{sub_id}/validate", headers=auth_headers)
            assert v.status_code == status.HTTP_200_OK
            assert v.json()["status"] == "duplicate"
            assert v.json()["person_lineage_id"] == canonical_id

        count = (
            db.query(PersonLineageModel)
            .filter(
                PersonLineageModel.person_subject_id == two_people["person_subject_id"],
                PersonLineageModel.person_object_id == two_people["person_object_id"],
                PersonLineageModel.relationship == "phd_supervisor_of",
            )
            .count()
        )
        assert count == 1

    def test_symmetric_collaborator_reversed_dedups(self, db, auth_headers, two_people):  # noqa
        # collaborator_of is non-directional: validating (A,B) then (B,A) links both
        # submissions to the SAME canonical row; the second is marked duplicate.
        with TestClient(app) as client:
            def submit_and_validate(one_id, two_id):
                r = client.post(
                    "/person_lineage_submission/",
                    json={
                        "person_subject_name": "A", "person_object_name": "B",
                        "relationship": "collaborator_of", "who_sent_this": "cur",
                        "person_subject_curie_or_id": one_id, "person_object_curie_or_id": two_id,
                    },
                    headers=auth_headers,
                )
                sub_id = r.json()["person_lineage_submission_id"]
                return client.post(
                    f"/person_lineage_submission/{sub_id}/validate", headers=auth_headers
                ).json()

            first = submit_and_validate(two_people["person_subject_id"], two_people["person_object_id"])
            assert first["status"] == "validated"

            # reversed order -> same canonical, duplicate
            second = submit_and_validate(two_people["person_object_id"], two_people["person_subject_id"])
            assert second["status"] == "duplicate"
            assert second["person_lineage_id"] == first["person_lineage_id"]

        count = (
            db.query(PersonLineageModel)
            .filter(PersonLineageModel.relationship == "collaborator_of")
            .count()
        )
        assert count == 1

    def _make_resolved_submission(self, client, auth_headers, two_people):  # noqa
        r = client.post(
            "/person_lineage_submission/",
            json={
                "person_subject_name": "A", "person_object_name": "B",
                "relationship": "phd_supervisor_of", "who_sent_this": "cur",
                "person_subject_curie_or_id": two_people["person_subject_id"],
                "person_object_curie_or_id": two_people["person_object_id"],
            },
            headers=auth_headers,
        )
        return r.json()["person_lineage_submission_id"]

    def test_revalidate_is_idempotent_noop(self, db, auth_headers, two_people):  # noqa
        # Re-validating an already-linked submission is a harmless no-op: it stays
        # 'validated' (not flipped to 'duplicate') and no second canonical is made.
        with TestClient(app) as client:
            sub_id = self._make_resolved_submission(client, auth_headers, two_people)
            v1 = client.post(f"/person_lineage_submission/{sub_id}/validate", headers=auth_headers)
            assert v1.status_code == status.HTTP_200_OK
            canonical_id = v1.json()["person_lineage_id"]
            v2 = client.post(f"/person_lineage_submission/{sub_id}/validate", headers=auth_headers)
            assert v2.status_code == status.HTTP_200_OK
            assert v2.json()["status"] == "validated"
            assert v2.json()["person_lineage_id"] == canonical_id

        count = (
            db.query(PersonLineageModel)
            .filter(
                PersonLineageModel.person_subject_id == two_people["person_subject_id"],
                PersonLineageModel.person_object_id == two_people["person_object_id"],
                PersonLineageModel.relationship == "phd_supervisor_of",
            )
            .count()
        )
        assert count == 1

    def test_revalidate_after_status_reset_is_noop(self, db, auth_headers, two_people):  # noqa
        # Even if a curator resets status, an already-linked submission re-validates
        # as a no-op (still linked to the same canonical, no new canonical row).
        with TestClient(app) as client:
            sub_id = self._make_resolved_submission(client, auth_headers, two_people)
            v1 = client.post(f"/person_lineage_submission/{sub_id}/validate", headers=auth_headers)
            canonical_id = v1.json()["person_lineage_id"]
            client.patch(
                f"/person_lineage_submission/{sub_id}",
                json={"status": "pending"},
                headers=auth_headers,
            )
            again = client.post(f"/person_lineage_submission/{sub_id}/validate", headers=auth_headers)
            assert again.status_code == status.HTTP_200_OK
            assert again.json()["person_lineage_id"] == canonical_id
            # no-op does not re-stamp status — the curator-set value is preserved
            assert again.json()["status"] == "pending"

        count = (
            db.query(PersonLineageModel)
            .filter(
                PersonLineageModel.person_subject_id == two_people["person_subject_id"],
                PersonLineageModel.person_object_id == two_people["person_object_id"],
            )
            .count()
        )
        assert count == 1

    def test_validate_rejected_submission_blocked(self, auth_headers, two_people):  # noqa
        # A rejected submission can't be validated (won't silently un-reject).
        with TestClient(app) as client:
            sub_id = self._make_resolved_submission(client, auth_headers, two_people)
            client.patch(
                f"/person_lineage_submission/{sub_id}",
                json={"status": "rejected"},
                headers=auth_headers,
            )
            v = client.post(f"/person_lineage_submission/{sub_id}/validate", headers=auth_headers)
            assert v.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_validate_self_pair_rejected(self, auth_headers, two_people):  # noqa
        # Both names resolved to the same person -> can't validate.
        with TestClient(app) as client:
            r = client.post(
                "/person_lineage_submission/",
                json={
                    "person_subject_name": "A", "person_object_name": "A",
                    "relationship": "collaborator_of", "who_sent_this": "cur",
                    "person_subject_curie_or_id": two_people["person_subject_id"],
                    "person_object_curie_or_id": two_people["person_subject_id"],
                },
                headers=auth_headers,
            )
            sub_id = r.json()["person_lineage_submission_id"]
            v = client.post(f"/person_lineage_submission/{sub_id}/validate", headers=auth_headers)
            assert v.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_list_for_person_matches_either_side(self, db, auth_headers, two_people):  # noqa
        # GET /person_lineage_submission/person/{id} returns submissions where the
        # person is resolved on either side, and ignores submissions where they
        # match only by name (id unresolved).
        s_id = two_people["person_subject_id"]
        o_id = two_people["person_object_id"]
        with TestClient(app) as client:
            # person s resolved as subject
            client.post(
                "/person_lineage_submission/",
                json={"person_subject_name": "A", "person_object_name": "B",
                      "relationship": "phd_supervisor_of", "who_sent_this": "cur",
                      "person_subject_curie_or_id": s_id, "person_object_curie_or_id": o_id},
                headers=auth_headers,
            )
            # person s resolved as object
            client.post(
                "/person_lineage_submission/",
                json={"person_subject_name": "C", "person_object_name": "D",
                      "relationship": "postdoc_supervisor_of", "who_sent_this": "cur",
                      "person_subject_curie_or_id": o_id, "person_object_curie_or_id": s_id},
                headers=auth_headers,
            )
            # name-only submission (no resolved id) must NOT be returned
            client.post(
                "/person_lineage_submission/",
                json={"person_subject_name": "E", "person_object_name": "F",
                      "relationship": "phd_supervisor_of", "who_sent_this": "cur"},
                headers=auth_headers,
            )

            by_id = client.get(f"/person_lineage_submission/person/{s_id}", headers=auth_headers)
            assert by_id.status_code == status.HTTP_200_OK
            rows = by_id.json()
            assert len(rows) == 2
            assert {r["person_subject_id"] for r in rows} == {s_id, o_id}

            curie = db.query(PersonModel).filter(PersonModel.person_id == s_id).one().curie
            by_curie = client.get(f"/person_lineage_submission/person/{curie}", headers=auth_headers)
            assert by_curie.status_code == status.HTTP_200_OK
            assert len(by_curie.json()) == 2

    def test_list_for_person_unknown_404(self, auth_headers):  # noqa
        with TestClient(app) as client:
            res = client.get("/person_lineage_submission/person/9999999", headers=auth_headers)
            assert res.status_code == status.HTTP_404_NOT_FOUND

    def test_destroy(self, auth_headers, test_submission):  # noqa
        with TestClient(app) as client:
            res = client.delete(
                f"/person_lineage_submission/{test_submission.new_id}", headers=auth_headers
            )
            assert res.status_code == status.HTTP_204_NO_CONTENT
            res = client.get(
                f"/person_lineage_submission/{test_submission.new_id}", headers=auth_headers
            )
            assert res.status_code == status.HTTP_404_NOT_FOUND
