import pytest
from starlette.testclient import TestClient
from fastapi import status, HTTPException

from agr_literature_service.api.main import app
from agr_literature_service.api.models import (
    ReferenceModel,
    IndexingPriorityModel,
    WorkflowTagModel,
    ModModel,
    TopicEntityTagSourceModel,
)
from agr_literature_service.api.crud import indexing_priority_crud as ip_crud
from ..fixtures import db  # noqa: F401
# from .fixtures import auth_headers  # noqa: F401
from .test_reference import test_reference  # noqa: F401


# ---------- helpers / fixtures ----------

def _ensure_mod(session, abbr: str) -> int:
    mod = session.query(ModModel).filter(ModModel.abbreviation == abbr).first()
    if not mod:
        mod = ModModel(abbreviation=abbr, short_name=abbr, full_name=abbr)
        session.add(mod)
        session.commit()
        session.refresh(mod)
    return mod.mod_id


def _ensure_tet_source(session, data_provider: str) -> int:
    src = (
        session.query(TopicEntityTagSourceModel)
        .filter(TopicEntityTagSourceModel.source_method == "abc_document_classifier")
        .filter(TopicEntityTagSourceModel.data_provider == data_provider)
        .first()
    )
    if not src:
        src = TopicEntityTagSourceModel(
            source_method="abc_document_classifier",
            data_provider=data_provider,
            source_url=None,
            additional_info=None,
        )
        session.add(src)
        session.commit()
        session.refresh(src)
    return src.topic_entity_tag_source_id


def _ensure_preindexing_wft(session, ref_curie: str, mod_abbr: str) -> int:
    """Create ATP:0000306 workflow tag for (ref, mod)."""
    ref_id = (
        session.query(ReferenceModel.reference_id)
        .filter(ReferenceModel.curie == ref_curie)
        .scalar()
    )
    mod_id = _ensure_mod(session, mod_abbr)
    wft = (
        session.query(WorkflowTagModel)
        .filter(WorkflowTagModel.reference_id == ref_id)
        .filter(WorkflowTagModel.mod_id == mod_id)
        .filter(WorkflowTagModel.workflow_tag_id == "ATP:0000306")
        .first()
    )
    if not wft:
        wft = WorkflowTagModel(
            reference_id=ref_id,
            mod_id=mod_id,
            workflow_tag_id="ATP:0000306",
        )
        session.add(wft)
        session.commit()
        session.refresh(wft)
    return wft.reference_workflow_tag_id


@pytest.fixture
def make_mod_and_source(db):  # noqa: F811
    """Factory to ensure MOD + TET source exist."""
    def _make(abbr: str) -> int:
        _ensure_mod(db, abbr)
        _ensure_tet_source(db, abbr)
        return 1
    return _make


# ---------- tests ----------

class TestIndexingPriorityCRUD:
    def test_create_success_and_show(self, db, auth_headers, test_reference, make_mod_and_source):  # noqa: F811
        with TestClient(app) as client:
            ref_curie = test_reference.new_ref_curie
            mod_abbr = "ZFIN"
            make_mod_and_source(mod_abbr)

            payload = {
                "reference_curie": ref_curie,
                "mod_abbreviation": mod_abbr,
                "indexing_priority": "ATP:0000211",
                "confidence_score": 0.87,
                "validation_by_biocurator": False,
            }
            r = client.post("/indexing_priority/", json=payload, headers=auth_headers)
            assert r.status_code == status.HTTP_201_CREATED
            new_id = int(r.json())

            r2 = client.get(f"/indexing_priority/{new_id}")
            assert r2.status_code == status.HTTP_200_OK
            data = r2.json()
            assert data["indexing_priority_id"] == new_id
            assert data["reference_curie"] == ref_curie
            assert data["mod_abbreviation"] == mod_abbr
            assert "updated_by_email" in data

    def test_create_duplicate(self, db, auth_headers, test_reference, make_mod_and_source):  # noqa: F811
        with TestClient(app) as client:
            ref_curie = test_reference.new_ref_curie
            mod_abbr = "SGD"
            make_mod_and_source(mod_abbr)

            payload = {
                "reference_curie": ref_curie,
                "mod_abbreviation": mod_abbr,
                "indexing_priority": "ATP:0000211",
                "confidence_score": 0.55,
            }
            r1 = client.post("/indexing_priority/", json=payload, headers=auth_headers)
            assert r1.status_code == status.HTTP_201_CREATED

            r2 = client.post("/indexing_priority/", json=payload, headers=auth_headers)
            assert r2.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
            assert "already exists" in r2.json()["detail"]

    def test_create_bad_ref_or_mod(self, auth_headers, make_mod_and_source):
        with TestClient(app) as client:
            payload = {
                "reference_curie": "AGRKB:NOT_A_REF",
                "mod_abbreviation": "WB",
                "indexing_priority": "ATP:0000211",
                "confidence_score": 0.5,
            }
            r = client.post("/indexing_priority/", json=payload, headers=auth_headers)
            assert r.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

            payload2 = dict(payload)
            payload2["reference_curie"] = "AGRKB:0000000001"
            payload2["mod_abbreviation"] = "NOPE"
            r2 = client.post("/indexing_priority/", json=payload2, headers=auth_headers)
            assert r2.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    def test_patch_and_show(self, db, auth_headers, test_reference, make_mod_and_source):  # noqa: F811
        with TestClient(app) as client:
            ref = test_reference.new_ref_curie
            mod1, mod2 = "FB", "MGI"
            make_mod_and_source(mod1)
            make_mod_and_source(mod2)

            payload = {
                "reference_curie": ref,
                "mod_abbreviation": mod1,
                "indexing_priority": "ATP:0000211",
                "confidence_score": 0.61,
                "validation_by_biocurator": False,
            }
            r = client.post("/indexing_priority/", json=payload, headers=auth_headers)
            assert r.status_code == status.HTTP_201_CREATED
            tag_id = int(r.json())

            patch_body = {
                "mod_abbreviation": mod2,
                "confidence_score": 0.9,
                "validation_by_biocurator": True,
            }
            r2 = client.patch(f"/indexing_priority/{tag_id}", json=patch_body, headers=auth_headers)
            assert r2.status_code == status.HTTP_202_ACCEPTED

            r3 = client.get(f"/indexing_priority/{tag_id}")
            assert r3.status_code == status.HTTP_200_OK
            data = r3.json()
            assert data["mod_abbreviation"] == mod2
            assert float(data["confidence_score"]) == pytest.approx(0.9, rel=0, abs=1e-6)
            assert data["validation_by_biocurator"] is True

    def test_destroy(self, db, auth_headers, test_reference, make_mod_and_source):  # noqa: F811
        with TestClient(app) as client:
            ref = test_reference.new_ref_curie
            mod = "XB"
            make_mod_and_source(mod)

            r = client.post(
                "/indexing_priority/",
                json={
                    "reference_curie": ref,
                    "mod_abbreviation": mod,
                    "indexing_priority": "ATP:0000211",
                    "confidence_score": 0.42,
                },
                headers=auth_headers,
            )
            tag_id = int(r.json())

            r_del = client.delete(f"/indexing_priority/{tag_id}", headers=auth_headers)
            assert r_del.status_code == status.HTTP_204_NO_CONTENT

            r_get = client.get(f"/indexing_priority/{tag_id}")
            assert r_get.status_code == status.HTTP_404_NOT_FOUND

    def test_get_ref_ids_with_indexing_priority_helper(self, db, test_reference, make_mod_and_source):  # noqa: F811
        ref_curie = test_reference.new_ref_curie
        ref_id = (
            db.query(ReferenceModel.reference_id)
            .filter(ReferenceModel.curie == ref_curie)
            .scalar()
        )

        mod_a = "RGD"
        mod_b = "WB"
        make_mod_and_source(mod_a)
        make_mod_and_source(mod_b)

        def create_tag(session, mod_abbr, prio):
            session.add(IndexingPriorityModel(
                reference_id=ref_id,
                mod_id=session.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_abbr).scalar(),
                source_id=_ensure_tet_source(session, mod_abbr),
                indexing_priority=prio,
                confidence_score=0.5,
                validation_by_biocurator=False,
            ))
            session.commit()

        create_tag(db, mod_a, "ATP:0000211")
        create_tag(db, mod_b, "ATP:0000212")

        ids_all_211 = set(ip_crud.get_ref_ids_with_indexing_priority(db, "ATP:0000211"))
        ids_mod_a_211 = set(ip_crud.get_ref_ids_with_indexing_priority(db, "ATP:0000211", mod_a))
        ids_mod_b_211 = set(ip_crud.get_ref_ids_with_indexing_priority(db, "ATP:0000211", mod_b))
        assert ref_id in ids_all_211
        assert ref_id in ids_mod_a_211
        assert ref_id not in ids_mod_b_211

    def test_set_priority_success_and_failure_paths(self, db, test_reference, make_mod_and_source):  # noqa: F811
        ref = test_reference.new_ref_curie
        mod = "ZFIN"
        make_mod_and_source(mod)
        _ensure_preindexing_wft(db, ref, mod)

        result = ip_crud.set_priority(
            db=db,
            reference_curie=ref,
            mod_abbreviation=mod,
            indexing_priority="ATP:0000211",
            confidence_score=0.777,
        )
        assert result["reference_curie"] == ref
        assert result["mod_abbreviation"] == mod
        assert result["indexing_priority"] == "ATP:0000211"

        wft_row = (
            db.query(WorkflowTagModel)
            .join(ReferenceModel, WorkflowTagModel.reference_id == ReferenceModel.reference_id)
            .join(ModModel, WorkflowTagModel.mod_id == ModModel.mod_id)
            .filter(ReferenceModel.curie == ref, ModModel.abbreviation == mod)
            .filter(WorkflowTagModel.workflow_tag_id.in_(["ATP:0000303", "ATP:0000304", "ATP:0000306"]))
            .order_by(WorkflowTagModel.reference_workflow_tag_id.desc())
            .first()
        )
        assert wft_row.workflow_tag_id == "ATP:0000303"

        _ensure_preindexing_wft(db, ref, mod)  # fresh ATP:0000306 again
        with pytest.raises(HTTPException):
            ip_crud.set_priority(
                db=db,
                reference_curie=ref,
                mod_abbreviation=mod,
                indexing_priority="ATP:0000211",
                confidence_score=0.9,
            )
        wft_row2 = (
            db.query(WorkflowTagModel)
            .join(ReferenceModel, WorkflowTagModel.reference_id == ReferenceModel.reference_id)
            .join(ModModel, WorkflowTagModel.mod_id == ModModel.mod_id)
            .filter(ReferenceModel.curie == ref, ModModel.abbreviation == mod)
            .filter(WorkflowTagModel.workflow_tag_id.in_(["ATP:0000303", "ATP:0000304"]))
            .order_by(WorkflowTagModel.reference_workflow_tag_id.desc())
            .first()
        )
        assert wft_row2.workflow_tag_id == "ATP:0000304"

    def test_get_indexing_priority_tag_shapes_and_names(self, db, auth_headers, test_reference, make_mod_and_source):  # noqa: F811
        ref = test_reference.new_ref_curie
        mod = "SGD"
        make_mod_and_source(mod)

        with TestClient(app) as client:
            r = client.post(
                "/indexing_priority/",
                json={
                    "reference_curie": ref,
                    "mod_abbreviation": mod,
                    "indexing_priority": "ATP:0000211",
                    "confidence_score": 0.33,
                },
                headers=auth_headers,
            )
            assert r.status_code == status.HTTP_201_CREATED

        out = ip_crud.get_indexing_priority_tag(db, ref)
        assert "current_priority_tag" in out
        assert "all_priority_tags" in out
        assert isinstance(out["current_priority_tag"], list)
        assert len(out["current_priority_tag"]) >= 1

        row = out["current_priority_tag"][0]
        for key in [
            "indexing_priority_id",
            "indexing_priority",
            "indexing_priority_name",
            "confidence_score",
            "validation_by_biocurator",
            "reference_curie",
            "mod_abbreviation",
            "updated_by_email",
            "date_updated",
        ]:
            assert key in row

        assert row["indexing_priority_name"]
