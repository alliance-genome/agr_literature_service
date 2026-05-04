import pytest
from fastapi import HTTPException

from agr_literature_service.api.models import (
    ReferenceModel,
    IndexingPriorityModel,
    ModModel,
)
from agr_literature_service.api.crud import indexing_priority_crud as ip_crud
from agr_literature_service.api.schemas.indexing_priority_schemas import (
    IndexingPrioritySchemaPost,
)
from ..fixtures import db as _db_fixture  # noqa: F401
from .fixtures import auth_headers as _auth_headers_fixture  # noqa: F401
from .test_reference import test_reference as _test_reference_fixture  # noqa: F401

db = _db_fixture
auth_headers = _auth_headers_fixture
test_reference = _test_reference_fixture

# ---------- monkeypatch app dependencies so tests are self-contained ----------


@pytest.fixture(autouse=True)
def patch_ip_helpers(monkeypatch):
    """
    Make ip_crud independent of seeded ATP/workflow data:
      - get_workflow_tags_from_process -> fixed list of ATP codes
      - get_name_to_atp_for_descendants -> fixed name map
    """
    def _fake_get_workflow_tags_from_process(process_atp_id):
        # Pretend the process ATP:0000210 has these two child tags
        return ["ATP:0000211", "ATP:0000212"]

    def _fake_get_name_to_atp_for_descendants(process_atp_id):
        # ip_crud only uses the 2nd return (atp_to_name)
        name_to_atp = {}
        atp_to_name = {
            "ATP:0000211": "High Priority",
            "ATP:0000212": "Medium Priority",
        }
        return name_to_atp, atp_to_name

    monkeypatch.setattr(ip_crud, "get_workflow_tags_from_process", _fake_get_workflow_tags_from_process)
    monkeypatch.setattr(ip_crud, "get_name_to_atp_for_descendants", _fake_get_name_to_atp_for_descendants)

    yield
    # nothing to unpatch explicitly (monkeypatch handles teardown)


# ---------- helpers ----------

def _ensure_mod(session, abbr: str) -> int:
    mod = session.query(ModModel).filter(ModModel.abbreviation == abbr).first()
    if not mod:
        mod = ModModel(abbreviation=abbr, short_name=abbr, full_name=abbr)
        session.add(mod)
        session.commit()
        session.refresh(mod)
    return mod.mod_id


def _mk_payload(ref_curie: str, mod_abbr: str, ip_code: str = "ATP:0000211", score: float = 0.5):
    return IndexingPrioritySchemaPost(
        reference_curie=ref_curie,
        mod_abbreviation=mod_abbr,
        predicted_indexing_priority=ip_code,
        confidence_score=score,
    )


# ---------- tests ----------

class TestIndexingPriorityCRUD:
    def test_create_and_show(self, db, test_reference):  # noqa
        ref_curie = test_reference.new_ref_curie
        mod_abbr = "ZFIN"
        _ensure_mod(db, mod_abbr)

        new_id = ip_crud.create(db, _mk_payload(ref_curie, mod_abbr, score=0.87))
        assert isinstance(new_id, int)

        data = ip_crud.show(db, new_id)
        assert data["indexing_priority_id"] == new_id
        assert data["reference_curie"] == ref_curie
        assert data["mod_abbreviation"] == mod_abbr
        assert "updated_by_email" in data

    def test_patch_and_show(self, db, test_reference):  # noqa
        ref_curie = test_reference.new_ref_curie
        mod1, mod2 = "FB", "MGI"
        _ensure_mod(db, mod1)
        _ensure_mod(db, mod2)

        tag_id = ip_crud.create(db, _mk_payload(ref_curie, mod1, score=0.61))

        ip_crud.patch(
            db,
            tag_id,
            {
                "mod_abbreviation": mod2,
                "confidence_score": 0.9,
                "curator_indexing_priority": "ATP:0000211",
            },
        )

        data = ip_crud.show(db, tag_id)
        assert data["mod_abbreviation"] == mod2
        assert float(data["confidence_score"]) == pytest.approx(0.9, rel=0, abs=1e-6)
        assert data["curator_indexing_priority"] == "ATP:0000211"

    def test_create_duplicate(self, db, test_reference):  # noqa
        ref_curie = test_reference.new_ref_curie
        mod_abbr = "SGD"
        _ensure_mod(db, mod_abbr)

        ip_crud.create(db, _mk_payload(ref_curie, mod_abbr))
        with pytest.raises(HTTPException) as ei:
            ip_crud.create(db, _mk_payload(ref_curie, mod_abbr))
        assert ei.value.status_code == 422
        assert "already exists" in ei.value.detail

    def test_create_bad_ref_or_mod(self, db):  # noqa
        # bad reference
        mod_abbr = "WB"
        _ensure_mod(db, mod_abbr)
        with pytest.raises(HTTPException) as ei1:
            ip_crud.create(db, _mk_payload("AGRKB:NOT_A_REF", mod_abbr))
        assert ei1.value.status_code == 422

        # bad MOD
        with pytest.raises(HTTPException) as ei2:
            ip_crud.create(db, _mk_payload("AGRKB:0000000001", "NOPE"))
        assert ei2.value.status_code == 422

    def test_destroy(self, db, test_reference):  # noqa
        ref_curie = test_reference.new_ref_curie
        mod_abbr = "XB"
        _ensure_mod(db, mod_abbr)

        tag_id = ip_crud.create(db, _mk_payload(ref_curie, mod_abbr, score=0.42))
        ip_crud.destroy(db, tag_id)

        with pytest.raises(HTTPException) as ei:
            ip_crud.show(db, tag_id)
        assert ei.value.status_code == 404

    def test_get_ref_ids_with_indexing_priority_helper(self, db, test_reference):  # noqa
        ref_curie = test_reference.new_ref_curie
        ref_id = (
            db.query(ReferenceModel.reference_id)
            .filter(ReferenceModel.curie == ref_curie)
            .scalar()
        )

        mod_a = "RGD"
        mod_b = "WB"
        for m in (mod_a, mod_b):
            _ensure_mod(db, m)

        db.add(IndexingPriorityModel(
            reference_id=ref_id,
            mod_id=db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_a).scalar(),
            predicted_indexing_priority="ATP:0000211",
            confidence_score=0.5,
        ))
        db.add(IndexingPriorityModel(
            reference_id=ref_id,
            mod_id=db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_b).scalar(),
            predicted_indexing_priority="ATP:0000212",
            confidence_score=0.5,
        ))
        db.commit()

        ids_all_211 = set(ip_crud.get_ref_ids_with_indexing_priority(db, "ATP:0000211"))
        ids_mod_a_211 = set(ip_crud.get_ref_ids_with_indexing_priority(db, "ATP:0000211", mod_a))
        ids_mod_b_211 = set(ip_crud.get_ref_ids_with_indexing_priority(db, "ATP:0000211", mod_b))
        assert ref_id in ids_all_211
        assert ref_id in ids_mod_a_211
        assert ref_id not in ids_mod_b_211

    def test_get_indexing_priority_tag_shapes_and_names(self, db, test_reference):  # noqa
        ref_curie = test_reference.new_ref_curie
        mod_abbr = "SGD"
        _ensure_mod(db, mod_abbr)

        ip_id = ip_crud.create(db, _mk_payload(ref_curie, mod_abbr, "ATP:0000211", 0.33))
        assert isinstance(ip_id, int)

        out = ip_crud.get_indexing_priority_tag(db, ref_curie)
        assert "current_priority_tag" in out
        assert "all_priority_tags" in out
        assert isinstance(out["current_priority_tag"], dict)
        assert len(out["current_priority_tag"]) >= 1

        row = out["current_priority_tag"]
        for key in [
            "indexing_priority_id",
            "predicted_indexing_priority",
            "predicted_indexing_priority_name",
            "confidence_score",
            "curator_indexing_priority",
            "reference_curie",
            "mod_abbreviation",
            "updated_by_email",
            "date_updated",
        ]:
            assert key in row

        assert row["predicted_indexing_priority_name"]
