import pytest
from fastapi import HTTPException

from agr_literature_service.api.models import (
    ReferenceModel,
    IndexingPriorityModel,
    WorkflowTagModel,
    ModModel,
    TopicEntityTagSourceModel,
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
      - get_name_to_atp_for_all_children -> fixed name map
      - wft_patch -> simple in-DB update of WorkflowTagModel.workflow_tag_id
    """
    def _fake_get_workflow_tags_from_process(process_atp_id):
        # Pretend the process ATP:0000210 has these two child tags
        return ["ATP:0000211", "ATP:0000212"]

    def _fake_get_name_to_atp_for_all_children(process_atp_id):
        # ip_crud only uses the 2nd return (atp_to_name)
        name_to_atp = {}
        atp_to_name = {
            "ATP:0000211": "High Priority",
            "ATP:0000212": "Medium Priority",
        }
        return name_to_atp, atp_to_name

    monkeypatch.setattr(ip_crud, "get_workflow_tags_from_process", _fake_get_workflow_tags_from_process)
    monkeypatch.setattr(ip_crud, "get_name_to_atp_for_all_children", _fake_get_name_to_atp_for_all_children)

    yield
    # nothing to unpatch explicitly (monkeypatch handles teardown)


@pytest.fixture(autouse=True)
def patch_wft_patch(monkeypatch):
    """
    Replace ip_crud.wft_patch with a minimal in-DB updater so set_priority
    can flip ATP:0000306 -> {success,failed} without depending on router logic.
    """
    def _fake_wft_patch(db, reference_workflow_tag_id, update_dict):
        wft = (
            db.query(WorkflowTagModel)
            .filter(WorkflowTagModel.reference_workflow_tag_id == reference_workflow_tag_id)
            .first()
        )
        if not wft:
            # Mirror your real patch behavior with an HTTPException if needed
            raise HTTPException(status_code=404, detail="workflow tag not found")
        if "workflow_tag_id" in update_dict:
            wft.workflow_tag_id = update_dict["workflow_tag_id"]
        db.add(wft)
        db.commit()

    monkeypatch.setattr(ip_crud, "wft_patch", _fake_wft_patch)
    yield


# ---------- helpers ----------

def _ensure_mod(session, abbr: str) -> int:
    mod = session.query(ModModel).filter(ModModel.abbreviation == abbr).first()
    if not mod:
        mod = ModModel(abbreviation=abbr, short_name=abbr, full_name=abbr)
        session.add(mod)
        session.commit()
        session.refresh(mod)
    return mod.mod_id


def _ensure_tet_source(session, data_provider: str) -> int:
    # First ensure the MOD exists for the data_provider
    mod_id = _ensure_mod(session, data_provider)

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
            secondary_data_provider_id=mod_id,
            source_evidence_assertion="automated_assertion",  # Provide a default value
        )
        session.add(src)
        session.commit()
        session.refresh(src)
    return src.topic_entity_tag_source_id


def _ensure_preindexing_wft(session, ref_curie: str, mod_abbr: str) -> int:
    """Create ATP:0000306 workflow tag for (ref, mod) if missing."""
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


def _mk_payload(ref_curie: str, mod_abbr: str, ip_code: str = "ATP:0000211", score: float = 0.5):
    return IndexingPrioritySchemaPost(
        reference_curie=ref_curie,
        mod_abbreviation=mod_abbr,
        indexing_priority=ip_code,
        confidence_score=score,
    )


# ---------- tests ----------

class TestIndexingPriorityCRUD:
    def test_create_and_show(self, db, test_reference): # noqa
        ref_curie = test_reference.new_ref_curie
        mod_abbr = "ZFIN"
        _ensure_mod(db, mod_abbr)
        _ensure_tet_source(db, mod_abbr)

        new_id = ip_crud.create(db, _mk_payload(ref_curie, mod_abbr, score=0.87))
        assert isinstance(new_id, int)

        data = ip_crud.show(db, new_id)
        assert data["indexing_priority_id"] == new_id
        assert data["reference_curie"] == ref_curie
        assert data["mod_abbreviation"] == mod_abbr
        assert "updated_by_email" in data

    def test_patch_and_show(self, db, test_reference): # noqa
        ref_curie = test_reference.new_ref_curie
        mod1, mod2 = "FB", "MGI"
        _ensure_mod(db, mod1)
        _ensure_mod(db, mod2)
        _ensure_tet_source(db, mod1)
        _ensure_tet_source(db, mod2)

        tag_id = ip_crud.create(db, _mk_payload(ref_curie, mod1, score=0.61))

        ip_crud.patch(
            db,
            tag_id,
            {
                "mod_abbreviation": mod2,
                "confidence_score": 0.9,
                "validation_by_biocurator": True,
            },
        )

        data = ip_crud.show(db, tag_id)
        assert data["mod_abbreviation"] == mod2
        assert float(data["confidence_score"]) == pytest.approx(0.9, rel=0, abs=1e-6)

        # Handle both string and boolean representations
        validation_value = data["validation_by_biocurator"]
        if isinstance(validation_value, str):
            # If it's a string, check for 'true' (case-insensitive)
            assert validation_value.lower() == 'true'
        else:
            # If it's a boolean, check for True
            assert validation_value is True

    def test_create_duplicate(self, db, test_reference): # noqa
        ref_curie = test_reference.new_ref_curie
        mod_abbr = "SGD"
        _ensure_mod(db, mod_abbr)
        _ensure_tet_source(db, mod_abbr)

        ip_crud.create(db, _mk_payload(ref_curie, mod_abbr))
        with pytest.raises(HTTPException) as ei:
            ip_crud.create(db, _mk_payload(ref_curie, mod_abbr))
        assert ei.value.status_code == 422
        assert "already exists" in ei.value.detail

    def test_create_bad_ref_or_mod(self, db): # noqa
        # bad reference
        mod_abbr = "WB"
        _ensure_mod(db, mod_abbr)
        _ensure_tet_source(db, mod_abbr)
        with pytest.raises(HTTPException) as ei1:
            ip_crud.create(db, _mk_payload("AGRKB:NOT_A_REF", mod_abbr))
        assert ei1.value.status_code == 422

        # bad MOD
        with pytest.raises(HTTPException) as ei2:
            ip_crud.create(db, _mk_payload("AGRKB:0000000001", "NOPE"))
        assert ei2.value.status_code == 422

    def test_destroy(self, db, test_reference): # noqa
        ref_curie = test_reference.new_ref_curie
        mod_abbr = "XB"
        _ensure_mod(db, mod_abbr)
        _ensure_tet_source(db, mod_abbr)

        tag_id = ip_crud.create(db, _mk_payload(ref_curie, mod_abbr, score=0.42))
        ip_crud.destroy(db, tag_id)

        with pytest.raises(HTTPException) as ei:
            ip_crud.show(db, tag_id)
        assert ei.value.status_code == 404

    def test_get_ref_ids_with_indexing_priority_helper(self, db, test_reference): # noqa
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
            _ensure_tet_source(db, m)

        db.add(IndexingPriorityModel(
            reference_id=ref_id,
            mod_id=db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_a).scalar(),
            source_id=_ensure_tet_source(db, mod_a),
            indexing_priority="ATP:0000211",
            confidence_score=0.5,
            validation_by_biocurator=False,
        ))
        db.add(IndexingPriorityModel(
            reference_id=ref_id,
            mod_id=db.query(ModModel.mod_id).filter(ModModel.abbreviation == mod_b).scalar(),
            source_id=_ensure_tet_source(db, mod_b),
            indexing_priority="ATP:0000212",
            confidence_score=0.5,
            validation_by_biocurator=False,
        ))
        db.commit()

        ids_all_211 = set(ip_crud.get_ref_ids_with_indexing_priority(db, "ATP:0000211"))
        ids_mod_a_211 = set(ip_crud.get_ref_ids_with_indexing_priority(db, "ATP:0000211", mod_a))
        ids_mod_b_211 = set(ip_crud.get_ref_ids_with_indexing_priority(db, "ATP:0000211", mod_b))
        assert ref_id in ids_all_211
        assert ref_id in ids_mod_a_211
        assert ref_id not in ids_mod_b_211

    def test_set_priority_success_and_failure_paths(self, db, test_reference): # noqa
        ref_curie = test_reference.new_ref_curie
        mod_abbr = "ZFIN"
        _ensure_mod(db, mod_abbr)
        _ensure_tet_source(db, mod_abbr)
        _ensure_preindexing_wft(db, ref_curie, mod_abbr)

        result = ip_crud.set_priority(
            db=db,
            reference_curie=ref_curie,
            mod_abbreviation=mod_abbr,
            indexing_priority="ATP:0000211",
            confidence_score=0.777,
        )
        assert result["reference_curie"] == ref_curie
        assert result["mod_abbreviation"] == mod_abbr
        assert result["indexing_priority"] == "ATP:0000211"

        wft_row = (
            db.query(WorkflowTagModel)
            .join(ReferenceModel, WorkflowTagModel.reference_id == ReferenceModel.reference_id)
            .join(ModModel, WorkflowTagModel.mod_id == ModModel.mod_id)
            .filter(ReferenceModel.curie == ref_curie, ModModel.abbreviation == mod_abbr)
            .filter(WorkflowTagModel.workflow_tag_id.in_(["ATP:0000303", "ATP:0000304", "ATP:0000306"]))
            .order_by(WorkflowTagModel.reference_workflow_tag_id.desc())
            .first()
        )
        assert wft_row.workflow_tag_id == "ATP:0000303"

        _ensure_preindexing_wft(db, ref_curie, mod_abbr)  # fresh ATP:0000306 again
        with pytest.raises(HTTPException):
            ip_crud.set_priority(
                db=db,
                reference_curie=ref_curie,
                mod_abbreviation=mod_abbr,
                indexing_priority="ATP:0000211",
                confidence_score=0.9,
            )
        wft_row2 = (
            db.query(WorkflowTagModel)
            .join(ReferenceModel, WorkflowTagModel.reference_id == ReferenceModel.reference_id)
            .join(ModModel, WorkflowTagModel.mod_id == ModModel.mod_id)
            .filter(ReferenceModel.curie == ref_curie, ModModel.abbreviation == mod_abbr)
            .filter(WorkflowTagModel.workflow_tag_id.in_(["ATP:0000303", "ATP:0000304"]))
            .order_by(WorkflowTagModel.reference_workflow_tag_id.desc())
            .first()
        )
        assert wft_row2.workflow_tag_id == "ATP:0000304"

    def test_get_indexing_priority_tag_shapes_and_names(self, db, test_reference): # noqa
        ref_curie = test_reference.new_ref_curie
        mod_abbr = "SGD"
        _ensure_mod(db, mod_abbr)
        _ensure_tet_source(db, mod_abbr)

        ip_id = ip_crud.create(db, _mk_payload(ref_curie, mod_abbr, "ATP:0000211", 0.33))
        assert isinstance(ip_id, int)

        out = ip_crud.get_indexing_priority_tag(db, ref_curie)
        assert "current_priority_tag" in out
        assert "all_priority_tags" in out
        assert isinstance(out["current_priority_tag"], dict)
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
