"""SCRUM-6456: the curation statuses the WB curation status form loaded move from the ABC source to the
form source; everything else keeps its association."""
import importlib.util
from pathlib import Path
from typing import Any, cast

from agr_literature_service.api.models import CurationStatusModel, CurationStatusSourceAssociationModel, \
    ModModel, TagSourceModel
from agr_literature_service.api.user import _set_current_user, get_current_agent_id, get_global_user_id, \
    set_global_user_id
from ..data_ingest.utils.test_author import _make_reference
from ...fixtures import db  # noqa

# ticket-prefixed file name, so load it by path (see test_mark_article_plus_supplement_main)
_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "agr_literature_service" / "lit_processing" / "oneoff_scripts"
    / "SCRUM-6456_attribute_wb_curation_status_form.py"
)
_spec = importlib.util.spec_from_file_location("attribute_wb_curation_status_form", _SCRIPT)
assert _spec is not None and _spec.loader is not None
script = importlib.util.module_from_spec(_spec)
cast(Any, _spec.loader).exec_module(script)
attribute_to_form = cast(Any, script).attribute_to_form

MOD = "WBCSF6456"


def _source(db, mod_id, method):  # noqa
    source = TagSourceModel(source_evidence_assertion="ATP:0000036", source_method=method,
                            validation_type="professional_biocurator", data_provider=MOD,
                            secondary_data_provider_id=mod_id, description=f"{method} test source")
    db.add(source)
    db.commit()
    return source.tag_source_id


def _status(db, reference_id, mod_id, source_id, creator, topic, value, association_value=None):  # noqa
    """A curation_status row with one association, both written by `creator`."""
    set_global_user_id(db, creator)
    row = CurationStatusModel(topic=topic, reference_id=reference_id, mod_id=mod_id, curation_status=value)
    db.add(row)
    db.commit()
    db.add(CurationStatusSourceAssociationModel(
        curation_status_id=row.curation_status_id, tag_source_id=source_id,
        curation_status=association_value or value))
    db.commit()
    return row.curation_status_id


def _source_of(db, curation_status_id):  # noqa
    return sorted(source_id for (source_id,) in db.query(CurationStatusSourceAssociationModel.tag_source_id).filter(
        CurationStatusSourceAssociationModel.curation_status_id == curation_status_id))


def test_attribute_to_form(db):  # noqa
    saved_user = (get_global_user_id(), get_current_agent_id())
    try:
        mod = ModModel(abbreviation=MOD, short_name=MOD, full_name="SCRUM-6456 test MOD")
        db.add(mod)
        db.commit()
        mod_id = mod.mod_id
        abc = _source(db, mod_id, script.ABC_SOURCE_METHOD)
        form = _source(db, mod_id, script.FORM_SOURCE_METHOD)
        rid = _make_reference(db, "AGRKB:CSF6456-1").reference_id

        # loaded from the form: moved
        wont_curate = _status(db, rid, mod_id, abc, "WBPerson6456", "ATP:0000010", script.WONT_CURATE)
        positive_061 = _status(db, rid, mod_id, abc, "WBPerson6456", "ATP:0000061", script.POSITIVE_NOT_IN_OA)
        # not form rows: left alone
        positive_other_topic = _status(db, rid, mod_id, abc, "WBPerson6456", "ATP:0000011",
                                       script.POSITIVE_NOT_IN_OA)
        pipeline = _status(db, rid, mod_id, abc, "pubmed_search_new_references", "ATP:0000012", script.WONT_CURATE)
        # a form row whose ABC association differs from the row: skipped
        edited = _status(db, rid, mod_id, abc, "WBPerson6456", "ATP:0000013", script.WONT_CURATE,
                         association_value="ATP:0000236")
        # a form row a curator edited in the ABC: row and ABC association change together and still agree,
        # but the updater is no longer the loader: skipped
        edited_in_abc = _status(db, rid, mod_id, abc, "WBPerson6456", "ATP:0000062", script.POSITIVE_NOT_IN_OA)
        set_global_user_id(db, "curator_scrum6456")
        db.get(CurationStatusModel, edited_in_abc).note = "curated for GO"
        db.query(CurationStatusSourceAssociationModel).filter(
            CurationStatusSourceAssociationModel.curation_status_id == edited_in_abc).one().note = "curated for GO"
        db.commit()
        # a form row that already has the form source: skipped
        both = _status(db, rid, mod_id, abc, "WBPerson6456", "ATP:0000014", script.WONT_CURATE)
        db.add(CurationStatusSourceAssociationModel(curation_status_id=both, tag_source_id=form,
                                                    curation_status=script.WONT_CURATE))
        db.commit()

        set_global_user_id(db, "SCRUM-6456_attribute_wb_curation_status_form")
        # a dry run counts and writes nothing
        counts = attribute_to_form(db, mod_abbreviation=MOD)
        assert counts["move: total"] == 2
        assert counts["skipped: ABC association differs from the row"] == 1
        assert counts["skipped: edited after the load"] == 1
        assert counts["skipped: already has the form source"] == 1
        assert _source_of(db, wont_curate) == [abc]

        counts = attribute_to_form(db, mod_abbreviation=MOD, apply=True, batch_size=1)
        assert counts["move: total"] == 2
        assert _source_of(db, wont_curate) == [form]
        assert _source_of(db, positive_061) == [form]
        for untouched in (positive_other_topic, pipeline, edited, edited_in_abc):
            assert _source_of(db, untouched) == [abc]
        assert _source_of(db, both) == sorted([abc, form])
        # the association kept its reported value; the curation_status row is unchanged
        moved = db.query(CurationStatusSourceAssociationModel).filter(
            CurationStatusSourceAssociationModel.curation_status_id == wont_curate).one()
        assert moved.curation_status == script.WONT_CURATE
        assert db.get(CurationStatusModel, wont_curate).curation_status == script.WONT_CURATE

        # nothing left to move
        assert attribute_to_form(db, mod_abbreviation=MOD)["move: total"] == 0
    finally:
        _set_current_user(*saved_user)
