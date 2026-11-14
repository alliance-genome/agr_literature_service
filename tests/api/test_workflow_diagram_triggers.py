"""What starts each workflow, for the workflow diagram (workflow_diagram_triggers)."""
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import status
from starlette.testclient import TestClient

from agr_literature_service.api.crud import ateam_db_helpers, workflow_diagram_triggers as triggers
from agr_literature_service.api.main import app
from agr_literature_service.api.models import ModModel, WorkflowTransitionModel
from ..fixtures import load_name_to_atp_and_relationships_mock
from ..fixtures import db  # noqa
from .fixtures import auth_headers  # noqa

FILES_UPLOADED = "ATP:0000134"
TEXT_CONVERSION_NEEDED = "ATP:0000162"
CONVERTED_TO_TEXT = "ATP:0000163"


def _transition(transition_to, actions):
    return SimpleNamespace(transition_to=transition_to, actions=actions)


def _atp_id_by_name(name, fallback=None):
    """get_atp_id_by_name against the mocked ontology: the mock rebinds
    ateam_db_helpers.name_to_atp, which workflow_tag_crud imported earlier."""
    if name in ateam_db_helpers.name_to_atp:
        return ateam_db_helpers.name_to_atp[name]
    if fallback:
        return fallback
    raise ValueError(name)


@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       load_name_to_atp_and_relationships_mock)
@patch.object(triggers, "get_atp_id_by_name", _atp_id_by_name)
def test_proceed_on_value_actions_become_handoffs():
    load_name_to_atp_and_relationships_mock()
    handoffs = triggers._proceed_on_value_handoffs([
        # two transitions into the same state carry the same action: one handoff
        _transition(FILES_UPLOADED, ["proceed_on_value::category::research_article::ATP:0000162"]),
        _transition(FILES_UPLOADED, ["proceed_on_value::category::research_article::ATP:0000162",
                                     "proceed_on_value::category::review_article::ATP:0000162"]),
        # a target named by its workflow
        _transition(CONVERTED_TO_TEXT, ["proceed_on_value::reference_type::Experimental::curation classification",
                                        "proceed_on_value::all::ATP:0000358"]),
        # other actions and unknown workflows are not handoffs
        _transition(CONVERTED_TO_TEXT, ["sub_task_complete::entity extraction",
                                        "proceed_on_value::all::no such workflow"]),
        _transition("ATP:0000141", None),
    ])
    assert handoffs == {
        (FILES_UPLOADED, TEXT_CONVERSION_NEEDED): ["research articles", "review articles"],
        (CONVERTED_TO_TEXT, "ATP:0000311"): ["Experimental papers"],
        (CONVERTED_TO_TEXT, "ATP:0000358"): [None],
    }


def test_conditions_join_and_an_unconditional_handoff_wins():
    assert triggers._join_conditions(["research articles", "review articles"]) == \
        "research articles or review articles"
    assert triggers._join_conditions(["research articles", None]) is None


def test_entry_points_per_mod():
    def targets(mod):
        return {(source, curie) for source, curie, _condition in triggers._entry_points(mod)}

    # every MOD but the Alliance gets "file needed" on corpus entry
    assert ("corpus_entry", "ATP:0000141") in targets("FB")
    assert targets("AGR") == set()
    # ZFIN's abstract classifiers start on corpus entry too
    assert {("corpus_entry", "ATP:0000306"), ("corpus_entry", "ATP:0000380")} <= targets("ZFIN")
    # SGD manual indexing: on corpus entry, and from the sort page
    assert {("corpus_entry", "ATP:0000274"), ("sort_page", "ATP:0000273")} <= targets("SGD")
    # WB author workflows: the sort page checkbox and the PubMed update
    assert {("sort_page", "ATP:0000109"), ("pubmed_update", "ATP:0000389")} <= targets("WB")
    assert not {curie for _source, curie in targets("FB")} & {"ATP:0000109", "ATP:0000389", "ATP:0000274"}
    # email extraction starts on corpus entry for the MODs that extract emails (SCRUM-6845)
    for mod in ("FB", "SGD", "WB", "XB", "ZFIN"):
        assert ("corpus_entry", "ATP:0000358") in targets(mod)
    assert ("corpus_entry", "ATP:0000358") not in targets("MGI") | targets("RGD")


@patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
       load_name_to_atp_and_relationships_mock)
def test_first_pass_curation_starts_once_three_workflows_complete():
    load_name_to_atp_and_relationships_mock()
    handoffs = triggers._first_pass_curation_handoffs()
    assert set(handoffs) == {("ATP:0000174", "ATP:0000371"), ("ATP:0000169", "ATP:0000371"),
                             ("ATP:0000312", "ATP:0000371")}


class TestWorkflowDiagramTriggersApi:

    def test_unknown_mod_returns_404(self, db, auth_headers):  # noqa
        with TestClient(app) as client:
            response = client.get(url="/workflow_tag/workflow_diagram_triggers/NO_SUCH_MOD",
                                  headers=auth_headers)
            assert response.status_code == status.HTTP_404_NOT_FOUND

    @patch("agr_literature_service.api.crud.ateam_db_helpers.load_name_to_atp_and_relationships",
           load_name_to_atp_and_relationships_mock)
    def test_triggers_for_a_mod(self, db, auth_headers):  # noqa
        load_name_to_atp_and_relationships_mock()
        mod = db.query(ModModel).filter_by(abbreviation="FB").one_or_none()
        if mod is None:
            mod = ModModel(abbreviation="FB", short_name="FB", full_name="FlyBase")
            db.add(mod)
            db.commit()
        db.add(WorkflowTransitionModel(
            mod=mod, transition_from=TEXT_CONVERSION_NEEDED, transition_to=CONVERTED_TO_TEXT,
            actions=["proceed_on_value::category::research_article::curation classification"]))
        db.commit()

        processes = {"ATP:0000141": "ATP:0000140", CONVERTED_TO_TEXT: "ATP:0000161", "ATP:0000311": "ATP:0000311"}
        names = {"ATP:0000140": "file upload", "ATP:0000141": "file needed", "ATP:0000161": "text conversion",
                 CONVERTED_TO_TEXT: "file converted to text", "ATP:0000311": "curation classification"}
        with TestClient(app) as client, \
                patch.object(triggers, "get_map_ateam_curies_to_names", return_value=dict(names)), \
                patch.object(triggers, "_resolve_process_hierarchy", return_value=(processes, {})), \
                patch.object(triggers, "get_atp_id_by_name", _atp_id_by_name):
            response = client.get(url="/workflow_tag/workflow_diagram_triggers/FB", headers=auth_headers)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        file_upload = [e for e in data["entry_points"] if e["to_state"] == "ATP:0000141"]
        assert file_upload == [{
            "source": "corpus_entry", "source_label": "Paper enters corpus",
            "to_state": "ATP:0000141", "to_state_name": "file needed",
            "to_process": "ATP:0000140", "to_process_name": "file upload", "condition": None,
        }]
        curation = [h for h in data["handoffs"] if h["to_state"] == "ATP:0000311"]
        assert curation == [{
            "from_state": CONVERTED_TO_TEXT, "from_state_name": "file converted to text",
            "from_process": "ATP:0000161", "from_process_name": "text conversion",
            "to_state": "ATP:0000311", "to_state_name": "curation classification",
            "to_process": "ATP:0000311", "to_process_name": "curation classification",
            "condition": "research articles",
        }]
        # FB first pass curation is started in code, not by a transition
        assert {h["to_state"] for h in data["handoffs"]} >= {"ATP:0000371"}
