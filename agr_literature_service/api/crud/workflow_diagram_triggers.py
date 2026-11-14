"""Where a MOD's workflows start, for the workflow diagram.

The diagram's transitions (``workflow_tag_crud.get_workflow_tag_diagram``) only
move a paper between the states of one workflow. What starts each workflow is
recorded elsewhere, and is returned here so the diagram need not hard-code it:

* handoffs -- a state of one workflow starts another. Most are
  ``proceed_on_value`` actions on a transition into that state, read from the
  MOD's workflow_transition rows (e.g. FB "file converted to text" starts
  curation classification for research articles). FlyBase's first pass
  curation is started in code once entity extraction, reference classification
  and curation classification are all complete (``first_pass_curation``).
* entry points -- an event outside every workflow starts one: a paper entering
  the MOD's corpus, a curator's choice on the sort page, the PubMed update. These
  live in code, so they are listed from the same constants that code uses.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.mod_corpus_association_crud import file_needed_tag_atp_id
from agr_literature_service.api.crud.topic_entity_tag_utils import get_map_ateam_curies_to_names
from agr_literature_service.api.crud.utils.author_review import AUTHOR_REVIEW_MODS, AUTHOR_REVIEW_NEEDED
from agr_literature_service.api.crud.utils.corpus_entry_tags import AUTHOR_PERSON_CURATION_TAGS, \
    CORPUS_ENTRY_TAGS
from agr_literature_service.api.crud.workflow_tag_crud import _build_process_info, \
    _resolve_process_hierarchy, get_atp_id_by_name
from agr_literature_service.api.crud.workflow_transition_actions.first_pass_curation import \
    FIRST_PASS_CURATION_MOD, FIRST_PASS_CURATION_TBD_FALLBACK, FIRST_PASS_CURATION_TBD_NAME, \
    REQUIRED_COMPLETE_NAME_TO_FALLBACK
from agr_literature_service.api.models import ModModel, WorkflowTransitionModel

logger = logging.getLogger(__name__)

MANUAL_INDEXING_PROCESS = "ATP:0000273"

ENTRY_SOURCE_LABELS = {
    "corpus_entry": "Paper enters corpus",
    "sort_page": "Sort page",
    "pubmed_update": "PubMed update",
}


def _entry_points(mod: str) -> List[Tuple[str, str, Optional[str]]]:
    """(source, ATP the event sets, condition) for each event that starts one of
    the MOD's workflows (mod_corpus_association_crud.create / patch, the PubMed
    update)."""
    points: List[Tuple[str, str, Optional[str]]] = []
    if mod != "AGR":
        points.append(("corpus_entry", file_needed_tag_atp_id, None))
    for _name, curie, _states in CORPUS_ENTRY_TAGS.get(mod, []):
        points.append(("corpus_entry", curie, None))
    if mod == "SGD":
        points.append(("sort_page", MANUAL_INDEXING_PROCESS, "indexing status picked when sorting"))
    for _name, curie, _states in AUTHOR_PERSON_CURATION_TAGS.get(mod, []):
        points.append(("sort_page", curie, '"Author-Person curation" checked'))
    if mod in AUTHOR_REVIEW_MODS:
        points.append(("pubmed_update", AUTHOR_REVIEW_NEEDED, "curator-edited author list differs from PubMed"))
    return points


def _condition_label(checktype: str, value: str) -> Optional[str]:
    """A proceed_on_value check as the diagram shows it."""
    if checktype == "all":
        return None
    if checktype == "category":
        return value.replace("_", " ") + "s"
    if checktype == "reference_type":
        return f"{value} papers"
    return f"{checktype} {value}"


def _proceed_on_value_handoffs(transitions) -> Dict[Tuple[str, str], List[Optional[str]]]:
    """(state the transition moves into, ATP it starts) -> conditions, from the
    ``proceed_on_value::<checktype>::[<value>::]<target>`` actions. A target given
    by name (e.g. "entity extraction") is that workflow's process."""
    handoffs: Dict[Tuple[str, str], List[Optional[str]]] = {}
    for transition in transitions:
        for action in transition.actions or []:
            parts = action.split("::")
            if parts[0] != "proceed_on_value" or len(parts) < 3:
                continue
            checktype, value = parts[1], parts[2]
            target = parts[3] if len(parts) > 3 else value
            if not target.startswith("ATP:"):
                try:
                    target = get_atp_id_by_name(target)
                except ValueError:
                    logger.warning(f"workflow diagram: unknown workflow {target!r} in action {action!r}")
                    continue
            conditions = handoffs.setdefault((transition.transition_to, target), [])
            condition = _condition_label(checktype, value)
            if condition not in conditions:
                conditions.append(condition)
    return handoffs


def _first_pass_curation_handoffs() -> Dict[Tuple[str, str], List[Optional[str]]]:
    """FB: each prerequisite's "complete" state starts first pass curation once
    all three are complete (``first_pass_curation.set_first_pass_curation_tbd``)."""
    tbd = get_atp_id_by_name(FIRST_PASS_CURATION_TBD_NAME, fallback=FIRST_PASS_CURATION_TBD_FALLBACK)
    return {
        (get_atp_id_by_name(name, fallback=fallback), tbd): ["once all three prerequisites are complete"]
        for name, fallback in REQUIRED_COMPLETE_NAME_TO_FALLBACK.items()
    }


def _join_conditions(conditions: List[Optional[str]]) -> Optional[str]:
    # an unconditional handoff ("all") makes the others moot
    if None in conditions:
        return None
    return " or ".join(c for c in conditions if c is not None)


def get_workflow_diagram_triggers(mod: str, db: Session) -> Dict[str, List[Dict[str, Any]]]:
    mod_obj = db.query(ModModel).filter(ModModel.abbreviation == mod).first()
    if not mod_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod abbreviation '{mod}' does not exist")
    transitions = db.query(WorkflowTransitionModel).filter(
        WorkflowTransitionModel.mod_id == mod_obj.mod_id).all()

    handoffs = _proceed_on_value_handoffs(transitions)
    if mod == FIRST_PASS_CURATION_MOD:
        for key, conditions in _first_pass_curation_handoffs().items():
            handoffs.setdefault(key, []).extend(conditions)
    entry_points = _entry_points(mod)

    curies = {curie for pair in handoffs for curie in pair} | {curie for _s, curie, _c in entry_points}
    curie_to_name = get_map_ateam_curies_to_names(category="atpterm", curies=list(curies))
    process_cache, subprocess_cache = _resolve_process_hierarchy(curies, curie_to_name)

    def describe(curie: str, prefix: str) -> Dict[str, Any]:
        info = _build_process_info(curie, process_cache, subprocess_cache, curie_to_name)
        described = {
            f"{prefix}_state": curie,
            f"{prefix}_state_name": curie_to_name.get(curie, curie),
            f"{prefix}_process": info["workflow_process"],
            f"{prefix}_process_name": info["workflow_process_name"],
        }
        if "workflow_subprocess" in info:
            described[f"{prefix}_subprocess"] = info["workflow_subprocess"]
            described[f"{prefix}_subprocess_name"] = info["workflow_subprocess_name"]
        return described

    return {
        "entry_points": [
            {"source": source, "source_label": ENTRY_SOURCE_LABELS[source],
             **describe(curie, "to"), "condition": condition}
            for source, curie, condition in entry_points
        ],
        "handoffs": [
            {**describe(from_state, "from"), **describe(to_state, "to"),
             "condition": _join_conditions(conditions)}
            for (from_state, to_state), conditions in handoffs.items()
        ],
    }
