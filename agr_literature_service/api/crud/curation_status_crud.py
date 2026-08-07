"""
curation_status_crud.py
=============
"""
from collections import defaultdict
from datetime import datetime
from typing import Dict

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.ateam_db_helpers import map_curies_to_names, search_topic_list
from agr_literature_service.api.crud.reference_utils import normalize_reference_curie
from agr_literature_service.api.crud.topic_entity_tag_utils import get_reference_id_from_curie_or_id
from agr_literature_service.api.models import CurationStatusModel, ReferenceModel, ModModel, TopicEntityTagModel, \
    TopicEntityTagSourceModel
from agr_literature_service.api.schemas import CurationStatusSchemaPost
from agr_literature_service.api.schemas.curation_status_schemas import AggregatedCurationStatusAndTETInfoSchema
from agr_literature_service.api.crud.user_utils import map_to_user_id


def create(db: Session, curation_status: CurationStatusSchemaPost) -> CurationStatusModel:
    """

    :param db:
    :param curation_status:
    :return:
    """
    curation_status_data = jsonable_encoder(curation_status)
    if "created_by" in curation_status_data and curation_status_data["created_by"] is not None:
        curation_status_data["created_by"] = map_to_user_id(curation_status_data["created_by"], db)
    if "updated_by" in curation_status_data and curation_status_data["updated_by"] is not None:
        curation_status_data["updated_by"] = map_to_user_id(curation_status_data["updated_by"], db)
    reference_curie = curation_status_data.pop("reference_curie", None)
    if reference_curie is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="reference_curie not within curation_status_data")
    try:
        # get ref_id from curie
        reference_curie = normalize_reference_curie(db, reference_curie)
        ref_id = db.query(ReferenceModel).filter_by(curie=reference_curie).one().reference_id
        curation_status_data["reference_id"] = ref_id
        # look up mod
        abbreviation = curation_status_data.pop("mod_abbreviation", None)
        mod_id = db.query(ModModel).filter_by(abbreviation=abbreviation).one().mod_id
        curation_status_data["mod_id"] = mod_id
        curation_status_data["date_created"] = datetime.now().isoformat()
        db_obj = CurationStatusModel(**curation_status_data)
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
    except Exception as err:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Error creating curation_status: {err}")
    return db_obj


def destroy(db: Session, curation_status_id: int) -> None:
    """

    :param db:
    :param curation_status_id:
    :return:
    """

    curation_status = db.query(CurationStatusModel).filter(CurationStatusModel.curation_status_id == curation_status_id).first()
    if not curation_status:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CurationStatus with curation_status_id {curation_status_id} not found")
    db.delete(curation_status)
    db.commit()

    return None


def patch(db: Session, curation_status_id: int, curation_status_update) -> CurationStatusModel:
    """

    :param db:
    :param curation_status_id:
    :param curation_status_update:
    :return:
    """

    curation_status_data = curation_status_update.model_dump(exclude_unset=True)
    if "created_by" in curation_status_data and curation_status_data["created_by"] is not None:
        curation_status_data["created_by"] = map_to_user_id(curation_status_data["created_by"], db)
    if "updated_by" in curation_status_data and curation_status_data["updated_by"] is not None:
        curation_status_data["updated_by"] = map_to_user_id(curation_status_data["updated_by"], db)
    curation_status_db_obj = db.query(CurationStatusModel).filter(CurationStatusModel.curation_status_id == curation_status_id).first()
    if not curation_status_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CurationStatus with curation_status_id {curation_status_id} not found")

    for field, value in curation_status_data.items():
        setattr(curation_status_db_obj, field, value)

    curation_status_db_obj.dateUpdated = datetime.utcnow()
    db.add(curation_status_db_obj)
    db.commit()
    db.refresh(curation_status_db_obj)

    return curation_status_db_obj


def show(db: Session, curation_status_id: int) -> dict:
    """

    :param db:
    :param curation_status_id:
    :return:
    """

    curation_status = db.query(CurationStatusModel).filter(CurationStatusModel.curation_status_id == curation_status_id).one()
    curation_status_data = jsonable_encoder(curation_status)

    if not curation_status:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CurationStatus with the curation_status_id {curation_status_id} is not available")

    return curation_status_data


# Source evidence assertions that identify a manually created tag (author or
# professional biocurator). Everything else is treated as a computed prediction.
MANUAL_ASSERTIONS = {'ATP:0000035', 'ATP:0000036'}  # author, professional biocurator
# Data-novelty terms that mark a positive tag as "new data".
NEW_NOVELTY_TERMS = {'ATP:0000321', 'ATP:0000229', 'ATP:0000228'}

# Exact data-novelty term -> quick-add assessment column (no ontology-tree
# expansion; the column is filled only on an exact match).
NOVELTY_TO_COLUMN = {
    'ATP:0000321': 'new_data',
    'ATP:0000228': 'new_to_db',
    'ATP:0000229': 'new_to_field',
}
# The quick-add assessment columns, in display order.
ASSESSMENT_COLUMNS = ['has_data', 'new_data', 'new_to_db', 'new_to_field', 'no_data']
# The positive (has-data) columns; "no_data" is their negated counterpart.
POSITIVE_ASSESSMENT_COLUMNS = ['has_data', 'new_data', 'new_to_db', 'new_to_field']
# validation_by_professional_biocurator values that count as curator-validated
# (green ✓); 'validated_wrong' is excluded entirely; everything else is "?".
BIOCURATOR_VALIDATED = {'validated_right', 'validated_right_self'}
BIOCURATOR_VALIDATED_WRONG = 'validated_wrong'


def _assessment_kind(tet):
    """Which assessment column a tag falls under: 'no', 'new', or 'has'."""
    if tet.negated:
        return 'no'
    if tet.data_novelty in NEW_NOVELTY_TERMS:
        return 'new'
    return 'has'


def _assessment_columns(tet):
    """Quick-add columns a tag fills: 'no_data' when negated; otherwise
    'has_data' plus its exact-novelty column (new_data / new_to_db /
    new_to_field) when the data_novelty matches one."""
    if tet.negated:
        return ['no_data']
    cols = ['has_data']
    col = NOVELTY_TO_COLUMN.get(tet.data_novelty)
    if col:
        cols.append(col)
    return cols


def get_tet_list_summary(topic_curie, topic_tet_list_dict):
    if topic_curie not in topic_tet_list_dict or len(topic_tet_list_dict[topic_curie]) == 0:
        return {
            "tet_info_date_created": None,
            "tet_info_topic_source": [],
            "tet_info_has_data": False,
            "tet_info_new_data": False,
            "tet_info_no_data": False,
            "tet_info_manual_has_data": False,
            "tet_info_manual_new_data": False,
            "tet_info_manual_no_data": False,
            "tet_info_source_predictions": [],
            "tet_info_manual_assessments": [],
            "tet_info_assessment_states": {c: None for c in ASSESSMENT_COLUMNS}
        }
    # initialize earliest_dt from the very first row
    first_tet, _ = topic_tet_list_dict[topic_curie][0]
    if isinstance(first_tet.date_created, datetime):
        earliest_dt = first_tet.date_created
    else:
        date_str = str(first_tet.date_created).split()[0]
        earliest_dt = datetime.strptime(date_str, "%Y-%m-%d")
    has_data = new_data = no_data = False
    manual_has = manual_new = manual_no = False
    topic_sources = set()
    source_predictions = []
    manual_assessments = []
    # Per-column validation state for the quick-add grid.
    col_validated = set()
    col_unvalidated = set()
    source_map = {
        'ATP:0000035': 'author',
        'ATP:0000036': 'biocurator'
    }
    for tet, tet_source in topic_tet_list_dict[topic_curie]:
        assertion = tet_source.source_evidence_assertion
        is_manual = assertion in MANUAL_ASSERTIONS
        topic_sources.add(source_map.get(assertion, 'computational'))
        if isinstance(tet.date_created, datetime):
            dt = tet.date_created
        else:
            date_str = str(tet.date_created).split()[0]  # "2025-03-05"
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        if dt < earliest_dt:
            earliest_dt = dt
        kind = _assessment_kind(tet)
        if kind == 'no':
            no_data = True
            if is_manual:
                manual_no = True
        else:
            has_data = True
            if is_manual:
                manual_has = True
            if kind == 'new':
                new_data = True
                if is_manual:
                    manual_new = True
        # Expose each computed (non-manual) tag so the UI can show which source
        # predicted what, with its confidence, for curator validation. Manual
        # tags go to manual_assessments so the UI can tell an author-recorded
        # (unvalidated) bucket from a biocurator-validated one.
        if not is_manual:
            source_predictions.append({
                "source_method": tet_source.source_method,
                "source_evidence_assertion": assertion,
                "confidence_score": tet.confidence_score,
                "confidence_level": tet.confidence_level,
                "negated": bool(tet.negated),
                "assessment": kind,
                "data_novelty": tet.data_novelty,
                "entity": tet.entity,
                "entity_type": tet.entity_type
            })
        else:
            manual_assessments.append({
                "source": source_map.get(assertion),
                "negated": bool(tet.negated),
                "assessment": kind,
                "data_novelty": tet.data_novelty
            })
        # Quick-add column state: skip tags a biocurator marked wrong; a
        # biocurator-validated tag makes its columns "validated", otherwise
        # they are "unvalidated" (shown only if the row has no validated tag).
        vpb = tet.validation_by_professional_biocurator
        if vpb != BIOCURATOR_VALIDATED_WRONG:
            target = col_validated if vpb in BIOCURATOR_VALIDATED else col_unvalidated
            for col in _assessment_columns(tet):
                target.add(col)
    # Per-column state with a polarity guard: a biocurator-validated tag only
    # suppresses the OPPOSITE-polarity "?" (a validated positive hides a "no
    # data" prediction and vice versa), so an unvalidated prediction on a column
    # the biocurator hasn't resolved still shows "?" for the curator to act on.
    has_validated_positive = bool(col_validated & set(POSITIVE_ASSESSMENT_COLUMNS))
    has_validated_no = 'no_data' in col_validated
    assessment_states = {}
    for col in ASSESSMENT_COLUMNS:
        if col in col_validated:
            assessment_states[col] = 'validated'
        elif col in col_unvalidated:
            suppressed = (
                (col == 'no_data' and has_validated_positive)
                or (col in POSITIVE_ASSESSMENT_COLUMNS and has_validated_no)
            )
            assessment_states[col] = None if suppressed else 'unvalidated'
        else:
            assessment_states[col] = None
    topic_added = earliest_dt.isoformat()
    return {
        "tet_info_date_created": topic_added,
        "tet_info_topic_source": sorted(topic_sources),
        "tet_info_has_data": has_data,
        "tet_info_new_data": new_data,
        "tet_info_no_data": no_data,
        "tet_info_manual_has_data": manual_has,
        "tet_info_manual_new_data": manual_new,
        "tet_info_manual_no_data": manual_no,
        "tet_info_source_predictions": source_predictions,
        "tet_info_manual_assessments": manual_assessments,
        "tet_info_assessment_states": assessment_states
    }


def get_aggregated_curation_status_and_tet_info(db: Session, reference_curie, mod_abbreviation):

    reference_id = get_reference_id_from_curie_or_id(db=db, curie_or_reference_id=reference_curie)
    if reference_id is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"The reference curie {reference_curie} is not in the database.")
    mod_id = db.query(ModModel).filter_by(abbreviation=mod_abbreviation).one().mod_id
    if mod_id is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"The mod abbreviation {mod_abbreviation} is not in the database.")

    # create empty return objects with topics from atp subsets as keys
    agg_cur_stat_tet_objs: Dict[str, Dict[str, str]] = {topic["curie"]: {} for topic in
                                                        search_topic_list(topic=None, mod_abbr=mod_abbreviation)}

    # add tet info to the objects
    query = (
        db.query(TopicEntityTagModel, TopicEntityTagSourceModel)
        .join(
            TopicEntityTagSourceModel,
            TopicEntityTagModel.topic_entity_tag_source_id == TopicEntityTagSourceModel.topic_entity_tag_source_id
        )
        .filter(
            TopicEntityTagModel.reference_id == reference_id,
            TopicEntityTagSourceModel.data_provider == mod_abbreviation
        )
    )
    rows = query.all()

    topic_tet_list_dict = defaultdict(list)
    for tet, tet_source in rows:
        topic_tet_list_dict[tet.topic].append((tet, tet_source))

    query = (
        f"SELECT cs.curation_status_id, cs.topic, cs.curation_status, cs.curation_tag, cs.note, "
        f"cs.updated_by, cs.date_updated, "
        f"get_most_current_email(u.person_id) AS updated_by_email, "
        f"p.display_name AS updated_by_name "
        f"FROM curation_status cs "
        f"JOIN users u ON cs.updated_by = u.id "
        f"LEFT JOIN person p ON u.person_id = p.person_id "
        f"WHERE cs.mod_id = {mod_id} AND cs.reference_id = {reference_id}"
    )

    res = db.execute(text(query)).mappings().fetchall()
    for row in res:
        if row["topic"] not in agg_cur_stat_tet_objs:
            agg_cur_stat_tet_objs[row["topic"]] = {}
        agg_cur_stat_tet_objs[row["topic"]].update({
            "curst_curation_status_id": row["curation_status_id"],
            "curst_curation_status": row["curation_status"],
            "curst_curation_tag": row["curation_tag"],
            "curst_note": row["note"],
            "curst_updated_by": row["updated_by"],
            "curst_updated_by_email": row["updated_by_email"],
            "curst_updated_by_name": row["updated_by_name"],
            "curst_date_updated": row["date_updated"].isoformat()
        })
    topic_to_name = map_curies_to_names('atpterm', agg_cur_stat_tet_objs.keys())

    for topic_curie in agg_cur_stat_tet_objs.keys():
        topic_name = topic_to_name.get(topic_curie, topic_curie)
        agg_cur_stat_tet_objs[topic_curie]["topic_name"] = topic_name
        agg_cur_stat_tet_objs[topic_curie]["topic_curie"] = topic_curie
        agg_cur_stat_tet_objs[topic_curie].update(get_tet_list_summary(topic_curie, topic_tet_list_dict))
    return [AggregatedCurationStatusAndTETInfoSchema(**value) for value in agg_cur_stat_tet_objs.values()]
