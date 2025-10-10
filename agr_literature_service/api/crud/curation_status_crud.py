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

from agr_literature_service.api.crud.ateam_db_helpers import map_curies_to_names, search_topic
from agr_literature_service.api.crud.topic_entity_tag_utils import get_reference_id_from_curie_or_id
from agr_literature_service.api.models import CurationStatusModel, ReferenceModel, ModModel, TopicEntityTagModel, \
    TopicEntityTagSourceModel
from agr_literature_service.api.schemas import CurationStatusSchemaPost
from agr_literature_service.api.schemas.curation_status_schemas import AggregatedCurationStatusAndTETInfoSchema


def create(db: Session, curation_status: CurationStatusSchemaPost) -> int:
    """

    :param db:
    :param curation_status:
    :return:
    """

    curation_status_data = jsonable_encoder(curation_status)
    reference_curie = curation_status_data.pop("reference_curie", None)
    if reference_curie is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="reference_curie not within curation_status_data")
    try:
        # get ref_id from curie
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
    return db_obj.curation_status_id


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


def patch(db: Session, curation_status_id: int, curation_status_update) -> dict:
    """

    :param db:
    :param curation_status_id:
    :param curation_status_update:
    :return:
    """

    curation_status_data = curation_status_update.model_dump(exclude_unset=True)
    curation_status_db_obj = db.query(CurationStatusModel).filter(CurationStatusModel.curation_status_id == curation_status_id).first()
    if not curation_status_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CurationStatus with curation_status_id {curation_status_id} not found")

    for field, value in curation_status_data.items():
        setattr(curation_status_db_obj, field, value)

    curation_status_db_obj.dateUpdated = datetime.utcnow()
    db.add(curation_status_db_obj)
    db.commit()

    return {"message": "updated"}


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


def get_tet_list_summary(topic_curie, topic_tet_list_dict):
    if topic_curie not in topic_tet_list_dict or len(topic_tet_list_dict[topic_curie]) == 0:
        return {
            "tet_info_date_created": None,
            "tet_info_topic_source": [],
            "tet_info_has_data": False,
            "tet_info_new_data": False,
            "tet_info_no_data": False
        }
    # initialize earliest_dt from the very first row
    first_tet, _ = topic_tet_list_dict[topic_curie][0]
    if isinstance(first_tet.date_created, datetime):
        earliest_dt = first_tet.date_created
    else:
        date_str = str(first_tet.date_created).split()[0]
        earliest_dt = datetime.strptime(date_str, "%Y-%m-%d")
    has_data = new_data = no_data = False
    topic_sources = set()
    source_map = {
        'ATP:0000035': 'author',
        'ATP:0000036': 'biocurator'
    }
    for tet, tet_source in topic_tet_list_dict[topic_curie]:
        topic_sources.add(
            source_map.get(tet_source.source_evidence_assertion, 'computational')
        )
        if isinstance(tet.date_created, datetime):
            dt = tet.date_created
        else:
            date_str = str(tet.date_created).split()[0]  # "2025-03-05"
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        if dt < earliest_dt:
            earliest_dt = dt
        if tet.negated:
            no_data = True
        else:
            has_data = True
            if tet.data_novelty in {'ATP:0000321', 'ATP:0000229', 'ATP:0000228'}:
                new_data = True
    topic_added = earliest_dt
    return {
        "tet_info_date_created": topic_added,
        "tet_info_topic_source": sorted(topic_sources),
        "tet_info_has_data": has_data,
        "tet_info_new_data": new_data,
        "tet_info_no_data": no_data
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
                                                        search_topic(topic=None, mod_abbr=mod_abbreviation)}

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

    query = (f"SELECT cs.curation_status_id, cs.topic, cs.curation_status, cs.curation_tag, cs.note, cs.updated_by, "
             f"cs.date_updated, u.email AS updated_by_email "
             f"FROM curation_status cs JOIN users u ON cs.updated_by = u.id WHERE cs.mod_id = {mod_id} AND "
             f"cs.reference_id = {reference_id}")
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
            "curst_date_updated": row["date_updated"]
        })
    topic_to_name = map_curies_to_names('atpterm', agg_cur_stat_tet_objs.keys())

    for topic_curie in agg_cur_stat_tet_objs.keys():
        topic_name = topic_to_name.get(topic_curie, topic_curie)
        agg_cur_stat_tet_objs[topic_curie]["topic_name"] = topic_name
        agg_cur_stat_tet_objs[topic_curie]["topic_curie"] = topic_curie
        agg_cur_stat_tet_objs[topic_curie].update(get_tet_list_summary(topic_curie, topic_tet_list_dict))
    return [AggregatedCurationStatusAndTETInfoSchema(**value) for value in agg_cur_stat_tet_objs.values()]
