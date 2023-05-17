from typing import Dict

from fastapi import HTTPException
from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api.models import TopicEntityTagSourceModel, ReferenceModel, ModModel, TopicEntityTagModel


allowed_entity_type_map = {'ATP:0000005': 'gene', 'ATP:0000006': 'allele'}


def get_reference_id_from_curie_or_id(db: Session, curie_or_reference_id):
    reference_id = int(curie_or_reference_id) if curie_or_reference_id.isdigit() else None
    if reference_id is None:
        reference_id = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == curie_or_reference_id).one_or_none()
    if reference_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the reference_id or curie {curie_or_reference_id} is not available")
    return reference_id


def get_source_from_db(db: Session, topic_entity_tag_source_id: int) -> TopicEntityTagSourceModel:
    source: TopicEntityTagSourceModel = db.query(TopicEntityTagSourceModel).filter(
        TopicEntityTagSourceModel.topic_entity_tag_source_id == topic_entity_tag_source_id).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified source")
    return source


def add_source_obj_to_db_session(db: Session, topic_entity_tag_id: int, source: Dict):
    mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == source['mod_abbreviation']).scalar()
    if mod_id is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified MOD")
    source_obj = TopicEntityTagSourceModel(
        topic_entity_tag_id=topic_entity_tag_id,
        source=source["source"],
        confidence_level=source["confidence_level"],
        validated=source["validated"],
        validation_type=source["validation_type"],
        note=source["note"],
        mod_id=mod_id
    )
    db.add(source_obj)


def get_sorted_column_values(db: Session, column_name: str, desc: bool = False):
    curies = db.query(getattr(TopicEntityTagModel, column_name)).distinct()
    if column_name == "entity_type":
        return [curie for name, curie in sorted([(allowed_entity_type_map[curie[0]], curie[0]) for curie in curies],
                                                key=lambda x: x[0], reverse=desc)]
