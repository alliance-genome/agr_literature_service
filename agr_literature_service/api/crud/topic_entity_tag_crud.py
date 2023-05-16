"""
topic_entity_tag_crud.py
===========================
"""
import json
import urllib.request
from collections import defaultdict

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, case
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload

from agr_literature_service.api.models import (
    TopicEntityTagModel,
    ReferenceModel, TopicEntityTagQualifierModel, ModModel, TopicEntityTagSourceModel
)
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaPost, \
    TopicEntityTagSourceSchemaPost, TopicEntityTagSourceSchemaUpdate

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


def create_tag_with_source(db: Session, topic_entity_tag: TopicEntityTagSchemaPost) -> int:
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    reference_curie = topic_entity_tag_data.pop("reference_curie", None)
    if reference_curie is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="reference_curie not within topic_entity_tag_data")
    reference_id = get_reference_id_from_curie_or_id(db, reference_curie)
    topic_entity_tag_data["reference_id"] = reference_id
    qualifiers = topic_entity_tag_data.pop("qualifiers", []) or []
    sources = topic_entity_tag_data.pop("sources", []) or []
    db_obj = TopicEntityTagModel(**topic_entity_tag_data)
    try:
        db.add(db_obj)
        for qualifier in qualifiers:
            mod_id = db.query(ModModel).filter(ModModel.abbreviation == qualifier['mod_abbreviation']).scalar()
            if mod_id is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified MOD")
            qualifier_obj = TopicEntityTagQualifierModel(
                topic_entity_tag_id=db_obj.topic_entity_tag_id,
                qualifier=qualifier["qualifier"],
                qualifier_type=qualifier["qualifier_type"],
                mod_id=mod_id,
            )
            db.add(qualifier_obj)
        for source in sources:
            mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == source['mod_abbreviation']).scalar()
            if mod_id is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified MOD")
            source_obj = TopicEntityTagSourceModel(
                topic_entity_tag_id=db_obj.topic_entity_tag_id,
                source=source["source"],
                confidence_level=source["confidence_level"],
                validated=source["validated"],
                validation_type=source["validation_type"],
                note=source["note"],
                mod_id=mod_id
            )
            db.add(source_obj)
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"invalid request: {e}")
    return db_obj.topic_entity_tag_id


def show(db: Session, topic_entity_tag_id: int):
    topic_entity_tag = db.query(TopicEntityTagModel).get(topic_entity_tag_id)
    if not topic_entity_tag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"topic_entityTag with the topic_entity_tag_id {topic_entity_tag_id} "
                                   f"is not available")
    topic_entity_tag_data = jsonable_encoder(topic_entity_tag)
    if topic_entity_tag_data["reference_id"]:
        topic_entity_tag_data["reference_curie"] = db.query(ReferenceModel).filter(
            ReferenceModel.reference_id == topic_entity_tag_data["reference_id"]).first().curie
        del topic_entity_tag_data["reference_id"]

    qualifiers = db.query(TopicEntityTagQualifierModel).filter(
        TopicEntityTagQualifierModel.topic_entity_tag_id == topic_entity_tag_id).all()
    topic_entity_tag_data["qualifiers"] = [jsonable_encoder(qualifier) for qualifier in qualifiers]

    sources = db.query(TopicEntityTagSourceModel).options(joinedload(TopicEntityTagSourceModel.mod)).filter(
        TopicEntityTagSourceModel.topic_entity_tag_id == topic_entity_tag_id).all()
    topic_entity_tag_data["sources"] = [jsonable_encoder(source) for source in sources]
    for source in topic_entity_tag_data["sources"]:
        source["mod_abbreviation"] = source["mod"]["abbreviation"]
        del source["mod"]
        del source["mod_id"]
        del source["topic_entity_tag_id"]
    return topic_entity_tag_data


def add_source_to_tag(db: Session, topic_entity_tag_id: int, source: TopicEntityTagSourceSchemaPost):
    ...


def destroy_source(db: Session, topic_entity_tag_source_id: int):
    # remove tag if that's the last one
    ...


def patch_source(db: Session, topic_entity_tag_source_id: int, source: TopicEntityTagSourceSchemaUpdate):
    ...


def get_sorted_column_values(db: Session, column_name: str, desc: bool = False):
    curies = db.query(getattr(TopicEntityTagModel, column_name)).distinct()
    if column_name == "entity_type":
        return [curie for name, curie in sorted([(allowed_entity_type_map[curie[0]], curie[0]) for curie in curies],
                                                key=lambda x: x[0], reverse=desc)]


def show_all_reference_tags(db: Session, curie_or_reference_id, page: int = 1, page_size: int = None,
                            count_only: bool = False, sort_by: str = None, desc_sort: bool = False):
    if page < 1:
        page = 1
    if sort_by == "null":
        sort_by = None
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    query = db.query(TopicEntityTagModel).options(joinedload(TopicEntityTagModel.sources)).filter(
        TopicEntityTagModel.reference_id == reference_id)
    if count_only:
        return query.count()
    else:
        if sort_by:
            curie_ordering = case({curie: index for index, curie in enumerate(get_sorted_column_values(db, sort_by,
                                                                                                       desc_sort))},
                                  value=getattr(TopicEntityTagModel, sort_by))
            query = query.order_by(curie_ordering)
        return [jsonable_encoder(tet) for tet in query.offset((page - 1) * page_size if page_size else None).limit(
            page_size).all()]


def get_map_entity_curie_to_name(db: Session, curie_or_reference_id: str, token: str):
    reference_id = get_reference_id_from_curie_or_id(db, curie_or_reference_id)
    topics_and_entities = db.query(TopicEntityTagModel).filter(
        and_(TopicEntityTagModel.reference_id == reference_id,
             TopicEntityTagModel.entity_type.in_([key for key in allowed_entity_type_map.keys()]),
             TopicEntityTagModel.alliance_entity.isnot(None))).all()
    tags_by_entity_type = defaultdict(set)
    entity_curie_to_name = {}
    for tag in topics_and_entities:
        tags_by_entity_type[allowed_entity_type_map[tag.entity_type]].add(tag.alliance_entity)
    for entity_type, entity_curies in tags_by_entity_type.items():
        ateam_api = f'https://beta-curation.alliancegenome.org/api/{entity_type}/search?limit=1000&page=0'
        request_body = {"searchFilters": {
            "nameFilters": {
                "curie_keyword": {"queryString": " ".join(entity_curies), "tokenOperator": "OR"}
            }

        }}
        request_data_encoded = json.dumps(request_body)
        request_data_encoded_str = str(request_data_encoded)
        request = urllib.request.Request(url=ateam_api, data=request_data_encoded_str.encode('utf-8'))
        request.add_header("Authorization", f"Bearer {token}")
        request.add_header("Content-type", "application/json")
        request.add_header("Accept", "application/json")
        with urllib.request.urlopen(request) as response:
            resp = response.read().decode("utf8")
            resp_obj = json.loads(resp)
            entity_curie_to_name.update({entity["curie"]: entity[entity_type + "Symbol"]["displayText"]
                                         for entity in resp_obj["results"]})
    return entity_curie_to_name
