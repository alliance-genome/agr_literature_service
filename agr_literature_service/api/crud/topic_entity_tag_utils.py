import json
import urllib.request
from os import environ
from typing import Dict

from fastapi import HTTPException
from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api.models import TopicEntityTagSourceModel, ReferenceModel, ModModel, TopicEntityTagModel
from agr_literature_service.api.user import add_user_if_not_exists

allowed_entity_type_map = {'ATP:0000005': 'gene', 'ATP:0000006': 'allele'}

# TODO: fix these to get from database or some other place?
sgd_primary_display_tag = 'ATP:0000147'
sgd_additional_display_tag = 'ATP:0000132'
sgd_omics_display_tag = 'ATP:0000148'
sgd_review_display_tag = 'ATP:0000130'

sgd_primary_topics = ['ATP:0000128', 'ATP:0000012', 'ATP:0000079', 'ATP:0000129',
                      'other primary info']
sgd_review_topics = ['review']
sgd_omics_topics = ['ATP:0000085', 'ATP:0000150']
sgd_additional_topics = ['ATP:0000142', 'ATP:0000011', 'ATP:0000088', 'ATP:0000070',
                         'ATP:0000022', 'ATP:0000149', 'ATP:0000054', 'ATP:0000006',
                         'other additional literature']


def get_reference_id_from_curie_or_id(db: Session, curie_or_reference_id):
    reference_id = int(curie_or_reference_id) if curie_or_reference_id.isdigit() else None
    if reference_id is None:
        reference = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == curie_or_reference_id).one_or_none()
        if reference is not None:
            reference_id = reference.reference_id
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Reference with the reference_id or curie {curie_or_reference_id} "
                                       f"is not available")
    return reference_id


def get_source_from_db(db: Session, topic_entity_tag_source_id: int) -> TopicEntityTagSourceModel:
    source: TopicEntityTagSourceModel = db.query(TopicEntityTagSourceModel).filter(
        TopicEntityTagSourceModel.topic_entity_tag_source_id == topic_entity_tag_source_id).one_or_none()
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified source")
    return source


def add_audited_object_users_if_not_exist(db: Session, audited_obj: Dict):
    if "created_by" in audited_obj:
        add_user_if_not_exists(db, audited_obj["created_by"])
    if "updated_by" in audited_obj:
        add_user_if_not_exists(db, audited_obj["created_by"])


def add_source_obj_to_db_session(db: Session, source: Dict):
    mod = db.query(ModModel.mod_id).filter(ModModel.abbreviation == source['mod_abbreviation']).one_or_none()
    if mod is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified MOD")
    add_audited_object_users_if_not_exist(db, source)
    del source["mod_abbreviation"]
    source["mod_id"] = mod.mod_id
    source_obj = TopicEntityTagSourceModel(**source)
    db.add(source_obj)
    return source_obj


def get_sorted_column_values(db: Session, column_name: str, desc: bool = False):
    curies = db.query(getattr(TopicEntityTagModel, column_name)).distinct()
    if column_name == "entity_type":
        return [curie for name, curie in sorted([(allowed_entity_type_map[curie[0]], curie[0]) for curie in curies
                                                 if curie[0]], key=lambda x: x[0], reverse=desc)]


def get_map_ateam_curies_to_names(curies_category, curies, token):
    ateam_api_base_url = environ.get('ATEAM_API_URL', "https://beta-curation.alliancegenome.org/api")
    if curies_category == "species":
        curies_category = "ncbitaxonterm"
    ateam_api = f'{ateam_api_base_url}/{curies_category}/search?limit=1000&page=0'
    request_body = {
        "searchFilters": {
            "nameFilters": {
                "curie_keyword": {"queryString": " ".join(curies), "tokenOperator": "OR"}
            }
        }
    }
    request_data_encoded = json.dumps(request_body)
    request_data_encoded_str = str(request_data_encoded)
    request = urllib.request.Request(url=ateam_api, data=request_data_encoded_str.encode('utf-8'))
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("Content-type", "application/json")
    request.add_header("Accept", "application/json")
    with urllib.request.urlopen(request) as response:
        resp = response.read().decode("utf8")
        resp_obj = json.loads(resp)
        # from the A-team API, atp values have a "name" field and other entities (e.g., genes and alleles) have
        # symbol objects - e.g., geneSymbol.displayText
        return {entity["curie"]: entity["name"] if "name" in entity else entity[
            curies_category + "Symbol"]["displayText"] for entity in (resp_obj["results"] if "results" in
                                                                                             resp_obj else [])}


def check_and_set_sgd_display_tag(topic_entity_tag_data):

    topic = topic_entity_tag_data['topic']
    entity = topic_entity_tag_data['entity']
    entity_type = topic_entity_tag_data['entity_type']
    display_tag = topic_entity_tag_data['display_tag']
    if entity_type and not entity:
        topic_entity_tag_data['entity_type'] = None
    if topic in sgd_primary_topics and display_tag != sgd_primary_display_tag:
        topic_entity_tag_data['display_tag'] = sgd_primary_display_tag
    elif topic in sgd_review_topics and display_tag != sgd_review_display_tag:
        topic_entity_tag_data['display_tag'] = sgd_review_display_tag
    elif topic in sgd_omics_topics:
        if display_tag != sgd_omics_display_tag:
            topic_entity_tag_data['display_tag'] = sgd_omics_display_tag
        if entity_type:
            topic_entity_tag_data['entity_type'] = None
        if entity:
            topic_entity_tag_data['entity'] = None
    elif topic in sgd_additional_topics:
        if entity:
            if display_tag != sgd_additional_display_tag:
                topic_entity_tag_data['display_tag'] = sgd_additional_display_tag
        else:
            if display_tag:
                topic_entity_tag_data['display_tag'] = None
