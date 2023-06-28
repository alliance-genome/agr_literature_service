import json
import urllib.request
from os import environ
from typing import Dict

from fastapi import HTTPException
from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api.models import TopicEntityTagSourceModel, ReferenceModel, ModModel, TopicEntityTagModel


allowed_entity_type_map = {'ATP:0000005': 'gene', 'ATP:0000006': 'allele'}


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


def add_source_obj_to_db_session(db: Session, topic_entity_tag_id: int, source: Dict):
    mod = db.query(ModModel.mod_id).filter(ModModel.abbreviation == source['mod_abbreviation']).one_or_none()
    if mod is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cannot find the specified MOD")
    source_obj = TopicEntityTagSourceModel(
        topic_entity_tag_id=topic_entity_tag_id,
        source=source["source"],
        negated=source["negated"],
        confidence_level=source["confidence_level"],
        validation_value_author=source["validation_value_author"],
        validation_value_curator=source["validation_value_curator"],
        validation_value_curation_tools=source["validation_value_curation_tools"],
        note=source["note"],
        mod_id=mod.mod_id
    )
    db.add(source_obj)


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
