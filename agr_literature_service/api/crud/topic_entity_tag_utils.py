import json
import logging
import urllib.request
from os import environ
from typing import Dict, List
from urllib.error import HTTPError

import requests
from cachetools import TTLCache
from cachetools.func import ttl_cache
from fastapi import HTTPException
from fastapi_okta.okta_utils import get_authentication_token
from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api.models import TopicEntityTagSourceModel, ReferenceModel, ModModel, TopicEntityTagModel
from agr_literature_service.api.crud.topic_entity_id_mapping_utils import map_curies_to_names
from agr_literature_service.api.user import add_user_if_not_exists

logger = logging.getLogger(__name__)

# TODO: fix these to get from database or some other place?
sgd_primary_display_tag = 'ATP:0000147'
sgd_additional_display_tag = 'ATP:0000132'
sgd_omics_display_tag = 'ATP:0000148'
sgd_review_display_tag = 'ATP:0000130'

sgd_primary_topics = ['ATP:0000128', 'ATP:0000012', 'ATP:0000079', 'ATP:0000129',
                      'ATP:0000147']
sgd_review_topics = ['ATP:0000130']
sgd_omics_topics = ['ATP:0000085', 'ATP:0000150']
sgd_additional_topics = ['ATP:0000142', 'ATP:0000011', 'ATP:0000088', 'ATP:0000070',
                         'ATP:0000022', 'ATP:0000149', 'ATP:0000054', 'ATP:0000006',
                         'ATP:0000132']

root_topic_atp = 'ATP:0000002'
species_atp = 'ATP:0000123'


class ExpiringCache:
    def __init__(self, expiration_time=3600):  # set default to 1hr
        # to store up to 50,000 items at any given time
        self.cache = TTLCache(maxsize=50_000, ttl=expiration_time)

    def set(self, key, value):
        # set a value in the cache; it will automatically expire after the TTL
        self.cache[key] = value

    def get(self, key):
        # get a value from the cache; returns None if the key is expired or not found
        return self.cache.get(key, None)


id_to_name_cache = ExpiringCache(expiration_time=7200)
valid_id_to_name_cache = ExpiringCache(expiration_time=7200)


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
    if "created_by" in audited_obj and audited_obj["created_by"]:
        add_user_if_not_exists(db, audited_obj["created_by"])
    if "updated_by" in audited_obj and audited_obj["updated_by"]:
        add_user_if_not_exists(db, audited_obj["updated_by"])


def add_source_obj_to_db_session(db: Session, source: Dict):
    secondary_data_provider = db.query(ModModel.mod_id).filter(
        ModModel.abbreviation == source['secondary_data_provider_abbreviation']).one_or_none()
    if secondary_data_provider is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Cannot find the specified secondary data provider")
    add_audited_object_users_if_not_exist(db, source)
    del source["secondary_data_provider_abbreviation"]
    source["secondary_data_provider_id"] = secondary_data_provider.mod_id
    source_obj = TopicEntityTagSourceModel(**source)
    db.add(source_obj)
    return source_obj


def get_sorted_column_values(reference_id: int, db: Session, column_name: str, desc: bool = False):

    if column_name == "entity":
        results = (
            db.query(
                TopicEntityTagModel.entity,
                TopicEntityTagModel.entity_type
            )
            .filter_by(reference_id=reference_id)
            .distinct()
            .all()
        )
        entity_type_to_entities: Dict[str, List[str]] = {}
        for result in results:
            if result.entity_type in entity_type_to_entities:
                entity_type_to_entities[result.entity_type].append(result.entity)
            else:
                entity_type_to_entities[result.entity_type] = [result.entity]
        curie_name_map = _get_map_abc_entity_curies_to_names(db, entity_type_to_entities)
    else:
        curies = db.query(getattr(TopicEntityTagModel, column_name)).filter(
            TopicEntityTagModel.reference_id == reference_id).distinct()
        category = "atpterm" if column_name != "species" else "species"
        curie_name_map = map_curies_to_names(db, category, [curie[0] for curie in curies if curie[0]])

    return [curie for name, curie in sorted([(value, key) for key, value in curie_name_map.items()],
                                            key=lambda x: x[0], reverse=desc)]


def _get_map_abc_entity_curies_to_names(db, entity_type_to_entities):

    entity_curie_to_name_map = {}
    for entity_type in entity_type_to_entities:
        entity_curies = entity_type_to_entities[entity_type]
        curie_to_name_map = map_curies_to_names(db, entity_type, entity_curies)
        entity_curie_to_name_map.update(curie_to_name_map)

    """
    {'SGD:S000001085': 'DOG2', 'SGD:S000001086': 'DOG1', 'SGD:S000001855': 'ACT1', 'SGD:S000002592': 'ATC1', 'WB:WBGene00003001': 'lin-12', 'ZFIN:ZDB-GENE-000607-29': 'id:ibd5038', 'ZFIN:ZDB-GENE-000816-1': 'fgfr3', 'ZFIN:ZDB-GENE-980526-255': 'fgfr1a', 'ZFIN:ZDB-GENE-980526-488': 'fgfr4', 'ZFIN:ZDB-GENE-990415-72': 'fgf8a', 'ZFIN:ZDB-GENE-991228-4': 'etv5b'}
    """
    return entity_curie_to_name_map


def _get_map_sgd_curies_to_names(curies_category, curies):  # pragma: no cover

    curie_list = "|".join(curies).replace(" ", "+")
    sgd_api_base_url = environ.get("SGD_API_URL")
    url = f"{sgd_api_base_url}{curies_category}/{curie_list}"
    id_to_name_mapping = {}
    try:
        response = requests.get(url)
        for res in response.json():
            id_to_name_mapping[res['modEntityId']] = res['display_name']
            id_to_name_cache.set(res['modEntityId'], res['display_name'])
    except requests.RequestException as e:
        logger.error(f"An error occurred when running 'get_map_complex_pathway_ids_to_names': {e}")
        return None
    return id_to_name_mapping


def _get_map_wb_curies_to_names(curies_category, curies):
    post_data = {
        "datatype": curies_category, "entities": "|".join(curies)
    }
    url = environ.get(
        "WB_API_URL", "https://caltech-curation.textpressolab.com/pub/cgi-bin/forms/abc_readonly_api.cgi")
    id_to_name_mapping = {}
    try:
        response = requests.post(url, json=post_data,
                                 headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
        for mod_entity_id, display_name in response.json().items():
            id_to_name_mapping[mod_entity_id] = display_name
            id_to_name_cache.set(mod_entity_id, display_name)
    except requests.RequestException as e:
        logger.error(f"An error occurred when running 'WB entity curie to name resolution': {e}")
        return None
    return id_to_name_mapping


def get_map_entity_curies_to_names(db, entity_id_validation, curies_category, curies):
    curie_to_name_mapping = {}
    if entity_id_validation == "alliance":
        curie_to_name_mapping.update(map_curies_to_names(db, curies_category, curies))
    elif entity_id_validation.lower() == "wb":
        curie_to_name_mapping.update(_get_map_wb_curies_to_names(curies_category=curies_category, curies=curies))
    elif entity_id_validation.lower() == "sgd":
        curies_category = curies_category.replace("protein containing ", "")
        curie_to_name_mapping.update(_get_map_sgd_curies_to_names(curies_category=curies_category, curies=curies))
    return curie_to_name_mapping


def fallback_id_to_name_mapping(curies_category, curie_list, id_name_mapping):

    sgd_curies = []
    for curie in curie_list:
        if curie in id_name_mapping:
            continue
        if curie.startswith('SGD:'):
            sgd_curies.append(curie)
        # add other mod IDs checking here

    if len(sgd_curies) > 0:
        id_name_mapping.update(_get_map_sgd_curies_to_names(curies_category=curies_category, curies=sgd_curies))
    ## add fallback calls for other mods here

    for curie in curie_list:
        if curie not in id_name_mapping:
            id_name_mapping[curie] = curie  # map curie to itself if no result
    return id_name_mapping


def check_atp_ids_validity(curies, maxret=1000):

    curies_not_in_cache = [curie for curie in set(curies) if valid_id_to_name_cache.get(curie) is None]
    if len(curies_not_in_cache) == 0:
        return (set(curies), {curie: valid_id_to_name_cache.get(curie) for curie in set(curies)})

    valid_curies = {curie for curie in curies if valid_id_to_name_cache.get(curie) is not None}
    atp_to_name = {}
    ateam_api_base_url = environ.get('ATEAM_API_URL')
    ateam_api = f'{ateam_api_base_url}/atpterm/search?limit={maxret}&page=0'
    chunked_values = [curies_not_in_cache[i:i + maxret] for i in range(0, len(curies_not_in_cache), maxret)]
    for chunk in chunked_values:
        request_body = {
            "searchFilters": {
                "nameFilters": {
                    "curie_keyword": {
                        "queryString": " ".join(chunk),
                        "tokenOperator": "OR"
                    }
                }
            }
        }
        token = get_authentication_token()
        try:
            request_data_encoded = json.dumps(request_body).encode('utf-8')
            request = urllib.request.Request(url=ateam_api, data=request_data_encoded)
            request.add_header("Authorization", f"Bearer {token}")
            request.add_header("Content-type", "application/json")
            request.add_header("Accept", "application/json")
            with urllib.request.urlopen(request) as response:
                resp = response.read().decode("utf8")
                resp_obj = json.loads(resp)
                for entry in resp_obj.get("results", []):
                    atp_to_name[entry["curie"]] = entry["name"]
                    if entry["obsolete"] is False:
                        valid_curies.add(entry["curie"])
                        valid_id_to_name_cache.set(entry["curie"], entry["name"])
        except HTTPError as e:
            logger.error(f"HTTPError: in search_ateam: {e}")
        except Exception as e:
            logger.error(f"Exception: in search_ateam: {e}")

    return (valid_curies, atp_to_name)


@ttl_cache(maxsize=128, ttl=60 * 60)
def _get_ancestors_or_descendants(onto_node: str, ancestors_or_descendants: str = 'ancestors') -> List[str]:
    """

    This method `get_ancestors_or_descendants` is used to fetch the ancestors or descendants of a given ontology node.

    Parameters:
    - onto_node (str): The ontology node for which ancestors or descendants need to be fetched.
    - ancestors_or_descendants (str, optional): The type of relation to fetch. Default is `'ancestors'`.
    Valid values are `'ancestors'` and `'descendants'`.

    Returns:
    - list[str]: A list of ontology nodes that are ancestors or descendants of the given ontology node.

    Note:
    - This method uses the `get_authentication_token` function from the `okta_utils` module to fetch the authentication
    token.
    - It also relies on the `ATEAM_API_URL` environment variable to determine the base URL for the A-Team API.
    - The method will make an HTTP request to the A-Team API using the provided ontology node and relation type.
    - If successful, it will parse the response and extract the ontology node CURIEs from the `entities` field of the
    response JSON.
    - The extracted CURIEs will be returned as a list.
    - In case of any error, an empty list will be returned.

    Example Usage:
    ```python
    ontology_node = 'GO:0008150'
    relation_type = 'ancestors'
    result = get_ancestors_or_descendants(ontology_node, relation_type)
    print(result)
    # Output: ['GO:0022607', 'GO:0050896', 'GO:0008152', 'GO:0005575', 'GO:0050891', 'GO:0003674']

    ontology_node = 'DOID:0060047'
    relation_type = 'descendants'
    result = get_ancestors_or_descendants(ontology_node, relation_type)
    print(result)
    # Output: ['DOID:0060400', 'DOID:0060399']
    ```

    """
    if ancestors_or_descendants not in ['ancestors', 'descendants']:
        return []
    token = get_authentication_token()
    ateam_api_base_url = environ.get('ATEAM_API_URL', "https://beta-curation.alliancegenome.org/api")
    ateam_api = f'{ateam_api_base_url}/atpterm/{onto_node}/{ancestors_or_descendants}'
    try:
        request = urllib.request.Request(url=ateam_api)
        request.add_header("Authorization", f"Bearer {token}")
        request.add_header("Content-type", "application/json")
        request.add_header("Accept", "application/json")
    except Exception as e:
        logger.error(f"Exception setting up request:get_ancestors_or_descendants: {e}")
        return []
    try:
        with urllib.request.urlopen(request) as response:
            resp = response.read().decode("utf8")
            resp_obj = json.loads(resp)
            return [entity["curie"] for entity in resp_obj["entities"]] if "entities" in resp_obj else []
    except HTTPError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Error from A-team API")


def get_ancestors(onto_node: str):
    return _get_ancestors_or_descendants(onto_node=onto_node, ancestors_or_descendants="ancestors")


def get_descendants(onto_node: str):
    return _get_ancestors_or_descendants(onto_node=onto_node, ancestors_or_descendants="descendants")


def check_and_set_species(topic_entity_tag_data):

    topic = topic_entity_tag_data['topic']
    entity_type = topic_entity_tag_data['entity_type']
    if topic == species_atp or entity_type == species_atp:
        topic_entity_tag_data['species'] = None


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
    elif topic in sgd_omics_topics and display_tag != sgd_omics_display_tag:
        topic_entity_tag_data['display_tag'] = sgd_omics_display_tag
    elif topic in sgd_additional_topics and display_tag != sgd_additional_display_tag:
        topic_entity_tag_data['display_tag'] = sgd_additional_display_tag
    ### for review, other primary literature, other additional literature
    if topic == topic_entity_tag_data['display_tag']:
        if topic_entity_tag_data['entity_type']:
            topic_entity_tag_data['topic'] = topic_entity_tag_data['entity_type']
        else:
            # when there is no entity attached to the paper
            # currently 2% review papers without an entity attached
            topic_entity_tag_data['topic'] = root_topic_atp
