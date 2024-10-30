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
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.models import TopicEntityTagSourceModel, ReferenceModel, ModModel, TopicEntityTagModel
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
        curie_name_map = _get_map_ateam_entity_curies_to_names(entity_type_to_entities)
    else:
        curies = db.query(getattr(TopicEntityTagModel, column_name)).filter(
            TopicEntityTagModel.reference_id == reference_id).distinct()
        category = "ncbitaxonterm" if column_name == "species" else "atpterm"
        curie_name_map = get_map_ateam_curies_to_names(category,
                                                       [curie[0] for curie in curies if curie[0]])

    return [curie for name, curie in sorted([(value, key) for key, value in curie_name_map.items()],
                                            key=lambda x: x[0], reverse=desc)]


def _get_map_ateam_entity_curies_to_names(entity_type_to_entities):

    entity_types = [entity_type for entity_type in entity_type_to_entities.keys() if entity_type is not None]

    entity_type_curie_name_map = get_map_ateam_curies_to_names("atpterm",
                                                               entity_types)
    entity_curie_to_name_map = {}
    for entity_type in entity_type_to_entities:
        if entity_type is None:
            # entity_curie_to_name_map[entity_type] = entity_type_to_entitie[entity_type][0]
            continue
        entity_curies = entity_type_to_entities[entity_type]
        category = entity_type_curie_name_map[entity_type].replace(" ", "")
        # if "complex" in category:
        #    category = "complex"
        # elif "pathway" in category
        #    category = "pathway"
        if "allele" in category:
            category = "allele"
        curie_to_name_map = get_map_ateam_curies_to_names(category, entity_curies)
        entity_curie_to_name_map.update(curie_to_name_map)

    """
    {'SGD:S000001085': 'DOG2', 'SGD:S000001086': 'DOG1', 'SGD:S000001855': 'ACT1', 'SGD:S000002592': 'ATC1', 'WB:WBGene00003001': 'lin-12', 'ZFIN:ZDB-GENE-000607-29': 'id:ibd5038', 'ZFIN:ZDB-GENE-000816-1': 'fgfr3', 'ZFIN:ZDB-GENE-980526-255': 'fgfr1a', 'ZFIN:ZDB-GENE-980526-488': 'fgfr4', 'ZFIN:ZDB-GENE-990415-72': 'fgf8a', 'ZFIN:ZDB-GENE-991228-4': 'etv5b'}
    """
    return entity_curie_to_name_map


def _get_map_ateam_construct_ids_to_symbols(curies_category, curies, maxret):
    # curies = list(set(curies))
    ateam_api_base_url = environ.get('ATEAM_API_URL')
    ateam_api = f'{ateam_api_base_url}/{curies_category}/search?limit={maxret}&page=0'
    chunked_values = [curies[i:i + maxret] for i in range(0, len(curies), maxret)]
    return_dict = {}
    for chunk in chunked_values:
        request_body = {
            "searchFilters": {
                "modEntityIdFilters": {
                    "modEntityId": {
                        "queryString": " ".join(chunk),
                        "tokenOperator": "OR",
                        "useKeywordFields": False,
                        "queryType": "matchQuery"
                    }
                }
            }
        }
        token = get_authentication_token()
        try:
            request_data_encoded = json.dumps(request_body)
            request_data_encoded_str = str(request_data_encoded)
            request = urllib.request.Request(url=ateam_api, data=request_data_encoded_str.encode('utf-8'))
            request.add_header("Authorization", f"Bearer {token}")
            request.add_header("Content-type", "application/json")
            request.add_header("Accept", "application/json")
        except Exception as e:
            logger.error(f"Exception setting up request:get_map_ateam_curies_to_names: {e}")
            continue
        try:
            with urllib.request.urlopen(request) as response:
                resp = response.read().decode("utf8")
                resp_obj = json.loads(resp)
                for res in resp_obj["results"]:
                    unique_ids = [unique_id for unique_id in res['uniqueId'].split('|') if unique_id not in res['modEntityId']]
                    if unique_ids:
                        unique_id_selected = unique_ids[0]
                    else:
                        unique_id_selected = res['modEntityId']
                    return_dict[res['modEntityId']] = unique_id_selected
                    id_to_name_cache.set(res['modEntityId'], unique_id_selected)
        except HTTPError as e:
            logger.error(f"HTTPError:get_map_ateam_curies_to_names: {e}")
            continue
        except Exception as e:
            logger.error(f"Exception running request:get_map_ateam_curies_to_names: {e}")
            continue
    return return_dict


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


def get_map_entity_curies_to_names(entity_id_validation, curies_category, curies):
    curie_to_name_mapping = {}
    if entity_id_validation == "alliance":
        curie_to_name_mapping.update(get_map_ateam_curies_to_names(curies_category=curies_category,
                                                                   curies=curies))
    elif entity_id_validation.lower() == "wb":
        curie_to_name_mapping.update(_get_map_wb_curies_to_names(curies_category=curies_category, curies=curies))
    elif entity_id_validation.lower() == "sgd":
        curies_category = curies_category.replace("protein containing ", "")
        curie_to_name_mapping.update(_get_map_sgd_curies_to_names(curies_category=curies_category, curies=curies))
    return curie_to_name_mapping


def get_map_ateam_curies_to_names(curies_category, curies, maxret=1000):

    if "allele" in curies_category:
        curies_category = "allele"

    curies_not_in_cache = [curie for curie in set(curies) if id_to_name_cache.get(curie) is None]
    if len(curies_not_in_cache) == 0:
        return {curie: id_to_name_cache.get(curie) for curie in set(curies)}

    if curies_category == 'transgenicconstruct':
        curies_category = 'construct'
        return _get_map_ateam_construct_ids_to_symbols(curies_category, curies_not_in_cache, maxret)

    subtype = None
    if curies_category in ["AGMs", "AffectedGenomeModel", "affected genome model",
                           "strain", "genotype", "fish"]:
        if curies_category in ["strain", "genotype", "fish"]:
            subtype = curies_category
        curies_category = "agm"

    return_dict = {}
    keyword_name = "curie" if curies_category in ["atpterm", "ncbitaxonterm", "ecoterm"] else "modEntityId"
    ateam_api_base_url = environ.get('ATEAM_API_URL')
    ateam_api = f'{ateam_api_base_url}/{curies_category}/search?limit={maxret}&page=0'
    chunked_values = [curies_not_in_cache[i:i + maxret] for i in range(0, len(curies_not_in_cache), maxret)]

    for chunk in chunked_values:
        request_body = {
            "searchFilters": {
                "nameFilters": {
                    keyword_name: {
                        "queryString": " ".join(chunk),
                        "tokenOperator": "OR",
                        "useKeywordFields": False,
                        "queryType": "matchQuery"
                    }
                }
            }
        }
        if subtype:
            request_body["searchFilters"]["subtypeFilters"] = {
                "subtype.name": {
                    "queryString": subtype,
                    "tokenOperator": "OR"
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

                # process the API response and collect mappings
                new_mappings = {}
                if curies_category == "agm":
                    new_mappings = {
                        entity[keyword_name]: entity.get("name") for entity in resp_obj.get("results", [])
                    }
                else:
                    new_mappings = {
                        entity[keyword_name]: entity.get("name") or entity.get(curies_category + "Symbol", {}).get("displayText", entity[keyword_name])
                        for entity in resp_obj.get("results", [])
                    }

                # update return dictionary and cache
                for curie, name in new_mappings.items():
                    id_to_name_cache.set(curie, name)
                    return_dict[curie] = name

                return_dict = fallback_id_to_name_mapping(curies_category, chunk, return_dict)

        except HTTPError as e:

            logger.error(f"HTTPError:get_map_ateam_curies_to_names: {e}")

            return_dict = fallback_id_to_name_mapping(curies_category, chunk, return_dict)

        except Exception as e:

            logger.error(f"Exception in get_map_ateam_curies_to_names: {e}")

            return_dict = fallback_id_to_name_mapping(curies_category, chunk, return_dict)

    # add already cached curies to return_dict
    for curie in set(curies) - set(curies_not_in_cache):
        return_dict[curie] = id_to_name_cache.get(curie)
    return return_dict


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


def delete_manual_tets(db: Session, curie_or_reference_id: str, mod_abbreviation: str):
    """
    for deleting manually added topic entity tags and automated ones imported from mods
    ATP:0000035 => assertion by author
    ATP:0000036 => assertion by professional curator / curator assertion
    """

    ref = get_reference(db=db, curie_or_reference_id=str(curie_or_reference_id))
    if ref is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"The reference curie or id {curie_or_reference_id} is not in the database")
    reference_id = ref.reference_id
    mod = db.query(ModModel).filter_by(abbreviation=mod_abbreviation).one_or_none()
    if mod is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"The mod abbreviation {mod_abbreviation} is not in the database")
    mod_id = mod.mod_id
    try:
        sql_query = text("""
            DELETE FROM topic_entity_tag
            WHERE reference_id = :reference_id
            AND topic_entity_tag_source_id IN (
                SELECT topic_entity_tag_source_id
                FROM topic_entity_tag_source
                WHERE secondary_data_provider_id = :mod_id
                AND (
                   (source_method = 'abc_literature_system' AND source_evidence_assertion IN ('ATP:0000035', 'ATP:0000036')) OR
                   (source_method != 'abc_literature_system' AND source_evidence_assertion NOT IN ('ATP:0000035', 'ATP:0000036'))
                )
            )
        """)

        db.execute(sql_query, {
            'reference_id': reference_id,
            'mod_id': mod_id
        })
        db.commit()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"An error occurred when deleting manual tets: {e}")


def delete_non_manual_tets(db: Session, curie_or_reference_id: str, mod_abbreviation: str):
    """
    ATP:0000035 => assertion by author
    ATP:0000036 => assertion by professional curator / curator assertion
    """

    ref = get_reference(db=db, curie_or_reference_id=str(curie_or_reference_id))
    if ref is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"The reference curie or id {curie_or_reference_id} is not in the database")
    reference_id = ref.reference_id
    mod = db.query(ModModel).filter_by(abbreviation=mod_abbreviation).one_or_none()
    if mod is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"The mod abbreviation {mod_abbreviation} is not in the database")
    mod_id = mod.mod_id
    try:
        sql_query = text("""
        DELETE FROM topic_entity_tag
        WHERE reference_id = :reference_id
        AND EXISTS (
            SELECT 1
            FROM topic_entity_tag_source
            WHERE topic_entity_tag_source_id = topic_entity_tag.topic_entity_tag_source_id
            AND secondary_data_provider_id = :mod_id
            AND source_method = 'abc_literature_system'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM topic_entity_tag_source
            WHERE topic_entity_tag_source_id = topic_entity_tag.topic_entity_tag_source_id
            AND source_evidence_assertion IN ('ATP:0000035', 'ATP:0000036')
        )
        """)
        db.execute(sql_query, {
            'reference_id': reference_id,
            'mod_id': mod_id
        })
        db.commit()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"An error occurred when deleting non-manual tets: {e}")

    sql_query = text("""
        SELECT count(*) FROM topic_entity_tag
        WHERE reference_id = :reference_id
        AND topic_entity_tag_source_id in (
            SELECT topic_entity_tag_source_id
            FROM topic_entity_tag_source
            WHERE secondary_data_provider_id = :mod_id
            AND (
               (source_method = 'abc_literature_system' AND source_evidence_assertion IN ('ATP:0000035', 'ATP:0000036')) OR
               (source_method != 'abc_literature_system' AND source_evidence_assertion NOT IN ('ATP:0000035', 'ATP:0000036'))
            )
        )
    """)

    rows = db.execute(sql_query, {
        'reference_id': reference_id,
        'mod_id': mod_id
    }).fetchall()

    return len(rows)
