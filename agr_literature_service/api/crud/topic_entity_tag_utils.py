import json
import urllib.request
from os import environ
from typing import Dict, List
from urllib.error import HTTPError

from cachetools.func import ttl_cache
from fastapi import HTTPException
from sqlalchemy.orm import Session
from starlette import status

from agr_literature_service.api.models import TopicEntityTagSourceModel, ReferenceModel, ModModel, TopicEntityTagModel
from agr_literature_service.api.user import add_user_if_not_exists
from agr_literature_service.lit_processing.utils.okta_utils import get_authentication_token

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
    if "created_by" in audited_obj and audited_obj["created_by"]:
        add_user_if_not_exists(db, audited_obj["created_by"])
    if "updated_by" in audited_obj and audited_obj["updated_by"]:
        add_user_if_not_exists(db, audited_obj["updated_by"])


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


def get_sorted_column_values(reference_id: int, db: Session, column_name: str, token, desc: bool = False):

    if column_name == "entity":
        results = (
            db.query(
                TopicEntityTagModel.entity,
                TopicEntityTagModel.entity_type,
                TopicEntityTagModel.species
            )
            .filter(TopicEntityTagModel.reference_id == reference_id)
            .distinct()
            .all()
        )
        distinct_values = [
            {"entity": row.entity,
             "entity_type": row.entity_type,
             "species": row.species}
            for row in results if row.entity is not None
        ]
        curie_name_map = get_map_aterm_entity_curies_to_names(distinct_values, token)
    else:
        curies = db.query(getattr(TopicEntityTagModel, column_name)).filter(
            TopicEntityTagModel.reference_id == reference_id).distinct()
        curie_name_map = get_map_ateam_curies_to_names(column_name,
                                                       [curie[0] for curie in curies if curie[0]], token)
    return [curie for name, curie in sorted([(value, key) for key, value in curie_name_map.items()],
                                            key=lambda x: x[0], reverse=desc)]


def get_map_aterm_entity_curies_to_names(distinct_values, token):

    """
    entity_type:
    ATP:0000128 protein containing complex
    ATP:0000006 allele
    ATP:0000005 gene
    ATP:0000123 species
    """

    entity_types = list(set([value['entity_type'] for value in distinct_values if 'entity_type' in value]))

    entity_type_curie_name_map = get_map_ateam_curies_to_names("entity_type",
                                                               entity_types,
                                                               token)
    taxon_category_to_entity_curies = {}
    for row in distinct_values:
        entity_curie = row['entity']
        category = entity_type_curie_name_map[row['entity_type']]
        if "complex" in category:
            category = "complex"
        elif "pathway" in category:
            category = "pathway"
        taxon = row['species']
        entity_curies = taxon_category_to_entity_curies.setdefault((taxon, category), [])
        entity_curies.append(entity_curie)
        taxon_category_to_entity_curies[(taxon, category)] = entity_curies

    """
    {
    ('NCBITaxon:559292', 'gene'): ['SGD:S000001085', 'SGD:S000001086',
                                   'SGD:S000001855', 'SGD:S000002592'],
    ('NCBITaxon:6239', 'gene'): ['WB:WBGene00003001'],
    ('NCBITaxon:7955', 'gene'): ['ZFIN:ZDB-GENE-000607-29', 'ZFIN:ZDB-GENE-000816-1',
                                 'ZFIN:ZDB-GENE-980526-255', 'ZFIN:ZDB-GENE-980526-488',
                                 'ZFIN:ZDB-GENE-990415-72', 'ZFIN:ZDB-GENE-991228-4']
    }
    """

    ateam_api_base_url = environ.get('ATEAM_API_URL', "https://beta-curation.alliancegenome.org/api")
    entity_curie_to_name_map = {}
    for (taxon, category) in taxon_category_to_entity_curies:
        ateam_api = f'{ateam_api_base_url}/{category}/search?limit=1000&page=0'
        entity_curies = taxon_category_to_entity_curies[(taxon, category)]
        request_body = {
            "searchFilters": {
                "nameFilters": {
                    "curie_keyword": {
                        "queryString": " ".join(entity_curies),
                        "tokenOperator": "OR"
                    }
                },
                "taxonFilters": {
                    "taxon.curie_keyword": {
                        "queryString": taxon,
                        "tokenOperator": "AND"
                    }
                }
            }
        }
        curie_to_name_map = get_data_from_ateam_api(ateam_api, category, request_body, token)
        for entity_curie in entity_curies:
            entity_curie_to_name_map[entity_curie] = curie_to_name_map.get(entity_curie, entity_curie)
    """
    {'SGD:S000001085': 'DOG2', 'SGD:S000001086': 'DOG1', 'SGD:S000001855': 'ACT1', 'SGD:S000002592': 'ATC1', 'WB:WBGene00003001': 'lin-12', 'ZFIN:ZDB-GENE-000607-29': 'id:ibd5038', 'ZFIN:ZDB-GENE-000816-1': 'fgfr3', 'ZFIN:ZDB-GENE-980526-255': 'fgfr1a', 'ZFIN:ZDB-GENE-980526-488': 'fgfr4', 'ZFIN:ZDB-GENE-990415-72': 'fgf8a', 'ZFIN:ZDB-GENE-991228-4': 'etv5b'}
    """
    return entity_curie_to_name_map


def get_map_ateam_curies_to_names(curies_category, curies, token):
    ateam_api_base_url = environ.get('ATEAM_API_URL', "https://beta-curation.alliancegenome.org/api")
    if curies_category == "species":
        curies_category = "ncbitaxonterm"
    else:
        curies_category = "atpterm"
    ateam_api = f'{ateam_api_base_url}/{curies_category}/search?limit=1000&page=0'
    request_body = {
        "searchFilters": {
            "nameFilters": {
                "curie_keyword": {
                    "queryString": " ".join(curies),
                    "tokenOperator": "OR"
                }
            }
        }
    }
    return get_data_from_ateam_api(ateam_api, curies_category, request_body, token)


def get_data_from_ateam_api(ateam_api, curies_category, request_body, token):

    request_data_encoded = json.dumps(request_body)
    request_data_encoded_str = str(request_data_encoded)
    request = urllib.request.Request(url=ateam_api, data=request_data_encoded_str.encode('utf-8'))
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("Content-type", "application/json")
    request.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(request) as response:
            resp = response.read().decode("utf8")
            resp_obj = json.loads(resp)
            # from the A-team API, atp values have a "name" field and other entities (e.g., genes and alleles) have
            # symbol objects - e.g., geneSymbol.displayText
            return {entity["curie"]: entity["name"] if "name" in entity else entity[
                curies_category + "Symbol"]["displayText"] for entity in (resp_obj["results"] if "results" in
                                                                                                 resp_obj else [])}
    except HTTPError:
        return {}


@ttl_cache(maxsize=128, ttl=60 * 60)
def get_ancestors_or_descendants(onto_node: str, ancestors_or_descendants: str = 'ancestors') -> List[str]:
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
    request = urllib.request.Request(url=ateam_api)
    request.add_header("Authorization", f"Bearer {token}")
    request.add_header("Content-type", "application/json")
    request.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(request) as response:
            resp = response.read().decode("utf8")
            resp_obj = json.loads(resp)
            return [entity["curie"] for entity in resp_obj["entities"]] if "entities" in resp_obj else []
    except HTTPError:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Error from A-team API")


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
