from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Security
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.auth import get_authenticated_user
from agr_literature_service.api.crud import ateam_db_helpers

router = APIRouter(
    prefix="/ontology",
    tags=['Ontology']
)


get_db = database.get_db
db_session: Session = Depends(get_db)


@router.get('/entity_validation/{taxon}/{entity_type}/{entity_list:path}',
            status_code=200)
def entity_validation(taxon: str,
                      entity_type: str,
                      entity_list: str,
                      user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    return ateam_db_helpers.map_entity_to_curie(entity_type, entity_list, taxon)


@router.get('/map_curie_to_name/{category}/{curie}',
            status_code=200)
def map_curie_to_name(category: str,
                      curie: str,
                      user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    mapping = ateam_db_helpers.map_curies_to_names(category, [curie])
    return mapping.get(curie, curie)


@router.get('/search_topic/{topic}',
            status_code=200)
def search_topic(topic: str,
                 mod_abbr: str = None,
                 user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    return ateam_db_helpers.search_topic(topic, mod_abbr)


@router.get('/search_descendants/{ancestor_curie}/{direct_children_only}/{include_self}/{include_names}',
            status_code=200)
@router.get('/search_descendants/{ancestor_curie}',
            status_code=200)
def search_descendants(ancestor_curie: str,
                       direct_children_only: bool = False,
                       include_self: bool = False,
                       include_names: bool = False,
                       user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    return ateam_db_helpers.atp_get_all_descendants(ancestor_curie, direct_children_only, include_self, include_names)


@router.get('/search_species/{species}',
            status_code=200)
def search_species(species: str,
                   user: Optional[Dict[str, Any]] = Security(get_authenticated_user)):
    return ateam_db_helpers.search_species(species)
