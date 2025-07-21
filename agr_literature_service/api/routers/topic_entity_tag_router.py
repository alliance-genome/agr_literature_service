from multiprocessing import Process, Value
from typing import List, Dict, Union

from fastapi import APIRouter, Depends, Response, Security, status, HTTPException
from fastapi_okta import OktaUser
from sqlalchemy.orm import Session

from agr_literature_service.api import database
from agr_literature_service.api.crud import topic_entity_tag_crud, \
    ateam_db_helpers, topic_entity_tag_utils
from agr_literature_service.api.routers.authentication import auth
from agr_literature_service.api.routers.okta_utils import get_okta_mod_access
from agr_literature_service.api.schemas import TopicEntityTagSchemaShow, TopicEntityTagSchemaPost, ResponseMessageSchema
from agr_literature_service.api.schemas.topic_entity_tag_schemas import TopicEntityTagSchemaRelated, \
    TopicEntityTagSourceSchemaUpdate, TopicEntityTagSchemaUpdate, \
    TopicEntityTagSourceSchemaShow, TopicEntityTagSourceSchemaCreate
from agr_literature_service.api.user import set_global_user_from_okta

router = APIRouter(
    prefix="/topic_entity_tag",
    tags=['Topic Entity Tag']
)


get_db = database.get_db
db_session: Session = Depends(get_db)
db_user = Security(auth.get_user)

revalidate_all_tags_already_running = Value('b', False)


@router.post('/', status_code=status.HTTP_201_CREATED, response_model=Dict)
def create_tag(request: TopicEntityTagSchemaPost, user: OktaUser = db_user, db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.create_tag(db, request)


@router.get('/{topic_entity_tag_id}',
            response_model=TopicEntityTagSchemaShow,
            status_code=200)
def show_tag(topic_entity_tag_id: int,
             db: Session = db_session):
    return topic_entity_tag_crud.show_tag(db, topic_entity_tag_id)


@router.patch('/{topic_entity_tag_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch_tag(topic_entity_tag_id: int,
              request: TopicEntityTagSchemaUpdate,
              user: OktaUser = db_user,
              db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.patch_tag(db, topic_entity_tag_id, request)


@router.delete('/{topic_entity_tag_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete_tag(topic_entity_tag_id,
               user: OktaUser = db_user,
               db: Session = db_session):
    set_global_user_from_okta(db, user)
    topic_entity_tag_crud.destroy_tag(db, topic_entity_tag_id, get_okta_mod_access(user))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post('/source',
             status_code=status.HTTP_201_CREATED,
             response_model=int)
def create_source(request: TopicEntityTagSourceSchemaCreate,
                  user: OktaUser = db_user,
                  db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.create_source(db, request)


@router.delete('/source/{topic_entity_tag_source_id}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete_source(topic_entity_tag_source_id,
                  user: OktaUser = db_user,
                  db: Session = db_session):
    set_global_user_from_okta(db, user)
    topic_entity_tag_crud.destroy_source(db, topic_entity_tag_source_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch('/source/{topic_entity_tag_source_id}',
              status_code=status.HTTP_202_ACCEPTED,
              response_model=ResponseMessageSchema)
def patch_source(topic_entity_tag_source_id,
                 request: TopicEntityTagSourceSchemaUpdate,
                 user: OktaUser = db_user,
                 db: Session = db_session):
    set_global_user_from_okta(db, user)
    return topic_entity_tag_crud.patch_source(db, topic_entity_tag_source_id, request)


@router.get('/source/all',
            status_code=200)
def show_all_source(db: Session = db_session):
    return topic_entity_tag_crud.show_all_source(db)


@router.get('/source/{topic_entity_tag_source_id}',
            response_model=TopicEntityTagSourceSchemaShow,
            status_code=200)
def show_source(topic_entity_tag_source_id: int,
                db: Session = db_session):
    return topic_entity_tag_crud.show_source(db, topic_entity_tag_source_id)


@router.get('/source/{source_evidence_assertion}/{source_method}/{data_provider}/{secondary_data_provider_abbreviation}',
            response_model=TopicEntityTagSourceSchemaShow,
            status_code=200)
def show_source_by_name(source_evidence_assertion: str,
                        source_method: str,
                        data_provider: str,
                        secondary_data_provider_abbreviation: str,
                        db: Session = db_session):
    return topic_entity_tag_crud.show_source_by_name(db, source_evidence_assertion, source_method,
                                                     data_provider, secondary_data_provider_abbreviation)


@router.get('/by_reference/{curie_or_reference_id}',
            status_code=200)
def show_all_reference_tags(
    curie_or_reference_id: str,
    page: int = 1,
    page_size: int = None,
    column_only: str = None,
    column_filter: str = None,
    column_values: str = None,
    count_only: bool = False,
    sort_by: str = None,
    desc_sort: bool = False,
    db: Session = db_session
) -> Union[List[TopicEntityTagSchemaRelated], int]:
    result = topic_entity_tag_crud.show_all_reference_tags(
        db, curie_or_reference_id,
        page, page_size,
        count_only, sort_by, desc_sort,
        column_only, column_filter,
        column_values
    )
    return result


@router.get('/by_mod/{mod_abbreviation}',
            status_code=200)
def get_reference_tags(mod_abbreviation: str,
                       days_updated: int = 7,
                       db: Session = db_session):
    return topic_entity_tag_crud.get_all_topic_entity_tags_by_mod(db, mod_abbreviation, days_updated)


@router.get('/get_curie_to_name_from_all_tets/',
            response_model=Dict[str, str],
            status_code=200)
def get_curie_to_name_from_all_tets(curie_or_reference_id: str,
                                    db: Session = db_session):
    return topic_entity_tag_crud.get_curie_to_name_from_all_tets(db, curie_or_reference_id)


def revalidate_tags_process_wrapper(already_running, email: str, delete_all_first: bool, curie_or_reference_id: str,
                                    validation_values_only: bool):
    try:
        already_running.value = True
        topic_entity_tag_crud.revalidate_all_tags(email=email, delete_all_first=delete_all_first,
                                                  curie_or_reference_id=curie_or_reference_id,
                                                  validation_values_only=validation_values_only)
    finally:
        already_running.value = False


@router.get('/revalidate_all_tags/',
            status_code=200)
def revalidate_all_tags(email: str = None,
                        delete_all_tags_first: bool = False,
                        curie_or_reference_id: str = None,
                        validation_values_only: bool = False,
                        user: OktaUser = db_user,
                        db: Session = db_session):
    if not user.groups or "SuperAdmin" not in user.groups:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Only users in the okta 'SuperAdmin' group are allowed to perform this request.")
    set_global_user_from_okta(db, user)
    global revalidate_all_tags_already_running
    if email is None:
        return {
            "message": "You need to provide an email address to revalidate all tags. You will receive an email at "
                       "the end of the validation process"
        }
    if revalidate_all_tags_already_running.value:
        return {
            "message": "Revalidation in progress, no need to submit the request again."
        }
    else:
        p = Process(target=revalidate_tags_process_wrapper,
                    args=(revalidate_all_tags_already_running, email, delete_all_tags_first, curie_or_reference_id,
                          validation_values_only))
        p.start()
        return {
            "message": "Revalidation of all tags started. You will receive an email when done."
        }


@router.delete('/delete_manual_tets/{reference_curie}/{mod_abbreviation}',
               status_code=status.HTTP_204_NO_CONTENT)
def delete_manual_tags(reference_curie,
                       mod_abbreviation,
                       user: OktaUser = db_user,
                       db: Session = db_session):
    set_global_user_from_okta(db, user)
    topic_entity_tag_utils.delete_manual_tets(db, reference_curie, mod_abbreviation)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# Use :path so entity_list can include slashes.
@router.get('/entity_validation/{taxon}/{entity_type}/{entity_list:path}',
            status_code=200)
def entity_validation(taxon: str,
                      entity_type: str,
                      entity_list: str):
    return ateam_db_helpers.map_entity_to_curie(entity_type, entity_list, taxon)


@router.get('/search_topic/{topic}',
            status_code=200)
def search_topic(topic: str,
                 mod_abbr: str = None):
    return ateam_db_helpers.search_topic(topic, mod_abbr)


@router.get('/search_descendants/{ancestor_curie}',
            status_code=200)
def search_descendants(ancestor_curie: str):
    return ateam_db_helpers.atp_get_all_descendents(ancestor_curie)


@router.get('/search_species/{species}',
            status_code=200)
def search_species(species: str):
    return ateam_db_helpers.search_species(species)


@router.post('/set_no_tet_status/{mod_abbreviation}/{reference_curie}/{uid}',
             status_code=200)
def set_no_tet_status(mod_abbreviation: str,
                      reference_curie: str,
                      uid: str,
                      db: Session = db_session):
    return topic_entity_tag_crud.set_indexing_status_for_no_tet_data(db,
                                                                     mod_abbreviation,
                                                                     reference_curie,
                                                                     uid)
