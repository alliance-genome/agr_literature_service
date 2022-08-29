from agr_literature_service.api.schemas import ReferenceSchemaPost, ResourceSchemaPost
from agr_literature_service.api.crud.mod_crud import create as mod_create
from agr_literature_service.api.crud.user_crud import create as user_create
from agr_literature_service.api.user import set_global_user_id
from agr_literature_service.api.crud.reference_crud import (
    create as reference_create)
from agr_literature_service.api.crud.resource_crud import (
    create as resource_create)
from sqlalchemy.orm import Session


def initialise(db: Session, test_str: str):

    # add User "XXX_Bob"
    user = user_create(db, "{}_Bob".format(test_str))
    # By adding set_global_user_id here we do not need to pass the
    # created_by and updated_by dict elements to the schema validators.
    set_global_user_id(db, user.id)
    okta_user = user.id

    # add mods
    mods = []
    data = {
        "abbreviation": '{}_FB'.format(test_str),
        "short_name": "{}_FB".format(test_str),
        "full_name": "{}_ont_1".format(test_str)
    }

    mod_create(db, data)  # type: ignore
    mods.append('{}_FB'.format(test_str))

    data = {
        "abbreviation": '{}_RGD'.format(test_str),
        "short_name": "{}_Rat".format(test_str),
        "full_name": "{}_ont_2".format(test_str)
    }
    mod_create(db, data)  # type: ignore
    mods.append('{}_RGD'.format(test_str))

    # Add references and resource
    refs = []
    ress = []
    for title in ['Bob {} 1'.format(test_str), 'Bob {} 2'.format(test_str), 'Bob {} 3'.format(test_str)]:
        reference = ReferenceSchemaPost(title=title, category="thesis", abstract="3", language="MadeUp")
        res = reference_create(db, reference)
        refs.append(res)

        resource = ResourceSchemaPost(title=title, abstract="3", open_access=True)
        res = resource_create(db, resource)
        ress.append(res)
    return (refs, ress, mods, okta_user)
