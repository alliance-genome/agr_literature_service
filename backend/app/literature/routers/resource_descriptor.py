from typing import List

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status
from fastapi import Response
from fastapi import Security

from fastapi_auth0 import Auth0User
from literature.user import set_global_user_id

#from literature.schemas import ResourceDesciptorSchema

from literature.crud import resource_descriptor_crud
from literature.routers.authentication import auth

router = APIRouter(
    prefix="/resource-descriptor",
    tags=['ResourceDescriptor']
)


@router.get('/',
            status_code=200)
def show():
    return resource_descriptor.show()


@router.put('/',
            status_code=status.HTTP_202_ACCEPTED,
            dependencies=[Depends(auth.implicit_scheme)])
def update(user: Auth0User = Security(auth.get_user)):
    set_global_user_id(user.id)
    return resource_descriptor_crud.update()
