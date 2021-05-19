import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi_sqlalchemy import db

#from literature.schemas import ResourceDescriptor

from literature.models import ResourceDescriptor

from initialize import update_resource_descriptor

def update():
    return update_resource_descriptor()


def show():
    return db.session.query(ResourceDescriptor).all()
