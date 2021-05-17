import sqlalchemy

from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi import UploadFile

from fastapi.encoders import jsonable_encoder

from fastapi_sqlalchemy import db

from literature.models import Reference
from literature.models import Resource

from literature.schemas import FileSchemaUpdate
from literature.schemas import FileSchemaShow


def create(file: UploadFile):
    print(file.content_type)
    file_contents = file.read()
    md5sum = hashlib.md5(file_contents).hexdigest()
    print(md5sum)
    exit()

    db.session.add(db_obj)
    db.session.commit()
    db.session.refresh(db_obj)

    return db_obj


def destroy(filename: str):
    file = db.session.query(File).filter(File.filename == filename).first()
    if not file:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"file with name {filename} not found")
    db.session.delete(file)
    db.session.commit()

    return None


def update(filename: str, file_update: FileSchemaUpdate):

    file_db_obj = db.session.query(File).filter(file.filename == filename).first()
    if not file_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Filename with filename {filename} not found")


    if author_update.resource_curie and author_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")

    db.session.commit()
    db.flush()

    return file_db_obj


def show(filename: str):
    file = db.session.query(File).filter(File.filename == filename).first()

    if not file:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with the filename {filename} is not available")

    return file


def show_changesets(filename: str):
    file = db.session.query(File).filter(File.filename == filename).first()
    if not file:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with the filename {filename} is not available")

    changesets = []
    for version in filename.versions:
        changesets.append(version.changeset)

    return changesets
