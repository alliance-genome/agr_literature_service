import io
import os
import hashlib

from botocore.client import BaseClient

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

from literature.s3.upload import upload_file_to_bucket
from literature.s3.delete import delete_file_in_bucket
from literature.s3.download import download_file_from_bucket

from literature.config import config

from literature.models import File


def create(s3: BaseClient, parent_entity_type : str, curie: str, file_contents: str, display_name: str, content_type: str):
    filename, file_extension = os.path.splitext(display_name)
    bucket_name = 'agr-literature'
    md5sum = hashlib.md5(file_contents).hexdigest()
    s3_filename = curie + '-File-' + md5sum + file_extension
    folder= config.ENV_STATE + '/agr/'+ curie

    file_data = {'s3_filename': s3_filename,
                 'size': len(file_contents),
                 'content_type': content_type,
                 'md5sum': md5sum,
                 'folder': folder,
                 'display_name': display_name,
                 'extension': file_extension,
                 'public': False
                 }

    if parent_entity_type == 'reference':
        reference = db.session.query(Reference).filter(Reference.curie == curie).first()
        file_data['reference'] = reference
        if not reference:
            HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                          detail=f"Reference with the curie {curie} is not available")

        file_obj = db.session.query(File).filter(File.md5sum == md5sum,
                                                 File.reference_id == reference.reference_id).first()
        if file_obj:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                detail=f"File with md5sum {md5sum} and Reference Curie {curie} already exists: File ID {file_obj.file_id}")



    upload_obj = upload_file_to_bucket(s3_client=s3,
                                       file_obj=io.BytesIO(file_contents),
                                       bucket=bucket_name,
                                       folder=folder,
                                       object_name=s3_filename)

    if not upload_obj:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Unable to upload file to s3: {display_name}")

    file_db_obj = File(**file_data)

    db.session.add(file_db_obj)
    db.session.commit()
    db.session.refresh(file_db_obj)

    return file_db_obj


def destroy(s3: BaseClient, filename: str):
    file_obj = db.session.query(File).filter(File.s3_filename == filename).first()
    if not file_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"file with name {filename} not found")

    bucket_name = 'agr-literature'
    delete_obj = delete_file_in_bucket(s3_client=s3,
                                       bucket=bucket_name,
                                       folder=file_obj.folder,
                                       object_name=filename)

    db.session.delete(file_obj)
    db.session.commit()

    return None


def update(filename: str, file_update: FileSchemaUpdate):
    file_db_obj = db.session.query(File).filter(File.s3_filename == filename).first()
    if not file_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with filename {filename} not found")

    for field, value in vars(file_update).items():
        if value is None:
            continue
        if field == "reference_curie":
            reference_curie = value
            reference = db.session.query(Reference).filter(Reference.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Reference with curie {reference_curie} does not exist")
            file_db_obj.reference = reference
        else:
            setattr(file_db_obj, field, value)

    db.session.commit()
    db.session.flush()

    return file_db_obj


def show(filename: str):
    file_obj = db.session.query(File).filter(File.s3_filename == filename).first()

    if not file_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with the filename {filename} is not available")

    return file_obj


def download(s3: BaseClient, filename: str):
    file_obj = db.session.query(File).filter(File.s3_filename == filename).first()

    if not file_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with the filename {filename} is not available")

    bucket_name = "agr-literature"

    return [download_file_from_bucket(s3,
                                     bucket_name,
                                     file_obj.folder,
                                     file_obj.s3_filename), file_obj.content_type]


def show(filename: str):
    file_obj = db.session.query(File).filter(File.s3_filename == filename).first()

    if not file_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with the filename {filename} is not available")

    return file_obj


def show_changesets(filename: str):
    file_obj = db.session.query(File).filter(File.s3_filename == filename).first()
    if not file_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with the filename {filename} is not available")

    changesets = []
    for version in file_obj.versions:
        changesets.append(version.changeset)

    return changesets
