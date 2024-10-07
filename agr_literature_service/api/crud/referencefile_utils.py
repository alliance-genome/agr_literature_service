import logging
from os import environ

import boto3
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ReferencefileModel
from agr_literature_service.api.s3.delete import delete_file_in_bucket


logger = logging.getLogger(__name__)


def read_referencefile_db_obj(db: Session, referencefile_id: int):
    referencefile = db.query(ReferencefileModel).filter(
        ReferencefileModel.referencefile_id == referencefile_id).one_or_none()

    if not referencefile:
        logger.warning(f"Referencefile not found for referencefile id {referencefile_id}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Referencefile with referencefile_id {referencefile_id} is not available")
    return referencefile


def get_s3_folder_from_md5sum(md5sum: str):
    env_state = environ.get("ENV_STATE", "")
    if env_state == "" or env_state == "test":
        folder = "test"
    elif env_state == "prod":
        folder = "prod"
    else:
        folder = "develop"
    folder += "/reference/documents/"
    folder += "/".join([char for char in md5sum[0:4]])
    return folder


def remove_file_from_s3(md5sum: str):  # pragma: no cover
    folder = get_s3_folder_from_md5sum(md5sum)
    client = boto3.client('s3')
    if not delete_file_in_bucket(s3_client=client, bucket="agr-literature", folder=folder, object_name=md5sum + ".gz"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with md5sum {md5sum} is not available")


def remove_from_s3_and_db(db: Session, referencefile: ReferencefileModel):
    copies = db.query(ReferencefileModel).filter(ReferencefileModel.md5sum == referencefile.md5sum).all()
    if len(copies) == 1:
        remove_file_from_s3(str(referencefile.md5sum))
    db.delete(referencefile)
    db.commit()
