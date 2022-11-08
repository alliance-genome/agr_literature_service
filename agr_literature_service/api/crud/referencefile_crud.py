import logging

import boto3
from fastapi import HTTPException, status, UploadFile
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_utils import read_referencefile_db_obj_from_md5sum_or_id, \
    create as create_metadata, get_s3_folder_from_md5sum
from agr_literature_service.api.models import ReferenceModel
from agr_literature_service.api.s3.delete import delete_file_in_bucket
from agr_literature_service.api.s3.upload import upload_file_to_bucket
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost
from agr_literature_service.api.schemas.response_message_schemas import messageEnum

logger = logging.getLogger(__name__)


def show(db: Session, md5sum_or_referencefile_id: str):
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    referencefile_dict = jsonable_encoder(referencefile)
    referencefile_dict["reference_curie"] = db.query(ReferenceModel.curie).filter(
        ReferenceModel.reference_id == referencefile_dict["reference_id"]).one()[0]
    del referencefile_dict["reference_id"]
    referencefile_dict["referencefile_mods"] = []
    if referencefile.referencefile_mods:
        for ref_file_mod in referencefile.referencefile_mods:
            ref_file_mod_dict = jsonable_encoder(ref_file_mod)
            del ref_file_mod_dict["mod_id"]
            if ref_file_mod.mod is not None:
                ref_file_mod_dict["mod_abbreviation"] = ref_file_mod.mod.abbreviation
            else:
                ref_file_mod_dict["mod_abbreviation"] = None
            referencefile_dict["referencefile_mods"].append(ref_file_mod_dict)
    return referencefile_dict


def patch(db: Session, md5sum_or_referencefile_id: str, request):
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    if "reference_curie" in request:
        res = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == request.reference_curie).one_or_none()
        if res is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Reference with curie {request.reference_curie} is not available")

        request["reference_id"] = res[0]
        del request["reference_curie"]
    for field, value in request.items():
        setattr(referencefile, field, value)
    db.commit()
    return {"message": messageEnum.updated}


def remove_file_from_s3(md5sum: str):
    folder = get_s3_folder_from_md5sum(md5sum)
    client = boto3.client('s3')
    if not delete_file_in_bucket(s3_client=client, bucket="agr-literature", folder=folder, object_name=md5sum + ".gz"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with md5sum {md5sum} is not available")


def destroy(db: Session, md5sum_or_referencefile_id: str, delete_file: bool = False):
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    if delete_file:
        remove_file_from_s3(referencefile.md5sum)
    db.delete(referencefile)
    db.commit()


def file_upload(db: Session, metadata: dict, file: UploadFile):
    # TODO: calculate md5sum and gzip file
    md5sum = "random"
    folder = get_s3_folder_from_md5sum(md5sum)
    create_request = ReferencefileSchemaPost(md5sum=md5sum, **metadata)
    create_metadata(db, create_request)
    client = boto3.client('s3')
    upload_file_to_bucket(s3_client=client, file_obj=file.file, bucket="agr-literature", folder=folder,
                          object_name=md5sum)
    return md5sum
