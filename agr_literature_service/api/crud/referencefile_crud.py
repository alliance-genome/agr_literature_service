import gzip
import hashlib
import logging
import os
import shutil

import boto3
from fastapi import HTTPException, status, UploadFile
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_utils import read_referencefile_db_obj_from_md5sum_or_id, \
    create as create_metadata, get_s3_folder_from_md5sum
from agr_literature_service.api.models import ReferenceModel, ReferencefileModel
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
            del ref_file_mod_dict["referencefile_id"]
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


def destroy(db: Session, md5sum_or_referencefile_id: str):
    referencefile = read_referencefile_db_obj_from_md5sum_or_id(db, md5sum_or_referencefile_id)
    if os.environ.get("ENV_STATE", "test") != "test":
        remove_file_from_s3(referencefile.md5sum)
    db.delete(referencefile)
    db.commit()


def file_upload(db: Session, metadata: dict, file: UploadFile):
    md5sum_hash = hashlib.md5()
    for byte_block in iter(lambda: file.file.read(4096), b""):
        md5sum_hash.update(byte_block)
    md5sum = md5sum_hash.hexdigest()
    folder = get_s3_folder_from_md5sum(md5sum)
    referencefile = db.query(ReferencefileModel).filter(ReferencefileModel.md5sum == md5sum).one_or_none()
    if referencefile is not None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail="The provided file md5sum is already present in the system")
    create_request = ReferencefileSchemaPost(md5sum=md5sum, **metadata)
    create_metadata(db, create_request)
    file.file.seek(0)
    temp_file_name = metadata["display_name"] + "." + metadata["file_extension"] + ".gz"
    with gzip.open(temp_file_name, 'wb') as f_out:
        shutil.copyfileobj(file.file, f_out)
    client = boto3.client('s3')
    with open(temp_file_name, 'rb') as gzipped_file:
        upload_file_to_bucket(s3_client=client, file_obj=gzipped_file, bucket="agr-literature", folder=folder,
                              object_name=md5sum + ".gz", ExtraArgs={'StorageClass': 'GLACIER_IR'})
    os.remove(temp_file_name)
    return md5sum
