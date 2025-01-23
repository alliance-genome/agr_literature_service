import gzip
import os
import shutil

import boto3
from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask
from starlette.responses import FileResponse

from agr_literature_service.api.models import ModModel
from agr_literature_service.api.models.ml_model_model import MLModel
from agr_literature_service.api.s3.upload import upload_file_to_bucket
from agr_literature_service.api.schemas.ml_model_schemas import MLModelSchemaPost, MLModelSchemaShow
from agr_literature_service.lit_processing.utils.s3_utils import download_file_from_s3

s3_client = boto3.client('s3')


def get_ml_model_s3_folder(task_type: str, mod_abbreviation: str, topic: str):
    env_state = os.environ.get("ENV_STATE", "")
    if env_state == "" or env_state == "test":
        s3_folder = "test"
    elif env_state == "prod":
        s3_folder = "prod"
    else:
        s3_folder = "develop"
    folder = f"{s3_folder}/ml_models/{task_type}/{mod_abbreviation}/{topic}"
    return folder


def upload(db: Session, request: MLModelSchemaPost, file: UploadFile):
    mod = get_mod(db, request.mod_abbreviation)
    if request.version_num is None or request.version_num <= 0:
        latest_version_num = db.query(MLModel.version_num).filter(
            MLModel.task_type == request.task_type,
            MLModel.mod_id == mod.mod_id,
            MLModel.topic == request.topic
        ).order_by(MLModel.version_num.desc()).first()

        if latest_version_num is None:
            latest_version_num = 0
        request.version_num = latest_version_num + 1

    # Save metadata to the database
    new_model = MLModel(
        task_type=request.task_type,
        mod_id=mod.mod_id,
        topic=request.topic,
        version_num=request.version_num,
        file_extension=request.file_extension,
        model_type=request.model_type,
        precision=request.precision,
        recall=request.recall,
        f1_score=request.f1_score,
        parameters=request.parameters,
        dataset_id=request.dataset_id
    )
    try:
        db.add(new_model)
        db.commit()
        db.refresh(new_model)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create ML model. Reason: {str(e)}")

    # Upload model file to S3
    env_state = os.environ.get("ENV_STATE", "")
    extra_args = {'StorageClass': 'GLACIER_IR'} if env_state == "prod" else {'StorageClass': 'STANDARD'}
    folder = get_ml_model_s3_folder(request.task_type, request.mod_abbreviation, request.topic)
    content_encoding = file.headers.get("Content-Encoding")
    temp_file_name = f"{str(request.version_num)}.gz"
    with gzip.open(temp_file_name, 'wb') as f_out:
        shutil.copyfileobj(file.file, f_out)
    with open(temp_file_name, 'rb') as gzipped_file:
        upload_file_to_bucket(s3_client=s3_client, file_obj=gzipped_file, bucket="agr-literature", folder=folder,
                              object_name=str(request.version_num) + ".gz", ExtraArgs=extra_args)
    os.remove(temp_file_name)
    return new_model.ml_model_id


def destroy(db: Session, ml_model_id: int):
    model = db.query(MLModel).filter(MLModel.ml_model_id == ml_model_id).first()
    if model:
        folder = get_ml_model_s3_folder(model.task_type, model.mod.mod_abbreviation, model.topic)
        object_key = f"{folder}/{str(model.version_num)}.gz"
        # Delete the file from S3
        s3_client.delete_object(Bucket='agr-literature', Key=object_key)
        # Delete the model from the database
        db.delete(model)
        db.commit()


def cleanup(file_path):
    os.remove(file_path)


def get_mod(db: Session, mod_abbreviation: str):
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=404, detail="Mod not found")
    return mod


def get_model(db: Session, task_type: str, mod_id: int, topic: str, version_num: int = None):
    query = db.query(MLModel).filter(
        MLModel.task_type == task_type,
        MLModel.mod_id == mod_id,
        MLModel.topic == topic
    )
    if version_num is not None and version_num > 0:
        query = query.filter(MLModel.version_num == version_num)
    else:
        query = query.order_by(MLModel.version_num.desc())
    model = query.first()
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    return model


def get_model_metadata(db: Session, task_type: str, mod_abbreviation: str, topic: str, version_num: int = None):
    mod = get_mod(db, mod_abbreviation)
    model = get_model(db, task_type, mod.mod_id, topic, version_num)
    return MLModelSchemaShow.from_orm(model)


def download_model(db: Session, task_type: str, mod_abbreviation: str, topic: str, version_num: int = None):
    mod = get_mod(db, mod_abbreviation)
    model = get_model(db, task_type, mod.mod_id, topic, version_num)
    folder = get_ml_model_s3_folder(task_type, mod_abbreviation, topic)
    object_key = f"{folder}/{str(model.version_num)}.gz"
    file_name_gzipped = f"{str(model.version_num)}.gz"
    file_name = f"{str(model.version_num)}.{model.file_extension}"
    download_file_from_s3(file_name_gzipped, bucketname="agr-literature", s3_file_location=object_key)
    with gzip.open(file_name_gzipped, 'rb') as f_in, open(file_name, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(file_name_gzipped)
    return FileResponse(path=file_name, filename=file_name, media_type="application/octet-stream",
                        background=BackgroundTask(cleanup, file_name))