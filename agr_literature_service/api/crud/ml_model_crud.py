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
        else:
            latest_version_num = latest_version_num[0]
        request.version_num = latest_version_num + 1

    if request.production:
        # Check if we have a production one already.
        query = db.query(MLModel).filter(
            MLModel.task_type == request.task_type,
            MLModel.mod_id == mod.mod_id,
            MLModel.production == True,
            MLModel.topic == request.topic
        )
        model = query.one_or_none()
        if model:
            model.production = False
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
        dataset_id=request.dataset_id,
        production=request.production,
        species=request.species,
        data_novelty=request.data_novelty,
        negated=request.negated
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
    topic = request.topic if request.topic is not None else "None"
    folder = get_ml_model_s3_folder(request.task_type, request.mod_abbreviation, topic)
    temp_file_name = f"{str(request.version_num)}.gz"
    with gzip.open(temp_file_name, 'wb') as f_out:
        shutil.copyfileobj(file.file, f_out)
    with open(temp_file_name, 'rb') as gzipped_file:
        upload_file_to_bucket(s3_client=s3_client, file_obj=gzipped_file, bucket="agr-literature", folder=folder,
                              object_name=str(request.version_num) + ".gz", ExtraArgs=extra_args)
    os.remove(temp_file_name)
    return get_model_schema_from_orm(new_model)


def destroy(db: Session, ml_model_id: int):
    model = db.query(MLModel).filter(MLModel.ml_model_id == ml_model_id).first()
    if model:
        folder = get_ml_model_s3_folder(model.task_type, model.mod.abbreviation, model.topic)
        object_key = f"{folder}/{str(model.version_num)}.gz"
        # Delete the file from S3
        s3_client.delete_object(Bucket='agr-literature', Key=object_key)
        # Do not delete the model from the database
        # Might be needed for TET references
        # Deleting the bucket file itself should be enough
        # db.delete(model)
        # db.commit()


def cleanup(file_path):
    os.remove(file_path)


def get_mod(db: Session, mod_abbreviation: str):
    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=404, detail="Mod not found")
    return mod


def get_model(db: Session, task_type: str, mod_id: int, topic: str = None, version: str = None):
    """NOTE: version can be the integer value to be used OR one of 'production' or 'latest' """
    query = db.query(MLModel).filter(
        MLModel.task_type == task_type,
        MLModel.mod_id == mod_id,
        MLModel.topic == topic
    )
    if version is not None:
        try:
            arg = int(version)
            query = query.filter(MLModel.version_num == arg)
        except ValueError:
            if version == 'production':
                query = query.filter(MLModel.production)
            elif version == 'latest':
                query = query.order_by(MLModel.version_num.desc())
            else:
                raise HTTPException(status_code=404, detail=f"version '{version}' is neither an integer or one the strings 'production' or 'latest'.")
    else:
        query = query.order_by(MLModel.version_num.desc())
    model = query.first()
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    return model


def get_model_schema_from_orm(model: MLModel):
    model_data = {
        "task_type": model.task_type,
        "mod_abbreviation": model.mod.abbreviation,
        "topic": model.topic,
        "version_num": model.version_num,
        "file_extension": model.file_extension,
        "model_type": model.model_type,
        "precision": model.precision,
        "recall": model.recall,
        "f1_score": model.f1_score,
        "parameters": model.parameters,
        "dataset_id": model.dataset_id,
        "ml_model_id": model.ml_model_id,
        "production": model.production,
        "species": model.species,
        "data_novelty": model.data_novelty,
        "negated": model.negated
    }
    return MLModelSchemaShow(**model_data)


def get_model_metadata(db: Session, task_type: str, mod_abbreviation: str, topic: str = None, version: str = None):
    mod = get_mod(db, mod_abbreviation)
    model = get_model(db, task_type, mod.mod_id, topic, version)
    return get_model_schema_from_orm(model)


def download_model_file(db: Session, task_type: str, mod_abbreviation: str, topic: str = None, version: str = None):
    mod = get_mod(db, mod_abbreviation)
    model = get_model(db, task_type, mod.mod_id, topic, version)
    topic = topic if topic is not None else "None"

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
