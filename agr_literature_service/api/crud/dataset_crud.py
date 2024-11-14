from collections import defaultdict
from typing import Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ModModel
from agr_literature_service.api.models.dataset_model import DatasetModel, DatasetEntryModel
from agr_literature_service.api.schemas.dataset_schema import DatasetSchemaPost, \
    DatasetSchemaDownload, DatasetSchemaUpdate, DatasetSchemaShow


def get_dataset(db: Session, mod_abbreviation: str, data_type: str, dataset_type: str,
                version: int = None) -> Optional[DatasetModel]:
    dataset = db.query(DatasetModel).join(DatasetModel.mod).filter(
        DatasetModel.mod.has(abbreviation=mod_abbreviation),
        DatasetModel.data_type == data_type,
        DatasetModel.dataset_type == dataset_type,
        DatasetModel.version == version,
    ).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


def create_dataset(db: Session, dataset: DatasetSchemaPost) -> DatasetSchemaShow:
    mod = db.query(ModModel).filter(ModModel.abbreviation == dataset.mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=404, detail=f"Mod with abbreviation {dataset.mod_abbreviation} not found")
    max_version = db.query(DatasetModel.version).filter(
        DatasetModel.mod_id == mod.mod_id,
        DatasetModel.data_type == dataset.data_type,
        DatasetModel.dataset_type == dataset.dataset_type
    ).order_by(DatasetModel.version.desc()).first()
    new_version = max_version.version + 1 if max_version else 1
    db_dataset = DatasetModel(
        mod_id=mod.mod_id,
        data_type=dataset.data_type,
        dataset_type=dataset.dataset_type,
        title=dataset.title,
        description=dataset.description,
        version=new_version,
        frozen=False,
        production=False
    )
    db.add(db_dataset)
    try:
        db.commit()
        db.refresh(db_dataset)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create dataset. Reason: {str(e)}")
    return DatasetSchemaShow(
        dataset_id=db_dataset.dataset_id,
        mod_abbreviation=db_dataset.mod.abbreviation,
        data_type=db_dataset.data_type,
        dataset_type=db_dataset.dataset_type,
        version=db_dataset.version,
        title=db_dataset.title,
        description=db_dataset.description,
        created_by=db_dataset.created_by,
        updated_by=db_dataset.updated_by,
        date_created=str(db_dataset.date_created),
        date_updated=str(db_dataset.date_updated)
    )


def delete_dataset(db: Session, mod_abbreviation: str, data_type: str, dataset_type: str, version: int):
    dataset = get_dataset(db, mod_abbreviation, data_type, dataset_type, version)
    db.delete(dataset)
    db.commit()


def download_dataset(db: Session, mod_abbreviation: str, data_type: str,
                     dataset_type: str, version: int) -> DatasetSchemaDownload:
    dataset = get_dataset(db, mod_abbreviation, data_type, dataset_type, version)
    # Return agrkb ids or entity curies based on the dataset type
    dataset_entry: DatasetEntryModel
    if dataset_type == "document":
        data_training = {dataset_entry.reference.curie: 1 if dataset_entry.positive else 0
                         for dataset_entry in dataset.dataset_entries if dataset_entry.set_type == "training"}
        data_testing = {dataset_entry.reference.curie: 1 if dataset_entry.positive else 0
                        for dataset_entry in dataset.dataset_entries if dataset_entry.set_type == "testing"}
    elif dataset_type == "entity":
        data_training = defaultdict(list)
        data_testing = defaultdict(list)
        for dataset_entry in dataset.dataset_entries:
            if dataset_entry.set_type == "training":
                data_training[dataset_entry.reference.curie].append(
                    dataset_entry.entity)
            else:
                data_testing[dataset_entry.reference.curie].append(
                    dataset_entry.entity)
    else:
        raise ValueError("Invalid dataset type")
    return DatasetSchemaDownload(
        dataset_id=dataset.dataset_id,
        data_training=data_training,
        data_testing=data_testing,
        mod_abbreviation=dataset.mod.abbreviation,
        data_type=dataset.data_type,
        dataset_type=dataset.dataset_type,
        description=dataset.description,
        date_created=dataset.date_created,
        date_updated=dataset.date_updated,
        created_by=dataset.created_by,
        updated_by=dataset.updated_by
    )


def check_either_tet_or_workflow_tag_id_provided(topic_entity_tag_id, workflow_tag_id):
    if topic_entity_tag_id is not None and workflow_tag_id is not None:
        raise HTTPException(status_code=400,
                            detail="Exactly one of topic_entity_tag_id or workflow_tag_id must be provided")


def add_entry_to_dataset(db: Session, mod_abbreviation: str, data_type: str, dataset_type: str,
                         version: int, reference_id: int, entity: str = None,
                         supporting_topic_entity_tag_id: int = None, supporting_workflow_tag_id: int = None,
                         set_type: str = "training"):
    check_either_tet_or_workflow_tag_id_provided(supporting_topic_entity_tag_id, supporting_workflow_tag_id)
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                          dataset_type=dataset_type, version=version)
    if dataset.frozen:
        raise HTTPException(status_code=403, detail="Dataset is frozen")
    new_dataset_entry = DatasetEntryModel(
        dataset_id=dataset.dataset_id,
        supporting_topic_entity_tag_id=supporting_topic_entity_tag_id,
        supporting_workflow_tag_id=supporting_workflow_tag_id,
        reference_id=reference_id,
        entity=entity,
        set_type=set_type
    )
    db.add(new_dataset_entry)
    db.commit()
    db.refresh(dataset)


def delete_entry_from_dataset(db: Session, mod_abbreviation: str, data_type: str, dataset_type: str, version: int,
                              reference_id: int, entity: str = None):
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                          dataset_type=dataset_type, version=version)
    if dataset.frozen:
        raise HTTPException(status_code=403, detail="Dataset is frozen")
    dataset_entry = db.query(DatasetEntryModel).filter(
        DatasetEntryModel.dataset_id == dataset.dataset_id,
        DatasetEntryModel.reference_id == reference_id,
        DatasetEntryModel.entity == entity
    ).first()
    if dataset_entry is None:
        raise HTTPException(status_code=404, detail="Dataset-Topic Entity Tag association not found")
    db.delete(dataset_entry)
    db.commit()


def patch_dataset(db: Session, mod_abbreviation: str, data_type: str, dataset_type: str, version: int,
                  dataset_update: DatasetSchemaUpdate):
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                          dataset_type=dataset_type, version=version)
    for key, value in dataset_update.dict(exclude_unset=True).items():
        setattr(dataset, key, value)
    db.commit()
    db.refresh(dataset)


def show_dataset(db, mod_abbreviation, data_type, dataset_type, version):
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type=data_type,
                          dataset_type=dataset_type, version=version)
    return DatasetSchemaShow(
        dataset_id=dataset.dataset_id,
        mod_abbreviation=dataset.mod.abbreviation,
        data_type=dataset.data_type,
        dataset_type=dataset.dataset_type,
        version=dataset.version,
        title=dataset.title,
        description=dataset.description,
        created_by=dataset.created_by,
        updated_by=dataset.updated_by,
        date_created=str(dataset.date_created),
        date_updated=str(dataset.date_updated)
    )