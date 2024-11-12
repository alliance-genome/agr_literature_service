from typing import Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ModModel
from agr_literature_service.api.models.dataset_model import DatasetModel
from agr_literature_service.api.models.topic_entity_tag_model import TopicEntityTagModel
from agr_literature_service.api.schemas.dataset_schema import DatasetSchemaShow, DatasetSchemaPost, \
    DatasetSchemaDownload, DatasetSchemaUpdate


def get_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str) -> Optional[DatasetModel]:
    dataset = db.query(DatasetModel).join(DatasetModel.mod).filter(
        DatasetModel.mod.has(abbreviation=mod_abbreviation),
        DatasetModel.data_type_topic == data_type_topic,
        DatasetModel.dataset_type == dataset_type
    ).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return dataset


def create_dataset(db: Session, dataset: DatasetSchemaPost) -> str:
    mod = db.query(ModModel).filter(ModModel.abbreviation == dataset.mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=404, detail=f"Mod with abbreviation {dataset.mod_abbreviation} not found")
    db_dataset = DatasetModel(
        mod_id=mod.mod_id,
        data_type_topic=dataset.data_type_topic,
        dataset_type=dataset.dataset_type,
        notes=dataset.notes
    )
    db.add(db_dataset)
    db.commit()
    db.refresh(db_dataset)
    return "created"


def delete_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str):
    dataset = get_dataset(db, mod_abbreviation, data_type_topic, dataset_type)
    db.delete(dataset)
    db.commit()


def download_dataset(db: Session, mod_abbreviation: str, data_type_topic: str,
                     dataset_type: str) -> DatasetSchemaDownload:
    dataset = get_dataset(db, mod_abbreviation, data_type_topic, dataset_type)
    # Return agrkb ids or entity curies based on the dataset type
    if dataset_type == "document":
        data = [tag.reference.curie for tag in dataset.topic_entity_tags]
    elif dataset_type == "entity":
        data = [tag.entity for tag in dataset.topic_entity_tags]
    else:
        raise ValueError("Invalid dataset type")
    return DatasetSchemaDownload(
        dataset_id=dataset.dataset_id,
        data=data,
        mod_abbreviation=dataset.mod.abbreviation,
        data_type_topic=dataset.data_type_topic,
        dataset_type=dataset.dataset_type,
        notes=dataset.notes,
        date_created=dataset.date_created,
        date_updated=dataset.date_updated,
        created_by=dataset.created_by,
        updated_by=dataset.updated_by
    )


def add_topic_entity_tag_to_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str,
                                    topic_entity_tag_id: int):
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                          dataset_type=dataset_type)
    topic_entity_tag = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).first()
    if not topic_entity_tag:
        raise HTTPException(status_code=404, detail="Tag not found")
    dataset.topic_entity_tags.append(topic_entity_tag)
    db.commit()
    db.refresh(dataset)


def delete_topic_entity_tag_from_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str,
                                         topic_entity_tag_id: int):
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                          dataset_type=dataset_type)
    
    topic_entity_tag = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id
    ).first()
    
    if topic_entity_tag is None:
        raise HTTPException(status_code=404, detail="Topic Entity Tag not found")
    
    dataset.topic_entity_tags.remove(topic_entity_tag)
    db.commit()


def destroy_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str):
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                          dataset_type=dataset_type)
    db.delete(dataset)
    db.commit()


def patch_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str,
                  dataset_update: DatasetSchemaUpdate):
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                          dataset_type=dataset_type)
    for key, value in dataset_update.dict(exclude_unset=True).items():
        setattr(dataset, key, value)
    db.commit()
    db.refresh(dataset)
