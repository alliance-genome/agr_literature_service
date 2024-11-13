from collections import defaultdict
from typing import Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from agr_literature_service.api.models import ModModel
from agr_literature_service.api.models.dataset_model import DatasetModel, DatasetTopicEntityTag
from agr_literature_service.api.models.topic_entity_tag_model import TopicEntityTagModel
from agr_literature_service.api.schemas.dataset_schema import DatasetSchemaShow, DatasetSchemaPost, \
    DatasetSchemaDownload, DatasetSchemaUpdate


def get_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str,
                version: int = None) -> Optional[DatasetModel]:
    dataset = db.query(DatasetModel).join(DatasetModel.mod).filter(
        DatasetModel.mod.has(abbreviation=mod_abbreviation),
        DatasetModel.data_type_topic == data_type_topic,
        DatasetModel.dataset_type == dataset_type,
        DatasetModel.version == version,
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
        title=dataset.title,
        description=dataset.description
    )
    db.add(db_dataset)
    try:
        db.commit()
        db.refresh(db_dataset)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to create dataset. Reason: {str(e)}")
    return "created"


def delete_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str, version: int):
    dataset = get_dataset(db, mod_abbreviation, data_type_topic, dataset_type, version)
    db.delete(dataset)
    db.commit()


def download_dataset(db: Session, mod_abbreviation: str, data_type_topic: str,
                     dataset_type: str) -> DatasetSchemaDownload:
    dataset = get_dataset(db, mod_abbreviation, data_type_topic, dataset_type)
    # Return agrkb ids or entity curies based on the dataset type
    document_data_training = []
    document_data_testing = []
    entity_data_training = []
    entity_data_testing = []
    if dataset_type == "document":
        tag_association: DatasetTopicEntityTag
        document_data_training = [
            {tag_association.topic_entity_tag.reference.curie: 0 if tag_association.topic_entity_tag.negated else 1}
            for tag_association in dataset.topic_entity_tag_associations if tag_association.set_type == "training"
        ]
        document_data_testing = [
            {tag_association.topic_entity_tag.reference.curie: 0 if tag_association.topic_entity_tag.negated else 1}
            for tag_association in dataset.topic_entity_tag_associations if tag_association.set_type == "testing"
        ]
    elif dataset_type == "entity":
        entity_data_training = defaultdict(list)
        entity_data_testing = defaultdict(list)
        for tag_association in dataset.topic_entity_tag_associations:
            if tag_association.set_type == "training":
                entity_data_training[tag_association.topic_entity_tag.reference.curie].append(
                    tag_association.topic_entity_tag.entity)
            else:
                entity_data_testing[tag_association.topic_entity_tag.reference.curie].append(
                    tag_association.topic_entity_tag.entity)
    else:
        raise ValueError("Invalid dataset type")
    return DatasetSchemaDownload(
        dataset_id=dataset.dataset_id,
        document_data_training=document_data_training,
        document_data_testing=document_data_testing,
        entity_data_training=entity_data_training,
        entity_data_testing=entity_data_testing,
        mod_abbreviation=dataset.mod.abbreviation,
        data_type_topic=dataset.data_type_topic,
        dataset_type=dataset.dataset_type,
        description=dataset.description,
        date_created=dataset.date_created,
        date_updated=dataset.date_updated,
        created_by=dataset.created_by,
        updated_by=dataset.updated_by
    )


def add_topic_entity_tag_to_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str,
                                    topic_entity_tag_id: int, set_type: str = "training"):
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                          dataset_type=dataset_type)
    topic_entity_tag = db.query(TopicEntityTagModel).filter(
        TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).first()
    if not topic_entity_tag:
        raise HTTPException(status_code=404, detail="Tag not found")
    new_dataset_tag_association = DatasetTopicEntityTag(
        dataset_id=dataset.dataset_id,
        topic_entity_tag_id=topic_entity_tag.topic_entity_tag_id,
        set_type=set_type
    )
    db.add(new_dataset_tag_association)
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
    dataset_tag_association = db.query(DatasetTopicEntityTag).filter(
        DatasetTopicEntityTag.dataset_id == dataset.dataset_id,
        DatasetTopicEntityTag.topic_entity_tag_id == topic_entity_tag.topic_entity_tag_id).first()
    if dataset_tag_association is None:
        raise HTTPException(status_code=404, detail="Dataset-Topic Entity Tag association not found")
    db.delete(dataset_tag_association)
    db.commit()


def patch_dataset(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str, version: int,
                  dataset_update: DatasetSchemaUpdate):
    dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                          dataset_type=dataset_type, version=version)
    for key, value in dataset_update.dict(exclude_unset=True).items():
        setattr(dataset, key, value)
    db.commit()
    db.refresh(dataset)


def create_version(db: Session, mod_abbreviation: str, data_type_topic: str, dataset_type: str):
    max_version = db.query(DatasetModel.version).filter(
        DatasetModel.mod.abbreviation == mod_abbreviation,
        DatasetModel.data_type_topic == data_type_topic,
        DatasetModel.dataset_type == dataset_type
    ).order_by(DatasetModel.version.desc()).first()
    current_dataset = get_dataset(db, mod_abbreviation=mod_abbreviation, data_type_topic=data_type_topic,
                                  dataset_type=dataset_type)
    new_version = max_version.version + 1 if max_version else 1
    new_dataset = DatasetModel(
        mod_id=current_dataset.mod_id,
        data_type_topic=data_type_topic,
        dataset_type=dataset_type,
        version=new_version,
        title=current_dataset.title,
        description=current_dataset.description
    )
    db.add(new_dataset)
    db.commit()
    db.refresh(new_dataset)
    for dataset_tag_association in current_dataset.topic_entity_tag_associations:
        new_dataset_tag_association = DatasetTopicEntityTag(
            dataset_id=new_dataset.dataset_id,
            topic_entity_tag_id=dataset_tag_association.topic_entity_tag_id,
            set_type=dataset_tag_association.set_type
        )
        db.add(new_dataset_tag_association)
    db.commit()
    return new_version
