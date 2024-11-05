from sqlalchemy.orm import Session
from typing import List, Optional

from agr_literature_service.api.models.dataset_model import DatasetModel
from agr_literature_service.api.models.topic_entity_tag_model import TopicEntityTagModel
from agr_literature_service.api.schemas.dataset_schema import DatasetCreate, DatasetUpdate


def create_blank_dataset(db: Session, dataset: DatasetCreate) -> DatasetModel:
    db_dataset = DatasetModel(
        mod_id=dataset.mod_id,
        data_type_topic=dataset.data_type_topic,
        dataset_type=dataset.dataset_type,
        notes=dataset.notes
    )
    db.add(db_dataset)
    db.commit()
    db.refresh(db_dataset)
    return db_dataset


def get_dataset(db: Session, dataset_id: int) -> Optional[DatasetModel]:
    return db.query(DatasetModel).filter(DatasetModel.dataset_id == dataset_id).first()


def get_datasets(db: Session, skip: int = 0, limit: int = 100) -> List[DatasetModel]:
    return db.query(DatasetModel).offset(skip).limit(limit).all()


def add_topic_entity_tag_to_dataset(db: Session, dataset_id: int, topic_entity_tag_id: int) -> Optional[DatasetModel]:
    dataset = get_dataset(db, dataset_id)
    if not dataset:
        return None

    topic_entity_tag = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).first()
    if not topic_entity_tag:
        return None

    dataset.topic_entity_tags.append(topic_entity_tag)
    db.commit()
    db.refresh(dataset)
    return dataset


def remove_topic_entity_tag_from_dataset(db: Session, dataset_id: int, topic_entity_tag_id: int) -> Optional[DatasetModel]:
    dataset = get_dataset(db, dataset_id)
    if not dataset:
        return None

    topic_entity_tag = db.query(TopicEntityTagModel).filter(TopicEntityTagModel.topic_entity_tag_id == topic_entity_tag_id).first()
    if not topic_entity_tag:
        return None

    dataset.topic_entity_tags.remove(topic_entity_tag)
    db.commit()
    db.refresh(dataset)
    return dataset


def destroy_dataset(db: Session, dataset_id: int) -> bool:
    dataset = get_dataset(db, dataset_id)
    if not dataset:
        return False

    db.delete(dataset)
    db.commit()
    return True


def update_dataset(db: Session, dataset_id: int, dataset: DatasetUpdate) -> Optional[DatasetModel]:
    db_dataset = get_dataset(db, dataset_id)
    if db_dataset:
        for key, value in dataset.dict(exclude_unset=True).items():
            setattr(db_dataset, key, value)
        db.commit()
        db.refresh(db_dataset)
    return db_dataset