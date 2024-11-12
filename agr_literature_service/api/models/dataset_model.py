from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String, Table, UniqueConstraint
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()

# Association table for many-to-many relationship between Dataset and TopicEntityTag
dataset_topic_entity_tag = Table(
    'dataset_topic_entity_tag',
    Base.metadata,
    
    Column(
        'dataset_id', 
        Integer, 
        ForeignKey('dataset.dataset_id')
    ),
    
    Column(
        'topic_entity_tag_id', 
        Integer, 
        ForeignKey('topic_entity_tag.topic_entity_tag_id')
    )
)


class DatasetModel(AuditedModel, Base):
    __tablename__ = "dataset"
    __versioned__: Dict = {}

    dataset_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship(
        "ModModel",
        foreign_keys="DatasetModel.mod_id"
    )

    data_type_topic = Column(
        String(),
        unique=False,
        nullable=False
    )

    dataset_type = Column(
        String(),
        unique=False,
        nullable=False
    )

    notes = Column(
        String(),
        unique=False,
        nullable=False
    )

    # Add relationship to TopicEntityTag
    topic_entity_tags = relationship(
        "TopicEntityTagModel",
        secondary=dataset_topic_entity_tag,
        back_populates="datasets"
    )

    __table_args__ = (
        UniqueConstraint('mod_id', 'data_type_topic', 'dataset_type', name='unique_dataset'),
    )

