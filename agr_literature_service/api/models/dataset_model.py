from typing import Dict, List

from sqlalchemy import Column, ForeignKey, Integer, String, Table, UniqueConstraint, Boolean, Enum
from sqlalchemy.orm import relationship, mapped_column, Mapped

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models import TopicEntityTagModel
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


# Association table for many-to-many relationship between Dataset and TopicEntityTag
class DatasetTopicEntityTag(Base):
    __tablename__ = 'dataset_topic_entity_tag'

    dataset_id: Mapped[int] = mapped_column(
        ForeignKey('dataset.dataset_id'),
        primary_key=True,
        ondelete='CASCADE'
    )

    topic_entity_tag_id: Mapped[int] = mapped_column(
        ForeignKey('topic_entity_tag.topic_entity_tag_id'),
        primary_key=True,
        ondelete='CASCADE'
    )

    dataset: Mapped["DatasetModel"] = relationship(back_populates="topic_entity_tags")
    topic_entity_tag: Mapped["TopicEntityTagModel"] = relationship(back_populates="datasets")

    set_type = Column(
        Enum('training', 'testing', name='set_type_enum'),
        nullable=False,
        default='training',
        server_default='training'
    )


class DatasetModel(AuditedModel, Base):
    __tablename__ = "dataset"
    __versioned__: Dict = {}

    dataset_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    title = Column(
        String(),
        unique=False,
        nullable=False
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

    version = Column(
        Integer(),
        nullable=True,
    )

    description = Column(
        String(),
        unique=False,
        nullable=False
    )

    topic_entity_tags: Mapped[List["DatasetTopicEntityTag"]] = relationship(back_populates="dataset")

    __table_args__ = (
        UniqueConstraint('mod_id', 'data_type_topic', 'dataset_type', 'version', name='unique_dataset'),
    )

