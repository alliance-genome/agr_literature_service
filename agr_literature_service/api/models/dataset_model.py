from typing import Dict, List

from sqlalchemy import Column, ForeignKey, Integer, String, Table, UniqueConstraint, Boolean, Enum, func
from sqlalchemy.orm import relationship, mapped_column, Mapped

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models import TopicEntityTagModel, ReferenceModel
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


# Association table for many-to-many relationship between Dataset and TopicEntityTag
class DatasetEntry(Base):
    __tablename__ = 'dataset_entry'

    dataset_id: Mapped[int] = mapped_column(
        ForeignKey('dataset.dataset_id'),
        primary_key=True,
        ondelete='CASCADE'
    )

    dataset: Mapped["DatasetModel"] = relationship(back_populates="dataset_entries")

    supporting_topic_entity_tag_id: Mapped[int] = mapped_column(
        ForeignKey('topic_entity_tag.topic_entity_tag_id'),
        ondelete="SET NULL"
    )

    supporting_topic_entity_tag: Mapped["TopicEntityTagModel"] = relationship(back_populates="dataset_entries")

    supporting_workflow_tag_id: Mapped[int] = mapped_column(
        ForeignKey('workflow_tag.reference_workflow_tag_id'),
        ondelete="SET NULL"
    )

    supporting_workflow_tag: Mapped["WorkflowTagModel"] = relationship(back_populates="dataset_entries")

    reference_id: Mapped[int] = mapped_column(
        ForeignKey('reference.reference_id')
    )

    # TODO: update reference ids when merging references

    reference: Mapped["ReferenceModel"] = relationship()

    entity = Column(
        String(),
        nullable=True,
        default=None
    )

    entity_count = Column(
        Integer(),
        nullable=True,
        default=None
    )

    sentence = Column(
        String(),
        nullable=True,
        default=None
    )

    section = Column(
        String(),
        nullable=True,
        default=None
    )

    positive = Column(
        Boolean(),
        nullable=False,
        default=True,
        server_default='true'
    )

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

    data_type = Column(
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
        nullable=False,
    )

    description = Column(
        String(),
        unique=False,
        nullable=False
    )

    frozen = Column(
        Boolean(),
        nullable=False,
        default=False
    )

    production = Column(
        Boolean(),
        nullable=False,
        default=False
    )

    dataset_entries: Mapped[List["DatasetEntry"]] = relationship()

    __table_args__ = (
        UniqueConstraint('mod_id', 'data_type_topic', 'dataset_type', 'version', name='unique_dataset')
    )

