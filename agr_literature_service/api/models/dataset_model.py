from typing import Dict, Optional

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint, Boolean, Enum
from sqlalchemy.orm import relationship, mapped_column, Mapped

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models import TopicEntityTagModel
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


# Association table for many-to-many relationship between Dataset and TopicEntityTag
class DatasetEntryModel(Base):
    __tablename__ = 'dataset_entry'
    __versioned__: Dict = {}

    dataset_entry_id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )

    dataset_id: Mapped[int] = mapped_column(
        ForeignKey('dataset.dataset_id', ondelete='CASCADE')
    )

    dataset: Mapped["DatasetModel"] = relationship(back_populates="dataset_entries")

    supporting_topic_entity_tag_id: Mapped[int] = mapped_column(
        ForeignKey('topic_entity_tag.topic_entity_tag_id', ondelete="SET NULL"),
        nullable=True
    )

    supporting_topic_entity_tag: Mapped["TopicEntityTagModel"] = relationship(back_populates="dataset_entries")

    supporting_workflow_tag_id: Mapped[int] = mapped_column(
        ForeignKey('workflow_tag.reference_workflow_tag_id', ondelete="SET NULL"),
        nullable=True
    )

    supporting_workflow_tag: Mapped["WorkflowTagModel"] = relationship(back_populates="dataset_entries")

    reference_curie = Column(
        String(),
        nullable=False
    )

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

    classification_value: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True
    )

    set_type: Mapped[str] = mapped_column(
        Enum('training', 'testing', name='set_type_enum'),
        nullable=False,
        default='training',
        server_default='training'
    )

    __table_args__ = (
        UniqueConstraint('dataset_id', 'reference_curie', 'entity', name='unique_dataset_entry'),
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

    frozen: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False
    )

    dataset_entries = relationship(
        "DatasetEntryModel",
        back_populates="dataset",
        cascade="all, delete-orphan",
        passive_deletes=True
    )

    __table_args__ = (
        UniqueConstraint('mod_id', 'data_type', 'dataset_type', 'version', name='unique_dataset'),
    )
