from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String, and_, CheckConstraint, UniqueConstraint, Boolean, or_, Table
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


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
