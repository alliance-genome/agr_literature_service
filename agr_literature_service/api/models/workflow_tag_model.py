"""
workflow_tag_model.py
==================
"""

from typing import Dict, List
from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    CheckConstraint,
)
from sqlalchemy.orm import relationship, Mapped

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class WorkflowTagModel(AuditedModel, Base):
    __tablename__ = "workflow_tag"
    __versioned__: Dict = {}

    # enforce uniqueness on mod_id + reference_id + workflow_tag_id
    __table_args__ = (
        UniqueConstraint(
            'mod_id',
            'reference_id',
            'workflow_tag_id',
            name='uq_workflow_tag_mod_ref_tag'
        ),
        # make sure workflow_tag_id always starts with 'ATP:'
        CheckConstraint(
            "workflow_tag_id LIKE 'ATP:%'",
            name='ck_workflow_tag_id_prefix'
        ),
    )

    reference_workflow_tag_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    reference = relationship(
        "ReferenceModel",
        foreign_keys="WorkflowTagModel.reference_id",
        back_populates="workflow_tag"
    )

    workflow_tag_id = Column(
        String(),
        unique=False,
        nullable=False,
        index=True
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id"),
        index=True,
        nullable=True
    )

    mod = relationship(
        "ModModel",
        foreign_keys="WorkflowTagModel.mod_id"
    )

    curation_tag = Column(
        String,
        nullable=True
    )

    note = Column(
        String,
        nullable=True
    )

    dataset_entries: Mapped[List["DatasetEntryModel"]] = relationship(back_populates="supporting_workflow_tag")  # type: ignore  # noqa

    def __str__(self):
        """
        Overwrite the default output.
        """
        return (
            f"ID: {self.reference_workflow_tag_id} "
            f"mod: {self.mod.abbreviation}, ref: {self.reference_id}, "
            f"workflow_tag: {self.workflow_tag_id} date_created: {self.date_created}"
        )
