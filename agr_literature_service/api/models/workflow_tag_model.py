"""
workflow_tag_model.py
==================
"""

from typing import Dict, List

from sqlalchemy import (Column, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import relationship, Mapped

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class WorkflowTagModel(AuditedModel, Base):
    __tablename__ = "workflow_tag"
    __versioned__: Dict = {}

    reference_workflow_tag_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

# reference id - internal reference id
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

# workflow_tag node term-id - string from A api.
    workflow_tag_id = Column(
        String(),
        unique=False,
        nullable=False
    )

# mod - from mod table (null means all).  Curators will be explicit and know that null means all, would never want to be vague and separate vague null from explicit all.
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

    dataset_entries: Mapped[List["DatasetEntryModel"]] = relationship(back_populates="supporting_workflow_tag")  # noqa

    def __str__(self):
        """
        Overwrite the default output.
        """
        return f"ID: {self.reference_workflow_tag_id} "\
            f"mod: {self.mod.abbreviation}, ref: {self.reference_id}, workflow_tag: {self.workflow_tag_id}"
