"""
workflow_transition_model.py
==================
"""

from typing import Dict

from sqlalchemy import (Column, ForeignKey, Integer,
                        String)

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class WorkflowTransitionModel(AuditedModel, Base):
    __tablename__ = "workflow_transition"
    __versioned__: Dict = {}

    workflow_transition_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

# mod id
    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

# workflow transitions from this workflow_tag, String from A-Team api.
    transition_from = Column(
        String(),
        unique=False,
        nullable=False
    )

    # workflow transitions to this workflow_tag, String from A-Team api.
    transition_to = Column(
        String(),
        unique=False,
        nullable=False
    )
