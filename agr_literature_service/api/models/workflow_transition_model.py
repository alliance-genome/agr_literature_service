"""
workflow_transition_model.py

See docs/source/workflow_automation.rst for detailed description.
==================
"""

from typing import Dict

from sqlalchemy import (Column, ForeignKey, Integer,
                        String, ARRAY)
from sqlalchemy.orm import relationship

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

    mod = relationship(
        "ModModel",
        foreign_keys="WorkflowTransitionModel.mod_id"
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

    # Correct type annotations
    requirements: Column = Column(ARRAY(String), unique=False, nullable=True)

    transition_type = Column(
        String(),
        unique=False,
        nullable=False,
        default='any',
        server_default='any'
    )

    actions: Column = Column(ARRAY(String()), unique=False, nullable=True)

    condition = Column(
        String(),
        unique=False,
        nullable=True
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return f"mod: {self.mod_id}, from: {self.transition_from}  to: {self.transition_to}\n\t"\
            f"actions: {self.actions}, condition: {self.condition}, req: {self.requirements}"
