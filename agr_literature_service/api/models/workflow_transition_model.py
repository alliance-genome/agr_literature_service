"""
workflow_transition_model.py

See docs/source/workflow_automation.rst for detailed description.
==================
"""

from typing import Dict

from sqlalchemy import (BigInteger, Column, ForeignKey, String, ARRAY)
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models.ontology_term_model import ontology_curie, register_curie_columns

enable_versioning()


class WorkflowTransitionModel(AuditedModel, Base):
    __tablename__ = "workflow_transition"
    __versioned__: Dict = {}

    workflow_transition_id = Column(
        'id',
        BigInteger,
        primary_key=True,
        autoincrement=True
    )

    # mod id
    mod_id = Column(
        BigInteger,
        ForeignKey("mod.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship(
        "ModModel",
        foreign_keys="WorkflowTransitionModel.mod_id"
    )

    # workflow transitions from / to these workflow tags: ATP ontologyterm ids (SCRUM-6720);
    # transition_from and transition_to are their curies
    transition_from_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=False)
    transition_from = ontology_curie(transition_from_id)

    transition_to_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=False)
    transition_to = ontology_curie(transition_to_id)

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


register_curie_columns(WorkflowTransitionModel, {
    "transition_from": ("transition_from_id", ("ATPTerm",)),
    "transition_to": ("transition_to_id", ("ATPTerm",)),
})
