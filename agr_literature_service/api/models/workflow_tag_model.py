"""
workflow_tag_model.py
==================
"""

from typing import Dict, List
from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship, Mapped

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models.ontology_term_model import ontology_curie, register_curie_columns

enable_versioning()


class WorkflowTagModel(AuditedModel, Base):
    __tablename__ = "workflow_tag"
    __versioned__: Dict = {}

    # enforce uniqueness on mod_id + reference_id + workflow tag (an ATP term, SCRUM-6720)
    __table_args__ = (
        UniqueConstraint(
            'mod_id',
            'reference_id',
            'workflow_tag_term_id',
            name='uq_workflow_tag_mod_ref_tag'
        ),
    )

    reference_workflow_tag_id = Column(
        'id',
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    reference = relationship(
        "ReferenceModel",
        foreign_keys="WorkflowTagModel.reference_id",
        back_populates="workflow_tag"
    )

    # SCRUM-6720: the workflow tag is an ATP ontologyterm id, stored as workflow_tag_term_id (a new
    # name, so raw SQL still naming workflow_tag_id fails loudly); workflow_tag_id is its curie
    workflow_tag_term_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=False, index=True)
    workflow_tag_id = ontology_curie(workflow_tag_term_id)

    mod_id = Column(
        Integer,
        ForeignKey("mod.id"),
        index=True,
        nullable=True
    )

    mod = relationship(
        "ModModel",
        foreign_keys="WorkflowTagModel.mod_id"
    )

    curation_tag_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=True)
    curation_tag = ontology_curie(curation_tag_id)

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


register_curie_columns(WorkflowTagModel, {
    "workflow_tag_id": ("workflow_tag_term_id", ("ATPTerm",)),
    "curation_tag": ("curation_tag_id", ("ATPTerm",)),
})
