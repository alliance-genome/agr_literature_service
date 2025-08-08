from typing import Dict
from sqlalchemy import UniqueConstraint, CheckConstraint, Column, ForeignKey, Integer, String, Float
from sqlalchemy.orm import relationship, mapped_column, Mapped
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class IndexingPriorityTagModel(Base, AuditedModel):
    __tablename__ = 'indexing_priority'
    __versioned__: Dict = {}

    # enforce uniqueness on mod_id + reference_id + indexing_priority
    __table_args__ = (
        UniqueConstraint(
            'mod_id',
            'reference_id',
            'indexing_priority',
            name='uq_indexing_priority_mod_ref_tag'
        ),
        # make sure indexing_priority always starts with 'ATP:'
        CheckConstraint(
            "indexing_priority LIKE 'ATP:%'",
            name='ck_indexing_priority_prefix'
        ),
    )

    indexing_priority_id: Mapped[int] = mapped_column(
        primary_key=True,
        autoincrement=True
    )

    indexing_priority = Column(
        String,
        index=True,
        nullable=False
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        foreign_keys="CurationTagModel.reference_id"
    )

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    mod = relationship(
        "ModModel",
        foreign_keys="CurationTagModel.mod_id"
    )

    confidence_score = Column(
        Float(),
        nullable=True,
        unique=False
    )

    source_id = Column(
        Integer,
        ForeignKey("topic_entity_tag_source.topic_entity_tag_source_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    validation_by_biocurator = Column(
        String(),
        nullable=True,
        unique=False
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return f"tag:{self.curation_tag_type} {self.curation_tag} mod:{self.mod} ref:{self.reference}"
