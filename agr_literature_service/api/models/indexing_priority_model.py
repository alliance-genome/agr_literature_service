from typing import Optional
from sqlalchemy import UniqueConstraint, CheckConstraint, ForeignKey
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy import Integer, String, Float
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models import ReferenceModel, ModModel


class IndexingPriorityModel(Base, AuditedModel):
    __tablename__ = "indexing_priority"

    __table_args__ = (
        UniqueConstraint(
            "mod_id",
            "reference_id",
            "indexing_priority",
            name="uq_indexing_priority_mod_ref_priority",  # renamed for clarity
        ),
        CheckConstraint(
            "indexing_priority LIKE 'ATP:%'",
            name="ck_indexing_priority_prefix",
        ),
        CheckConstraint(
            "(confidence_score IS NULL) OR (confidence_score >= 0.0 AND confidence_score <= 1.0)",
            name="ck_indexing_priority_confidence_range",
        ),
    )

    indexing_priority_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    indexing_priority: Mapped[str] = mapped_column(
        String, index=True, nullable=False
    )

    reference_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True, nullable=False
    )
    reference: Mapped["ReferenceModel"] = relationship(
        "ReferenceModel",
        foreign_keys="IndexingPriorityModel.reference_id",
    )

    mod_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("mod.mod_id", ondelete="CASCADE"),
        index=True, nullable=False
    )
    mod: Mapped["ModModel"] = relationship(
        "ModModel",
        foreign_keys="IndexingPriorityModel.mod_id",
    )

    confidence_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )

    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("topic_entity_tag_source.topic_entity_tag_source_id", ondelete="CASCADE"),
        index=True, nullable=False
    )

    validation_by_biocurator: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )

    def __str__(self) -> str:
        return f"priority:{self.indexing_priority} mod_id:{self.mod_id} ref_id:{self.reference_id}"
