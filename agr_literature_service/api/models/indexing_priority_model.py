from typing import Optional
from sqlalchemy import BigInteger, Column, UniqueConstraint, CheckConstraint, ForeignKey
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy import Float
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models import ReferenceModel, ModModel
from agr_literature_service.api.models.ontology_term_model import ontology_curie, register_curie_columns


class IndexingPriorityModel(Base, AuditedModel):
    __tablename__ = "indexing_priority"

    __table_args__ = (
        UniqueConstraint(
            "mod_id",
            "reference_id",
            name="uq_indexing_priority_mod_ref",
        ),
        CheckConstraint(
            "(confidence_score IS NULL) OR (confidence_score >= 0.0 AND confidence_score <= 1.0)",
            name="ck_indexing_priority_confidence_range",
        ),
        CheckConstraint(
            "predicted_indexing_priority_id IS NOT NULL OR curator_indexing_priority_id IS NOT NULL",
            name="ck_at_least_one_priority",
        ),
    )

    indexing_priority_id: Mapped[int] = mapped_column(
        'id',
        BigInteger, primary_key=True, autoincrement=True
    )

    # ATP terms (SCRUM-6720: ontologyterm ids; the *_indexing_priority attributes are their curies)
    predicted_indexing_priority_id = Column(BigInteger, ForeignKey("ontologyterm.id"), index=True, nullable=True)
    predicted_indexing_priority = ontology_curie(predicted_indexing_priority_id)

    reference_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("reference.id", ondelete="CASCADE"),
        index=True, nullable=False
    )
    reference: Mapped["ReferenceModel"] = relationship(
        "ReferenceModel",
        foreign_keys="IndexingPriorityModel.reference_id",
    )

    mod_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("mod.id", ondelete="CASCADE"),
        index=True, nullable=False
    )
    mod: Mapped["ModModel"] = relationship(
        "ModModel",
        foreign_keys="IndexingPriorityModel.mod_id",
    )

    confidence_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )

    curator_indexing_priority_id = Column(BigInteger, ForeignKey("ontologyterm.id"), nullable=True)
    curator_indexing_priority = ontology_curie(curator_indexing_priority_id)

    def __str__(self) -> str:
        return (
            f"predicted_indexing_priority:{self.predicted_indexing_priority} "
            f"curator_indexing_priority:{self.curator_indexing_priority} "
            f"mod_id:{self.mod_id} ref_id:{self.reference_id}"
        )


register_curie_columns(IndexingPriorityModel, {
    "predicted_indexing_priority": ("predicted_indexing_priority_id", ("ATPTerm",)),
    "curator_indexing_priority": ("curator_indexing_priority_id", ("ATPTerm",)),
})
