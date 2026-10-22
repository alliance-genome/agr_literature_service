from typing import Optional

# import sqlalchemy as sa
from sqlalchemy import BigInteger, CheckConstraint, Column, ForeignKey, Integer, String, Float, UniqueConstraint
from sqlalchemy.orm import relationship, Mapped, mapped_column

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models import ReferenceModel, ModModel
from agr_literature_service.api.models.ontology_term_model import ontology_curie, register_curie_columns


class ManualIndexingTagModel(Base, AuditedModel):
    """
    Stores curator validation for workflow confidence tags, per (reference, mod).
    """
    __tablename__ = "manual_indexing_tag"

    __table_args__ = (
        # guard against duplicate (mod, reference, tag) rows
        UniqueConstraint(
            "mod_id",
            "reference_id",
            "curation_tag_id",
            name="uq_mod_ref_tag",
        ),
        # confidence_score in [0, 1]
        CheckConstraint(
            "(confidence_score IS NULL) OR (confidence_score >= 0.0 AND confidence_score <= 1.0)",
            name="ck_confval_confidence_range",
        ),
        # OPTIONAL: restrict validation values; remove if you want it fully free-form
        CheckConstraint(
            "(validation_by_biocurator IS NULL "
            " OR validation_by_biocurator IN ('right','wrong'))",
            name="ck_manual_indexing_tag_validation",
        ),
    )

    # PK
    manual_indexing_tag_id: Mapped[int] = mapped_column(
        'id',
        Integer, primary_key=True, autoincrement=True
    )

    # Foreign keys
    reference_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("reference.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    mod_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("mod.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    # Relationships
    reference: Mapped["ReferenceModel"] = relationship(
        "ReferenceModel",
        foreign_keys=[reference_id],
    )
    mod: Mapped["ModModel"] = relationship(
        "ModModel",
        foreign_keys=[mod_id],
    )

    # Fields per acceptance criteria
    # Controlled note: an ATP term (SCRUM-6720: the ontologyterm id; curation_tag is its curie)
    curation_tag_id = Column(BigInteger, ForeignKey("ontologyterm.id"), index=True, nullable=False)
    curation_tag = ontology_curie(curation_tag_id)

    confidence_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True
    )

    # Use String instead of Enum; constrained via CheckConstraint above
    validation_by_biocurator: Mapped[Optional[str]] = mapped_column(
        String(32), nullable=True
    )

    note: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )

    def __str__(self) -> str:
        return (
            f"curation_tag:{self.curation_tag} "
            f"mod_id:{self.mod_id} "
            f"ref_id:{self.reference_id} "
        )


register_curie_columns(ManualIndexingTagModel, {"curation_tag": ("curation_tag_id", ("ATPTerm",))})
