from typing import Optional

# import sqlalchemy as sa
from sqlalchemy import CheckConstraint, ForeignKey, Integer, String, Float, UniqueConstraint
from sqlalchemy.orm import relationship, Mapped, mapped_column

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models import ReferenceModel, ModModel


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
            "curation_tag",
            name="uq_mod_ref_tag",
        ),
        # ATP id format (curation_tag)
        CheckConstraint(
            "curation_tag LIKE 'ATP:%'",
            name="ck_confval_curation_tag_prefix",
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
        Integer, primary_key=True, autoincrement=True
    )

    # Foreign keys
    reference_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    mod_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
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
    # Controlled note (ATP IDs)
    curation_tag: Mapped[Optional[str]] = mapped_column(
        String(64), index=True, nullable=False
    )

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
