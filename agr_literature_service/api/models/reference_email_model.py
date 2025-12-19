from typing import Dict

from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class ReferenceEmailModel(Base, AuditedModel):
    __tablename__ = "reference_email"
    __versioned__: Dict = {}

    reference_email_id = Column(Integer, primary_key=True, autoincrement=True)

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    email_id = Column(
        Integer,
        ForeignKey("email.email_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Relationships (you can wire these up on the other side if/when you like)
    reference = relationship("ReferenceModel", back_populates="reference_emails")
    email = relationship("EmailModel", back_populates="reference_emails")

    __table_args__ = (
        # Ensure each (reference, email) pair appears at most once
        UniqueConstraint(
            "reference_id",
            "email_id",
            name="uq_reference_email_reference_email",
        ),
        # Helpful composite index for joins / lookups in either direction
        Index(
            "ix_reference_email_reference_email",
            "reference_id",
            "email_id",
        ),
    )

    def __str__(self) -> str:
        return f"ReferenceEmail(reference_id={self.reference_id}, email_id={self.email_id})"
