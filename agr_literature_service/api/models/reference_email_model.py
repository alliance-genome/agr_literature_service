from typing import Dict

from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Index,
    text,
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

    email_address = Column(String(), nullable=False)

    reference = relationship("ReferenceModel", back_populates="reference_emails")

    __table_args__ = (
        Index(
            "uq_reference_email_reference_email_lower",
            "reference_id",
            text("lower(email_address)"),
            unique=True,
        ),
        Index(
            "ix_reference_email_reference_email",
            "reference_id",
            "email_address",
        ),
    )

    def __str__(self) -> str:
        return (
            f"ReferenceEmail(reference_id={self.reference_id}, "
            f"email_address={self.email_address})"
        )
