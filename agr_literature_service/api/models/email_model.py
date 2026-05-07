from typing import Dict
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Index,
    UniqueConstraint,
    Boolean,
    CheckConstraint,
    text,
)
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class EmailModel(Base, AuditedModel):
    __tablename__ = "email"
    __versioned__: Dict = {}

    email_id = Column(Integer, primary_key=True, autoincrement=True)

    # Allow person_id to be NULL
    person_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    person = relationship("PersonModel", back_populates="emails")

    email_address = Column(String(), nullable=False)

    # nullable=True so it can be NULL when person_id is NULL.
    # No server_default — that would conflict with the CHECK constraint.
    # Application code treats is_primary as defaulting to True when
    # creating person-related emails.
    is_primary = Column(Boolean, nullable=True)

    reference_emails = relationship("ReferenceEmailModel", back_populates="email")

    date_invalidated = Column(DateTime, nullable=True)

    __table_args__ = (
        # one email address per person
        UniqueConstraint(
            "person_id",
            "email_address",
            name="uq_email_person_address",
        ),

        # person_id and is_primary must both be NULL or both non-NULL.
        CheckConstraint(
            "((person_id IS NULL AND is_primary IS NULL) OR "
            "(person_id IS NOT NULL AND is_primary IS NOT NULL))",
            name="ck_email_person_primary_nulls_together",
        ),

        # At most one primary email per person.
        Index(
            "ux_email_person_primary_true",
            "person_id",
            unique=True,
            postgresql_where=text("is_primary = TRUE"),
        ),

        # Index to support lookups by email address
        Index("ix_email_address", "email_address"),

        # Functional index supporting func.lower(email_address) == :norm
        # lookups (see crud/person_crud.py:310, crud/user_utils.py:22).
        Index("ix_email_lower_email_address", text("lower(email_address)")),

        # Helpful composite index for queries like:
        #   WHERE person_id = ? AND is_primary = TRUE
        Index("ix_email_person_primary", "person_id", "is_primary"),

    )

    def __str__(self) -> str:
        status = "invalid" if self.date_invalidated else "active"
        return f"{self.email_address} [{status}]"
