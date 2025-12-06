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

    # New bool column "primary"
    #
    # - nullable=True so it can be NULL when person_id is NULL
    # - I’m *not* setting a server_default here because that would
    #   conflict with the CHECK constraint (see note below).
    #
    # we can still treat "primary" as defaulting to True
    # when creating person-related emails.
    primary = Column("primary", Boolean, nullable=True)

    reference_emails = relationship("ReferenceEmailModel", back_populates="email")

    date_invalidated = Column(DateTime, nullable=True)

    __table_args__ = (
        # one email address per person
        UniqueConstraint(
            "person_id",
            "email_address",
            name="uq_email_person_address",
        ),

        # Constraint: person_id and primary must both be NULL or both non-NULL
        #
        #    (person_id IS NULL AND primary IS NULL)
        # OR (person_id IS NOT NULL AND primary IS NOT NULL)
        CheckConstraint(
            '((person_id IS NULL AND "primary" IS NULL) OR '
            '(person_id IS NOT NULL AND "primary" IS NOT NULL))',
            name="ck_email_person_primary_nulls_together",
        ),

        # “At most one primary per person”:
        #    partial unique index on (person_id) where primary = TRUE.
        #    This is doable in Postgres.
        Index(
            "ux_email_person_primary_true",
            "person_id",
            unique=True,
            postgresql_where=text('"primary" = TRUE'),
        ),

        # Index to support lookups by email address
        Index("ix_email_address", "email_address"),

        # Helpful composite index: queries like
        #   WHERE person_id = ? AND primary = TRUE
        Index("ix_email_person_primary", "person_id", "primary"),

    )

    def __str__(self) -> str:
        status = "invalid" if self.date_invalidated else "active"
        return f"{self.email_address} [{status}]"
