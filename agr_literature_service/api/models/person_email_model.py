from typing import Dict
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Index,
    text,
)
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonEmailModel(Base, AuditedModel):
    __tablename__ = "person_email"
    __versioned__: Dict = {}

    person_email_id = Column(Integer, primary_key=True, autoincrement=True)

    person_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    person = relationship("PersonModel", back_populates="emails")

    email_address = Column(String(), nullable=False)

    date_made_old_email = Column(DateTime, nullable=True)

    __table_args__ = (
        Index(
            "uq_person_email_person_address_lower",
            "person_id",
            text("lower(email_address)"),
            unique=True,
        ),
        Index("ix_person_email_email_address", "email_address"),
        Index(
            "ix_person_email_lower_email_address",
            text("lower(email_address)"),
        ),
        Index(
            "ix_person_email_active_by_person",
            "person_id",
            postgresql_where=text("date_made_old_email IS NULL"),
        ),
    )

    @property
    def person_curie(self):
        """Convenience for serializers — the owning person's curie."""
        return self.person.curie if self.person else None

    def __str__(self) -> str:
        status = "old" if self.date_made_old_email else "active"
        return f"{self.email_address} [{status}]"
