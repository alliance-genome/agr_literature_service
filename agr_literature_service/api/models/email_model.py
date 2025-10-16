from typing import Dict
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class EmailModel(Base, AuditedModel):
    __tablename__ = "email"
    __versioned__: Dict = {}

    email_id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, ForeignKey("person.person_id", ondelete="CASCADE"), nullable=False, index=True)
    person = relationship("PersonModel", back_populates="emails")

    email_address = Column(String(), nullable=False)
    date_invalidated = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("person_id", "email_address", name="uq_email_person_address"),
        Index("ix_email_address", "email_address"),
    )

    def __str__(self) -> str:
        status = "invalid" if self.date_invalidated else "active"
        return f"{self.email_address} [{status}]"
