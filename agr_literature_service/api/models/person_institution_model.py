from typing import Dict
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonInstitutionModel(Base, AuditedModel):
    __tablename__ = "person_institution"
    __versioned__: Dict = {}

    person_institution_id = Column(Integer, primary_key=True, autoincrement=True)

    person_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    person = relationship("PersonModel", back_populates="institutions")

    institution = Column(String(), nullable=False)

    date_made_old_institution = Column(DateTime, nullable=True)

    # Deliberately NO uniqueness index, unlike person_email's
    # uq_person_email_person_address_lower. Emails are identifiers, so a
    # case-variant duplicate is the same address. Institutions are free text and
    # people return to them (Caltech -> MIT -> Caltech), so the old row and the
    # new active one must be allowed to coexist. There is also no lower()
    # functional index and no partial active-row index: nothing looks an
    # institution up by value, and there is no get_most_current_institution()
    # equivalent of the email SQL function for such an index to serve.
    __table_args__ = (
        Index("ix_person_institution_institution", "institution"),
    )

    @property
    def person_curie(self):
        """Convenience for serializers — the owning person's curie."""
        return self.person.curie if self.person else None

    def __str__(self) -> str:
        status = "old" if self.date_made_old_institution else "active"
        return f"{self.institution} [{status}]"
