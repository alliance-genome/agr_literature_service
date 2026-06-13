from typing import Dict
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Boolean, Index
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class LaboratoryPersonModel(Base, AuditedModel):
    __tablename__ = "laboratory_person"
    __versioned__: Dict = {}

    laboratory_person_id = Column(Integer, primary_key=True, autoincrement=True)

    laboratory_id = Column(
        Integer,
        ForeignKey("laboratory.laboratory_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    laboratory = relationship("LaboratoryModel", back_populates="lab_persons")

    person_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    person = relationship("PersonModel", back_populates="lab_persons")

    # Timestamp fields (when the person became/stopped being PI, when they became alum).
    is_pi = Column(DateTime, nullable=True)
    former_pi = Column(DateTime, nullable=True)
    alum = Column(DateTime, nullable=True)

    is_lab_contact = Column(Boolean, nullable=False, default=False, server_default="false")
    can_edit_lab = Column(Boolean, nullable=False, default=False, server_default="false")

    # Controlled vocabulary enforced by the API (LabPosition).
    lab_position = Column(String(), nullable=True)

    __table_args__ = (
        Index("ix_laboratory_person_laboratory_person", "laboratory_id", "person_id"),
    )

    @property
    def laboratory_curie(self):
        """Convenience for serializers — the laboratory curie via the laboratory FK."""
        return self.laboratory.curie if self.laboratory else None

    @property
    def person_curie(self):
        """Convenience for serializers — the person curie via the person FK."""
        return self.person.curie if self.person else None

    def __str__(self) -> str:
        return f"laboratory_person(lab={self.laboratory_id}, person={self.person_id})"
