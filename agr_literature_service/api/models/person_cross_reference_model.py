from typing import Dict
from sqlalchemy import ARRAY, Boolean, Column, ForeignKey, Integer, String, Index, UniqueConstraint, text
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonCrossReferenceModel(Base, AuditedModel):
    __tablename__ = "person_cross_reference"
    __versioned__: Dict = {}

    person_cross_reference_id = Column(Integer, primary_key=True, autoincrement=True)
    curie = Column(String(), nullable=False, index=True)
    curie_prefix = Column(String(), nullable=False, index=True)

    person_id = Column(Integer, ForeignKey("person.person_id", ondelete="CASCADE"), nullable=True, index=True)
    person = relationship("PersonModel", back_populates="cross_references")  # <-- match PersonModel name

    pages = Column(ARRAY(String), nullable=True)
    is_obsolete = Column(Boolean, nullable=False, server_default=text("false"))

    __table_args__ = (
        UniqueConstraint("curie", name="uq_person_xref_curie"),
        UniqueConstraint("person_id", "curie_prefix", name="uq_person_xref_person_prefix"),
        Index("ix_person_xref_person_id", "person_id"),
        Index("ix_person_xref_prefix_curie", "curie_prefix", "curie"),
    )

    def __str__(self) -> str:
        status = "obsolete" if self.is_obsolete else "active"
        return f"{self.curie} [{status}]"
