from typing import Dict
from sqlalchemy import ARRAY, Boolean, Column, ForeignKey, Integer, String, Index, and_, text
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
        # Uniqueness is enforced only among non-obsolete rows (mirrors the Biblio
        # cross_reference partial unique indexes), so a soft-deleted xref does not
        # block re-adding the same curie/prefix.
        Index("uq_person_xref_curie", "curie",
              unique=True,
              postgresql_where=(is_obsolete.is_(False))),
        Index("uq_person_xref_person_prefix", "person_id", "curie_prefix",
              unique=True,
              postgresql_where=(and_(is_obsolete.is_(False),
                                     person_id.isnot(None)))),
        Index("ix_person_xref_person_id", "person_id"),
        Index("ix_person_xref_prefix_curie", "curie_prefix", "curie"),
    )

    @property
    def person_curie(self):
        """Convenience for serializers — the owning person's curie."""
        return self.person.curie if self.person else None

    def __str__(self) -> str:
        status = "obsolete" if self.is_obsolete else "active"
        return f"{self.curie} [{status}]"
