from typing import Dict
from sqlalchemy import ARRAY, Boolean, Column, ForeignKey, Integer, String, Index, UniqueConstraint, text
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class LaboratoryCrossReferenceModel(Base, AuditedModel):
    __tablename__ = "laboratory_cross_reference"
    __versioned__: Dict = {}

    laboratory_cross_reference_id = Column(Integer, primary_key=True, autoincrement=True)
    curie = Column(String(), nullable=False, index=True)
    curie_prefix = Column(String(), nullable=False, index=True)

    laboratory_id = Column(
        Integer,
        ForeignKey("laboratory.laboratory_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    laboratory = relationship("LaboratoryModel", back_populates="cross_references")

    pages = Column(ARRAY(String), nullable=True)
    is_obsolete = Column(Boolean, nullable=False, server_default=text("false"))

    __table_args__ = (
        UniqueConstraint("curie", name="uq_laboratory_xref_curie"),
        UniqueConstraint("laboratory_id", "curie_prefix", name="uq_laboratory_xref_laboratory_prefix"),
        Index("ix_laboratory_xref_laboratory_id", "laboratory_id"),
        Index("ix_laboratory_xref_prefix_curie", "curie_prefix", "curie"),
    )

    @property
    def laboratory_curie(self):
        """Convenience for serializers — the owning laboratory's curie."""
        return self.laboratory.curie if self.laboratory else None

    def __str__(self) -> str:
        status = "obsolete" if self.is_obsolete else "active"
        return f"{self.curie} [{status}]"
