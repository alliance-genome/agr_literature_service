from typing import Dict
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Index, text
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class LaboratoryAlleleDesignationModel(Base, AuditedModel):
    __tablename__ = "laboratory_allele_designation"
    __versioned__: Dict = {}

    laboratory_allele_designation_id = Column(Integer, primary_key=True, autoincrement=True)

    laboratory_id = Column(
        Integer,
        ForeignKey("laboratory.laboratory_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    laboratory = relationship("LaboratoryModel", back_populates="allele_designations")

    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    mod = relationship("ModModel")

    allele_designation = Column(String(), nullable=False)

    is_obsolete = Column(Boolean, nullable=False, server_default=text("false"))

    __table_args__ = (
        # One active allele designation per (laboratory, mod); obsolete rows are
        # unlimited, so an allele designation can be soft-deleted and re-added.
        # Mirrors the laboratory_cross_reference / person_cross_reference pattern.
        # laboratory_id is NOT NULL here, so no IS NOT NULL clause is needed.
        Index(
            "uq_laboratory_allele_designation_lab_mod", "laboratory_id", "mod_id",
            unique=True,
            postgresql_where=(is_obsolete.is_(False)),
        ),
    )

    @property
    def mod_abbreviation(self):
        """Convenience for serializers — the MOD abbreviation via the mod FK."""
        return self.mod.abbreviation if self.mod else None

    @property
    def laboratory_curie(self):
        """Convenience for serializers — the owning laboratory's curie."""
        return self.laboratory.curie if self.laboratory else None

    def __str__(self) -> str:
        return f"{self.allele_designation} (lab={self.laboratory_id}, mod={self.mod_id})"
