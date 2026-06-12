from typing import Dict
from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
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

    __table_args__ = (
        UniqueConstraint(
            "laboratory_id", "mod_id", name="uq_laboratory_allele_designation_lab_mod"
        ),
    )

    @property
    def mod_abbreviation(self):
        """Convenience for serializers — the MOD abbreviation via the mod FK."""
        return self.mod.abbreviation if self.mod else None

    def __str__(self) -> str:
        return f"{self.allele_designation} (lab={self.laboratory_id}, mod={self.mod_id})"
