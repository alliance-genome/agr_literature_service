from typing import Dict
from sqlalchemy import (
    Column, Integer, String, ARRAY, Boolean, CheckConstraint,
)
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class LaboratoryModel(Base, AuditedModel):
    __tablename__ = "laboratory"
    __versioned__: Dict = {}

    __table_args__ = (
        # A laboratory must be identifiable by at least a strain_designation or a
        # name (curie alone is not enough). DB-level backstop for the API validator.
        CheckConstraint(
            "strain_designation IS NOT NULL OR name IS NOT NULL",
            name="ck_laboratory_name_or_strain",
        ),
    )

    laboratory_id = Column(Integer, primary_key=True, autoincrement=True)

    # Allocated from MATI on create (laboratory subdomain -> AGRKB:104), like
    # reference/resource/person. Required and unique like those siblings, and
    # indexed for lookup.
    curie = Column(String(), nullable=False, unique=True, index=True)

    name = Column(String(), nullable=True)
    strain_designation = Column(String(), nullable=True)

    institution = Column(ARRAY(String), nullable=True)
    webpage = Column(ARRAY(String), nullable=True)

    # Address fields (Person-style)
    city = Column(String(), nullable=True)
    state = Column(String(), nullable=True)
    postal_code = Column(String(), nullable=True)
    country = Column(String(), nullable=True)
    street_address = Column(String(), nullable=True)

    email = Column(ARRAY(String), nullable=True)
    # Controlled vocabulary enforced by the API (public / logged_in_user / not_shown).
    email_visibility = Column(String(), nullable=True)

    lab_is_open = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    # Controlled vocabulary enforced by the API (active / closed / unknown).
    status = Column(
        String(),
        nullable=False,
        default="active",
        server_default="active",
    )

    research_area = Column(String(), nullable=True)
    short_research_description = Column(String(), nullable=True)
    additional_information = Column(String(), nullable=True)
    private_note = Column(String(), nullable=True)

    cross_references = relationship(
        "LaboratoryCrossReferenceModel",
        back_populates="laboratory",
        cascade="all, delete-orphan",
    )
    allele_designations = relationship(
        "LaboratoryAlleleDesignationModel",
        back_populates="laboratory",
        cascade="all, delete-orphan",
    )
    lab_persons = relationship(
        "LaboratoryPersonModel",
        back_populates="laboratory",
        cascade="all, delete-orphan",
    )

    def __str__(self) -> str:
        return f"{self.name} ({self.curie})"
