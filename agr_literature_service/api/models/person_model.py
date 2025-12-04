from typing import Dict
from sqlalchemy import Column, Integer, String, ARRAY, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonModel(Base, AuditedModel):
    __tablename__ = "person"
    __versioned__: Dict = {}

    person_id = Column(Integer, primary_key=True, autoincrement=True)
    display_name = Column(String(), nullable=False)

    curie = Column(String(), nullable=True, index=True)     # optional (e.g., MATI)
    okta_id = Column(String(), nullable=True, index=True)   # optional
    mod_roles = Column(ARRAY(String), nullable=True)

    # Only keep these relationships
    emails = relationship("EmailModel", back_populates="person", cascade="all, delete-orphan")
    cross_references = relationship("PersonCrossReferenceModel", back_populates="person", cascade="all, delete-orphan")
    settings = relationship("PersonSettingModel", back_populates="person", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("okta_id", name="uq_person_okta_id"),
        Index("ix_person_display_name_trigram", "display_name"),
    )

    def __str__(self) -> str:
        return f"{self.display_name} ({self.curie or 'no-curie'})"
