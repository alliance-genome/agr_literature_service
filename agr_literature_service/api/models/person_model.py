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

    # Required display name
    display_name = Column(String(), nullable=False)

    # Optional identifiers
    curie = Column(String(), nullable=True, index=True)     # e.g., MATI id
    okta_id = Column(String(), nullable=True, index=True)   # Okta uid

    # Optional lists
    xrefs = Column(ARRAY(String), nullable=True)            # e.g., ["WBPerson:123", "ZFIN:..."]
    mod_roles = Column(ARRAY(String), nullable=True)        # e.g., Okta group?

    # Relationships
    users = relationship("UserModel", back_populates="person", cascade="all, delete-orphan")
    emails = relationship("EmailModel", back_populates="person", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("okta_id", name="uq_person_okta_id"),
        Index("ix_person_display_name_trigram", "display_name"),
    )

    def __str__(self) -> str:
        return f"{self.display_name} ({self.curie or 'no-curie'})"
