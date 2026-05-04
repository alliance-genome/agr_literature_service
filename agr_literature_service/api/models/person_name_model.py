from typing import Dict
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Index,
    Boolean,
    text,
)
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonNameModel(Base, AuditedModel):
    __tablename__ = "person_name"
    __versioned__: Dict = {}

    person_name_id = Column(Integer, primary_key=True, autoincrement=True)

    person_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    person = relationship("PersonModel", back_populates="names")

    first_name = Column(String(), nullable=True)
    middle_name = Column(String(), nullable=True)
    last_name = Column(String(), nullable=False)

    primary = Column("primary", Boolean, nullable=True)

    __table_args__ = (
        # At most one primary name per person
        Index(
            "ux_person_name_person_primary_true",
            "person_id",
            unique=True,
            postgresql_where=text('"primary" = TRUE'),
        ),

        # Composite index for queries like
        #   WHERE person_id = ? AND primary = TRUE
        Index("ix_person_name_person_primary", "person_id", "primary"),
    )

    def __str__(self) -> str:
        parts = []
        if self.first_name:
            parts.append(self.first_name)
        if self.middle_name:
            parts.append(self.middle_name)
        parts.append(self.last_name or "")
        label = " ".join(parts)
        if self.primary:
            label += " [primary]"
        return label
