from typing import Dict
from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class VocabularyAbcModel(Base, AuditedModel):
    __tablename__ = "vocabulary_abc"
    __versioned__: Dict = {}
    __table_args__ = (
        UniqueConstraint("vocabulary", name="uq_vocabulary_abc_vocabulary"),
    )

    vocabulary_abc_id = Column(Integer, primary_key=True, autoincrement=True)
    vocabulary = Column(String(), nullable=False)

    terms = relationship(
        "VocabularyTermAbcModel",
        back_populates="vocabulary",
        cascade="all, delete-orphan",
    )
