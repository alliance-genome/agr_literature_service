from typing import Dict
from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class VocabularyTermSynonymAbcModel(Base, AuditedModel):
    __tablename__ = "vocabulary_term_synonym_abc"
    __versioned__: Dict = {}
    __table_args__ = (
        UniqueConstraint("vocabulary_term_abc_id", "synonym_name",
                         name="uq_vocabulary_term_synonym_abc_term_synonym"),
    )

    vocabulary_term_synonym_abc_id = Column(Integer, primary_key=True, autoincrement=True)
    vocabulary_term_abc_id = Column(
        Integer,
        ForeignKey("vocabulary_term_abc.vocabulary_term_abc_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    synonym_name = Column(String(), nullable=False)

    term = relationship("VocabularyTermAbcModel", back_populates="synonyms")
