from typing import Dict
from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, String, UniqueConstraint, text
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class VocabularyTermAbcModel(Base, AuditedModel):
    __tablename__ = "vocabulary_term_abc"
    __versioned__: Dict = {}
    __table_args__ = (
        UniqueConstraint("vocabulary_abc_id", "name", name="uq_vocabulary_term_abc_vocab_name"),
    )

    vocabulary_term_abc_id = Column('id', BigInteger, primary_key=True, autoincrement=True)
    vocabulary_abc_id = Column(
        BigInteger,
        ForeignKey("vocabulary_abc.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name = Column(String(), nullable=False)
    # key='is_obsolete' keeps Continuum's <key>_mod flag on the obsolete_mod column
    is_obsolete = Column('obsolete', Boolean, key='is_obsolete', nullable=False, default=False,
                         server_default=text('false'))

    vocabulary = relationship("VocabularyAbcModel", back_populates="terms")
    synonyms = relationship(
        "VocabularyTermSynonymAbcModel",
        back_populates="term",
        cascade="all, delete-orphan",
    )
