from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from literature.database.base import Base


class ReferenceManualTermTagModel(Base):
    __tablename__ = 'reference_manual_term_tags'
    __versioned__ = {}

    reference_manual_term_tag_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey('references.reference_id'),
        index=True,
        nullable=False
    )

    reference = relationship(
        'ReferenceModel',
        back_populates="manual_term_tags"
    )

    ontology = Column(
        String(),
        unique=False,
        nullable=False
    )

    datatype = Column(
        String(),
        unique=False,
        nullable=False
    )

    term = Column(
        String(),
        unique=False,
        nullable=False
    )
