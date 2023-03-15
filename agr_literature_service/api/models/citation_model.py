# from typing import Dict

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
# from agr_literature_service.api.database.versioning import enable_versioning


class CitationModel(Base):
    __tablename__ = "citation"

    citation_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="citation_id"
    )

    citation = Column(
        String(),
        unique=False,
        nullable=True
    )

    short_citation = Column(
        String(),
        unique=False,
        nullable=True
    )
