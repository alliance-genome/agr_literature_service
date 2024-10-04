"""
Citations are filled in via triggers and stored procedures.
See api/triggers directory for more info.
"""

from sqlalchemy import Column, Integer, String
from agr_literature_service.api.database.base import Base


class CitationModel(Base):
    __tablename__ = "citation"
    __bind_key__ = 'lit'
    __table_args__ = {"schema": "lit"}

    citation_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
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
