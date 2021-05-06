from datetime import datetime
import pytz

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.orm import relationship
#from sqlalchemy_continuum import make_versioned

from literature.database.main import Base

if TYPE_CHECKING:
    from .user import User  # noqa: F401

#from references.schemas.allianceCategory import AllianceCategory

from enum import Enum

class Reference(Base):
    __tablename__ = 'references'
    __versioned__ = {}

    reference_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    curie = Column(
        String(28),
        unique=True,
        nullable=False,
        index=True
    )

    resource_id = Column(
        Integer,
        ForeignKey('resources.resource_id'),
        nullable=True
    )

    resource = relationship(
        'Resource',
        back_populates="references",
        single_parent=True,
    )

#    identifiers = relationship('Identifier' , backref='reference', lazy=True)

    title = Column(
        String,
        unique=False,
        nullable=True
    )

    authors = relationship(
        'Author',
        back_populates='reference',
        cascade="all, delete, delete-orphan"
    )

    editors = relationship(
        'Editor',
        back_populates='reference',
        cascade="all, delete, delete-orphan"
    )

    datePublished = Column(
        String(255),
        unique=False,
        nullable=True
    )

    dateArrivedInPubMed = Column(
        String(255),
        unique=False,
        nullable=True
    )

    dateLastModified = Column(
        String(255),
        unique=False,
        nullable=True
    )

    volume = Column(
        String(255),
        unique=False,
        nullable=True
    )

#    pages = relationship('Page' , backref='reference', lazy=True)

    abstract = Column(
        String(255),
        unique=False,
        nullable=True
    )

    citation = Column(
        String(255),
        unique=False,
        nullable=True
    )

#    keywords = relationship('Keyword' , backref='reference', lazy=True)

    pubMedType = Column(
        String(255),
        unique=False,
        nullable=True
    )

    publisher = Column(
        String(255),
        unique=False,
        nullable=True
    )

#    allianceCategory = Column(Enum(AllianceCategory), unique=False, nullable=True)

#    modReferenceTypes = relationship('ModReferenceType' , backref='reference', lazy=True)

    issueName = Column(
        String(255),
        unique=False,
        nullable=True
    )

    issueDate = Column(
        String(255),
        unique=False,
        nullable=True
    )

#    tags = relationship('Tag' , backref='reference', lazy=True)

#    meshTerms = relationship('MeshTerm' , backref='reference', lazy=True)

    resourceAbbreviation = Column(
        String(255),
        unique=False,
        nullable=True
    )

#    updatedBy = Column(
#        String(255),
#        unique=False,
#        nullable=True
#    )

    dateUpdated = Column(
        DateTime,
        nullable=True,
        default=datetime.utcnow
    )

    dateCreated = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone('UTC'))
    )
