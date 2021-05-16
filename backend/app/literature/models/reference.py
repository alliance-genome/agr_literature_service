from datetime import datetime
import pytz

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import DateTime
from sqlalchemy import ARRAY
from sqlalchemy import Enum

from sqlalchemy.orm import relationship

from literature.database.base import Base

from literature.schemas.referenceCategory import ReferenceCategory


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

    cross_references = relationship(
        'CrossReference',
        lazy='joined',
        back_populates='reference',
        cascade="all, delete, delete-orphan"
    )

    files = relationship(
        'File',
        lazy='joined',
        back_populates='reference',
    )



    resource_id = Column(
        Integer,
        ForeignKey('resources.resource_id'),
        nullable=True
    )

    resource = relationship(
        'Resource',
        lazy='joined',
        back_populates="references",
        single_parent=True,
    )

    title = Column(
        String,
        unique=False,
        nullable=True
    )

    language = Column(
        String,
        unique=False,
        nullable=True
    )

    modReferenceTypes = relationship(
        'ModReferenceType',
        lazy='joined',
        back_populates='reference',
        cascade="all, delete, delete-orphan"
    )

    authors = relationship(
        'Author',
        lazy='joined',
        back_populates='reference',
        cascade="all, delete, delete-orphan"
    )

    editors = relationship(
        'Editor',
        lazy='joined',
        back_populates='reference',
        cascade="all, delete, delete-orphan"
    )

    datePublished = Column(
        String(),
        unique=False,
        nullable=True
    )

    dateArrivedInPubMed = Column(
        String(),
        unique=False,
        nullable=True
    )

    dateLastModified = Column(
        String(),
        unique=False,
        nullable=True
    )

    volume = Column(
        String(),
        unique=False,
        nullable=True
    )

    pages = Column(
        String(),
        unique=False,
        nullable=True
    )

    abstract = Column(
        String(),
        unique=False,
        nullable=True
    )

    citation = Column(
        String(),
        unique=False,
        nullable=True
    )

    keywords = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    pubMedType = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    publisher = Column(
        String(),
        unique=False,
        nullable=True
    )

    category = Column(
        Enum(ReferenceCategory),
        unique=False,
        nullable=True
    )

    issueName = Column(
        String(),
        unique=False,
        nullable=True
    )

    issueDate = Column(
        String(),
        unique=False,
        nullable=True
    )

    tags = relationship(
        'ReferenceTag',
        lazy='joined',
        back_populates='reference',
        cascade="all, delete, delete-orphan"
    )

    mesh_terms = relationship(
        'MeshDetail',
        lazy='joined',
        back_populates='reference',
        cascade="all, delete, delete-orphan"
    )

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
