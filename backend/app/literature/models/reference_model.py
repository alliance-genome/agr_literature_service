"""
reference_model.py
==================
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import (ARRAY, Column, DateTime, Enum, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Boolean

from literature.database.base import Base
from literature.schemas import PubMedPublicationStatus, ReferenceCategory


class ReferenceModel(Base):
    __tablename__ = "references"
    __versioned__: Dict = {}

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
        "CrossReferenceModel",
        lazy="joined",
        back_populates="reference",
        cascade="all, delete, delete-orphan",
        passive_deletes=True
    )

    files = relationship(
        "FileModel",
        lazy="joined",
        back_populates="reference"
    )

    comment_and_corrections_out = relationship(
        "ReferenceCommentAndCorrectionModel",
        foreign_keys="ReferenceCommentAndCorrectionModel.reference_id_from",
        back_populates="reference_from"
    )

    comment_and_corrections_in = relationship(
        "ReferenceCommentAndCorrectionModel",
        foreign_keys="ReferenceCommentAndCorrectionModel.reference_id_to",

        back_populates="reference_to"
    )

    merged_into_id = Column(
        Integer,
        ForeignKey("references.reference_id")
    )

    merged_into_reference = relationship(
        "ReferenceModel",
        remote_side=[reference_id]
    )

    mergee_references = relationship(
        "ReferenceModel"
    )

    automated_term_tags = relationship(
        "ReferenceAutomatedTermTagModel",
        back_populates="reference"
    )

    manual_term_tags = relationship(
        "ReferenceManualTermTagModel",
        back_populates="reference"
    )

    notes = relationship(
        "NoteModel",
        lazy="joined",
        back_populates="reference"
    )

    resource_id = Column(
        Integer,
        ForeignKey("resources.resource_id"),
        index=True,
        nullable=True
    )

    resource = relationship(
        "ResourceModel",
        back_populates="references",
        single_parent=True,
    )

    verified_people = relationship(
        "PersonModel",
        lazy="joined",
        secondary="person_reference_link"
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

    mod_reference_types = relationship(
        "ModReferenceTypeModel",
        lazy="joined",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    mod_corpus_associations = relationship(
        "ModCorpusAssociationModel",
        lazy="joined",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    authors = relationship(
        "AuthorModel",
        lazy="joined",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    editors = relationship(
        "EditorModel",
        lazy="joined",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    date_published = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_arrived_in_pubmed = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_last_modified = Column(
        String(),
        unique=False,
        nullable=True
    )

    volume = Column(
        String(),
        unique=False,
        nullable=True
    )

    plain_language_abstract = Column(
        String(),
        unique=False,
        nullable=True
    )

    pubmed_abstract_languages = Column(
        ARRAY(String()),
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

    pubmed_type = Column(
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

    pubmed_publication_status = Column(
        Enum(PubMedPublicationStatus),
        unique=False,
        nullable=True
    )

    issue_name = Column(
        String(),
        unique=False,
        nullable=True
    )

    issue_date = Column(
        String(),
        unique=False,
        nullable=True
    )

    tags = relationship(
        "ReferenceTagModel",
        lazy="joined",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    mesh_terms = relationship(
        "MeshDetailModel",
        lazy="joined",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    date_updated = Column(
        DateTime,
        nullable=True,
        default=datetime.utcnow
    )

    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone("UTC"))
    )

    open_access = Column(
        Boolean,
        nullable=False,
        default=False
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        ids = "Reference id = {} curie='{}' resource_id='{}'\n".\
            format(self.reference_id, self.curie, self.resource_id)
        dates = "\tDates: updated='{}', created='{}', published='{}', arrived_p='{}', last_mod='{}'\n".\
            format(self.date_updated, self.date_created, self.date_published,
                   self.date_arrived_in_pubmed, self.date_last_modified)
        long = "\ttitle10='{}...'\n\tabstract10='{}...'\n".format(self.title[:10], self.abstract[:10])
        auths = [str(x) for x in self.authors]
        mesh = [str(x) for x in self.mesh_terms]
        tags = [str(x) for x in self.tags]
        peps = "\tauthors='{}'\n\teditors='{}'\n".format(auths, str(self.editors))
        arrs = "\ttags='{}'\n\tmesh='{}'\n\tkeywords='{}'\n".format(str(tags), str(mesh), self.keywords)
        return "{}{}{}{}{}".format(ids, dates, long, peps, arrs)
