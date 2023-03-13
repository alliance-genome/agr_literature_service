"""
reference_model.py
==================
"""


from typing import Dict

from sqlalchemy import (ARRAY, Column, Enum, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Boolean

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.schemas import PubMedPublicationStatus, ReferenceCategory
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class ReferenceModel(Base, AuditedModel):
    __tablename__ = "reference"
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

    cross_reference = relationship(
        "CrossReferenceModel",
        back_populates="reference",
        cascade="all, delete, delete-orphan",
        passive_deletes=True
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

    obsolete_reference = relationship(
        "ObsoleteReferenceModel",
        foreign_keys="ObsoleteReferenceModel.new_id"
    )

    resource_id = Column(
        Integer,
        ForeignKey("resource.resource_id"),
        index=True,
        nullable=True
    )

    resource = relationship(
        "ResourceModel",
        back_populates="reference",
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

    mod_referencetypes = relationship("ReferenceModReferencetypeAssociationModel")

    mod_corpus_association = relationship(
        "ModCorpusAssociationModel",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    author = relationship(
        "AuthorModel",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    date_published = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_published_start = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_published_end = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_arrived_in_pubmed = Column(
        String(),
        unique=False,
        nullable=True
    )

    date_last_modified_in_pubmed = Column(
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

    page_range = Column(
        String(),
        unique=False,
        nullable=True
    )

    abstract = Column(
        String(),
        unique=False,
        nullable=True
    )

    keywords = Column(
        ARRAY(String()),
        unique=False,
        nullable=True
    )

    pubmed_types = Column(
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

    mesh_term = relationship(
        "MeshDetailModel",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    citation = Column(
        String(),
        unique=False,
        nullable=True
    )

    workflow_tag = relationship(
        "WorkflowTagModel",
        foreign_keys="WorkflowTagModel.reference_id",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    topic_entity_tags = relationship(
        "TopicEntityTagModel",
        foreign_keys="TopicEntityTagModel.reference_id",
        back_populates="reference",
        cascade="all, delete, delete-orphan"
    )

    referencefiles = relationship(
        "ReferencefileModel",
        foreign_keys="ReferencefileModel.reference_id",
        back_populates="reference",
        cascade="all, delete, delete-orphan",
    )

    copyright_license_id = Column(
        Integer,
        ForeignKey("copyright_license.copyright_license_id"),
        nullable=True,
        index=True
    )

    copyright_license = relationship(
        "CopyrightLicenseModel"
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        ids = "Reference id = {} curie='{}' resource_id='{}'\n".\
            format(self.reference_id, self.curie, self.resource_id)
        dates = "\tDates: updated='{}', created='{}', published='{}', arrived_p='{}', last_mod='{}'\n".\
            format(self.date_updated, self.date_created, self.date_published,
                   self.date_arrived_in_pubmed, self.date_last_modified_in_pubmed)
        long = ""
        if self.title:
            long += "\ttitle10='{}\n".format(self.title[:10])
        else:
            long += "\tNO title??\n"
        if self.abstract:
            long += "\tabstract10='{}...'\n".format(self.abstract[:10])
        else:
            long += "NO abstract?\n"
        auths = [str(x) for x in self.author]
        mesh = [str(x) for x in self.mesh_term]
        peps = "\tauthors='{}'\n".format(auths)
        arrs = "\tmesh='{}'\n\tkeywords='{}'\n".format(str(mesh), self.keywords)
        return "{}{}{}{}{}".format(ids, dates, long, peps, arrs)
