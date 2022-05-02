"""
cross_reference_model.py
========================
"""


from typing import Dict

from sqlalchemy import ARRAY, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from literature.database.base import Base
from literature.database.versioning import enable_versioning


enable_versioning()


class CrossReferenceModel(Base):
    __tablename__ = "cross_reference"
    __versioned__: Dict = {}

    curie = Column(
        String,
        primary_key=True
    )

    is_obsolete = Column(
        Boolean,
        unique=False,
        default=False
    )

    reference_id = Column(
        Integer,
        ForeignKey("references.reference_id", ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="cross_reference"
    )

    resource_id = Column(
        Integer,
        ForeignKey("resources.resource_id"),
        index=True
    )

    resource = relationship(
        "ResourceModel",
        back_populates="cross_reference"
    )

    author = relationship(
        "AuthorModel",
        back_populates="orcid_cross_reference"
    )

    editors = relationship(
        "EditorModel",
        back_populates="orcid_cross_reference"
    )

    pages = Column(
        ARRAY(String()),
        nullable=True
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "CrossReference: curie='{}' is_obsolete='{}' reference_id='{}', resource_id='{}' pages={}".\
            format(self.curie, self.is_obsolete, self.reference_id, self.resource_id, self.pages)
