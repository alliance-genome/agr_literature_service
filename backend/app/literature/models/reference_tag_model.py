"""
reference_tag_model.py
======================
"""


from typing import Dict

from sqlalchemy import Column, Enum, ForeignKey, Integer
from sqlalchemy.orm import relationship

from literature.database.base import Base
from literature.schemas import TagName, TagSource
from literature.database.versioning import enable_versioning


enable_versioning()


class ReferenceTagModel(Base):
    __tablename__ = "reference_tags"
    __versioned__: Dict = {}

    reference_tag_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("references.reference_id",
                   ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="tags"
    )

    tag_name = Column(
        Enum(TagName),
        unique=False,
        nullable=False
    )

    tag_source = Column(
        Enum(TagSource),
        unique=False,
        nullable=False
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "name={}, source={}".format(self.tag_name, self.tag_source)
