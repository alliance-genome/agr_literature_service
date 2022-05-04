"""
mesh_detail_model.py
====================
"""

from typing import Dict

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from literature.database.base import Base
from literature.database.versioning import enable_versioning


enable_versioning()


class MeshDetailModel(Base):
    __tablename__ = "mesh_detail"
    __versioned__: Dict = {}

    mesh_detail_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id",
                   ondelete="CASCADE"),
        index=True
    )

    reference = relationship(
        "ReferenceModel",
        back_populates="mesh_term"
    )

    heading_term = Column(
        String,
        unique=False,
        nullable=False
    )

    qualifier_term = Column(
        String,
        unique=False,
        nullable=True
    )

    def __str__(self):
        """
        Overwrite the default output.
        """
        return "ht={}, qt={}".format(self.heading_term, self.qualifier_term)
