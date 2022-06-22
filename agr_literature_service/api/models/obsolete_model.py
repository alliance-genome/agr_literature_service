"""
obsolete_model.py

Presently just for the merging and obsoletion of references. Other taabkes may be added later.
If a reference is made obsolete and not merged then the new_id will be null.
===============
"""

from typing import Dict
from sqlalchemy import Column, ForeignKey, Integer, String

from agr_literature_service.api.database.base import Base


class ObsoleteReferenceModel(Base):
    """
       curie and new_id names choosen so that if resource and person or others are added
       then we can use existing code, by passing the class to the method, as all other
       fields will be the same.
       NOTE: If x is merged into y and then y is merged into z at a later date then
             the curie for x should now point to the new_id of z and not y.
             If we need to work out what happended then the version table will tell us.
             Similarly for z being obsoleted. (x, y and z should have new_id of null)
    """
    __tablename__ = "obsolete_reference_curie"
    __versioned__: Dict = {}

    obsolete_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    curie = Column(
        String,
        unique=True,
        nullable=True
    )

    new_id = Column(
        Integer,
        ForeignKey("reference.reference_id", ondelete="CASCADE"),
        index=True
    )
