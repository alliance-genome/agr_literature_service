"""
workflow_tag_model.py
==================
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import (Column, DateTime, ForeignKey, Integer,
                        String)
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning


enable_versioning()


class WorkflowTagModel(Base):
    __tablename__ = "workflow_tag"
    __versioned__: Dict = {}

    reference_workflow_tag_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

# reference id - internal reference id
    reference_id = Column(
        Integer,
        ForeignKey("reference.reference_id"),
        index=True,
        nullable=False
    )

    reference = relationship(
        "ReferenceModel",
        foreign_keys="WorkflowTagModel.reference_id",
        back_populates="workflow_tag"
    )

# workflow_tag node term-id - string from A api.
    workflow_tag_id = Column(
        String(),
        unique=False,
        nullable=False
    )

# mod - from mod table (null means all).  Curators will be explicit and know that null means all, would never want to be vague and separate vague null from explicit all.
    mod_id = Column(
        Integer,
        ForeignKey("mod.mod_id"),
        nullable=True
    )

# date created - timestamp
# date updated - timestamp
    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone("UTC"))
    )

    date_updated = Column(
        DateTime,
        nullable=True
    )

# created by - id from users table
# updated by - id from users table
    created_by = Column(
        String,
        ForeignKey("users.id"),
        nullable=False
    )

    updated_by = Column(
        String,
        ForeignKey("users.id"),
        nullable=True
    )
