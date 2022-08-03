"""
topic_entity_tag_model.py
==================
"""


from datetime import datetime
from typing import Dict

import pytz
from sqlalchemy import (Column, DateTime, ForeignKey, Integer, String)
from sqlalchemy.orm import relationship

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning

enable_versioning()


class TopicEntityTagModel(Base):
    __tablename__ = "topic_entity_tag"
    __versioned__: Dict = {}

    topic_entity_tag_id = Column(
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
        foreign_keys="TopicEntityTagModel.reference_id",
        back_populates="topic_entity_tags"
    )

    # Obtained from A-Team ontology node term-id
    topic = Column(
        String(),
        unique=False,
        nullable=False
    )

    # Obtained from A-Team ontology node term-id
    entity_type = Column(
        String(),
        unique=False,
        nullable=False
    )

    # One of the XXX_entity's MUST be set
    # cannot do via constraints so will need to be a
    # software check.
    alliance_entity = Column(
        String(),
        unique=False,
        nullable=True
    )

    mod_entity = Column(
        String(),
        unique=False,
        nullable=True
    )

    new_entity = Column(
        String(),
        unique=False,
        nullable=True
    )

    # Taxon_id
    taxon = Column(
        Integer(),
        unique=False,
        nullable=False
    )

    note = Column(
        String(),
        unique=False,
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
        nullable=True,
        default=datetime.utcnow
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


class TopicEntityTagPropModel(Base):
    __tablename__ = "topic_entity_tag_prop"
    __versioned__: Dict = {}

    topic_entity_tag_prop_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    topic_entity_tag_id = Column(
        Integer,
        ForeignKey("topic_entity_tag.topic_entity_tag_id", ondelete="CASCADE"),
        index=True,
        nullable=False
    )

    # Obtained from A-Team ontology qualifier
    qualifier = Column(
        String(),
        unique=False,
        nullable=False
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
        nullable=True,
        default=datetime.utcnow
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
