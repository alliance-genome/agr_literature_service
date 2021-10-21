from datetime import datetime
from typing import Dict

import pytz

from sqlalchemy import Enum
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Float
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import ARRAY

from sqlalchemy.orm import relationship

from literature.database.base import Base


class ReferenceManualTermTagModel(Base):
    __tablename__ = 'reference_manual_term_tags'
    __versioned__: Dict = {}

    reference_manual_term_tag_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey('references.reference_id'),
        index=True,
        nullable=False
    )

    reference = relationship(
        'ReferenceModel',
        back_populates="manual_term_tags"
    )

    ontology = Column(
         String(),
         unique=False,
         nullable=False
    )

    datatype = Column(
        String(),
        unique=False,
        nullable=False
    )

    term = Column(
        String(),
        unique=False,
        nullable=False
    )
