from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import Integer

from sqlalchemy.orm import relationship

from literature.database.base import Base


class ModReferenceType(Base):
    __tablename__ = 'mod_reference_types'
    __versioned__ = {}

    mod_reference_type_id = Column(
        Integer,
        primary_key=True,
        autoincrement=True
    )

    reference_id = Column(
        Integer,
        ForeignKey('references.reference_id',
                   ondelete='CASCADE')
    )

    reference = relationship(
        'Reference',
        back_populates="modReferenceTypes"
    )

    reference_type = Column(
        String(),
        unique=False,
        nullable=False
    )

    source = Column(
        String(),
        unique=False,
        nullable=True
    )
