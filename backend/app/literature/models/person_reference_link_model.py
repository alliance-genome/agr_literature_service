from sqlalchemy import Column, ForeignKey, Integer, String

from literature.database.base import Base


class PersonReferenceLinkModel(Base):
    __tablename__ = 'person_reference_link'

    person_id = Column(
        Integer,
        ForeignKey('people.person_id'),
        primary_key=True
    )

    reference_curie = Column(
        String,
        ForeignKey('references.curie'),
        primary_key=True
    )
