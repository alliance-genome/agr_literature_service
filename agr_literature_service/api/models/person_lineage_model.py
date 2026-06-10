from typing import Dict
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship as orm_relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonLineageModel(Base, AuditedModel):
    __tablename__ = "person_lineage"
    __versioned__: Dict = {}

    person_lineage_id = Column(Integer, primary_key=True, autoincrement=True)

    # Names are the primary identifiers and are always required. The person object
    # links are optional and late-bound: at creation time the caller typically knows
    # only the names, and the matching Person rows may not exist yet (or ever).
    person_one_name = Column(String(), nullable=False)
    person_one = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    person_two_name = Column(String(), nullable=False)
    person_two = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    # Controlled vocabulary enforced by the API (PersonPersonRole).
    relationship = Column(String(), nullable=False)

    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    who_sent_this = Column(String(), nullable=False)

    person_one_obj = orm_relationship("PersonModel", foreign_keys=[person_one])
    person_two_obj = orm_relationship("PersonModel", foreign_keys=[person_two])

    def __str__(self) -> str:
        return f"{self.person_one_name} -[{self.relationship}]-> {self.person_two_name}"
