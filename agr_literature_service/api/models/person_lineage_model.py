from typing import Dict
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship as orm_relationship
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()


class PersonLineageModel(Base, AuditedModel):
    """A validated person-to-person relationship (canonical fact).

    Keyed on the two resolved person ids; there is exactly one row per
    (person_one_id, person_two_id, relationship). Display names come from
    joining the person table, so no name columns are stored here.
    """
    __tablename__ = "person_lineage"
    __versioned__: Dict = {}

    person_lineage_id = Column(Integer, primary_key=True, autoincrement=True)

    person_one_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    person_two_id = Column(
        Integer,
        ForeignKey("person.person_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Controlled vocabulary enforced by the API (PersonPersonRole).
    relationship = Column(String(), nullable=False)

    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)

    person_one_obj = orm_relationship("PersonModel", foreign_keys=[person_one_id])
    person_two_obj = orm_relationship("PersonModel", foreign_keys=[person_two_id])
    submissions = orm_relationship("PersonLineageSubmissionModel", back_populates="canonical")

    __table_args__ = (
        UniqueConstraint(
            "person_one_id", "person_two_id", "relationship",
            name="uq_person_lineage_person_ids_relationship",
        ),
    )

    def __str__(self) -> str:
        return f"person_lineage({self.person_one_id} -[{self.relationship}]-> {self.person_two_id})"
