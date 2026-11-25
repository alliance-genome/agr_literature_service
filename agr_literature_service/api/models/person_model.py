from typing import Dict
from sqlalchemy import (Column, DateTime, String, ARRAY, Boolean)
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.user_model import (UserModel, child_agent_constraints, child_agent_id,
                                                          child_agent_type, id_alias)

enable_versioning()


class PersonModel(UserModel):
    """A person is an agent (SCRUM-6995): person.id is its agent.id, and its full name is
    the agent's display_name; the audit columns live on agent too."""
    __tablename__ = "person"
    __versioned__: Dict = {"exclude": ["agent_type"]}

    id = child_agent_id()
    person_id = id_alias()
    agent_type = child_agent_type('person')

    curie = Column(String(), nullable=False, unique=True, index=True)    # required, server-generated
    mod_roles = Column(ARRAY(String), nullable=True)
    webpage = Column(ARRAY(String), nullable=True)
    active_status = Column(
        String(),
        nullable=False,
        default="active",
        server_default="active",
    )
    privacy = Column(
        String(),
        nullable=False,
        default="hide_email",
        server_default="hide_email",
    )

    # Address fields
    city = Column(String(), nullable=True)
    state = Column(String(), nullable=True)
    postal_code = Column(String(), nullable=True)
    country = Column(String(), nullable=True)
    street_address = Column(String(), nullable=True)
    address_last_updated = Column(DateTime, nullable=True)

    biography_research_interest = Column(String(), nullable=True)

    unsubscribe = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default="false",
    )

    emails = relationship(
        "PersonEmailModel",
        foreign_keys="PersonEmailModel.person_id",
        back_populates="person",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    institutions = relationship(
        "PersonInstitutionModel",
        foreign_keys="PersonInstitutionModel.person_id",
        back_populates="person",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    cross_references = relationship(
        "PersonCrossReferenceModel",
        foreign_keys="PersonCrossReferenceModel.person_id",
        back_populates="person",
        cascade="all, delete-orphan",
    )
    settings = relationship(
        "PersonSettingModel",
        foreign_keys="PersonSettingModel.person_id",
        back_populates="person",
        cascade="all, delete-orphan",
    )
    names = relationship(
        "PersonNameModel",
        foreign_keys="PersonNameModel.person_id",
        back_populates="person",
        cascade="all, delete-orphan",
    )
    notes = relationship(
        "PersonNoteModel",
        foreign_keys="PersonNoteModel.person_id",
        back_populates="person",
        cascade="all, delete-orphan",
    )
    lab_persons = relationship(
        "LaboratoryPersonModel",
        foreign_keys="LaboratoryPersonModel.person_id",
        back_populates="person",
        cascade="all, delete-orphan",
    )

    __mapper_args__ = {
        "polymorphic_identity": "person",
        "polymorphic_load": "selectin",
        "inherit_condition": id == UserModel.id,
    }
    __table_args__ = child_agent_constraints('person', 'person')

    def __str__(self) -> str:
        return f"{self.display_name} ({self.curie})"
