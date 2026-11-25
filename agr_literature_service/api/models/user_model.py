from typing import Dict

from sqlalchemy import (BigInteger, CheckConstraint, Column, ForeignKeyConstraint, Index, Sequence, String,
                        UniqueConstraint, text)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import column_property
from agr_literature_service.api.database.base import Base
from agr_literature_service.api.database.versioning import enable_versioning
from agr_literature_service.api.models.audited_model import AuditedModel

enable_versioning()

AGENT_ID_SEQUENCE = Sequence('agent_id_seq')


def id_alias():
    """Another name for the id attribute (user_id, person_id), in queries and on instances.

    A hybrid rather than a synonym: Continuum cannot version a class with a synonym.
    """
    def _get(self):
        return self.id

    def _set(self, value):
        self.id = value

    return hybrid_property(_get, _set)


class UserModel(Base, AuditedModel):
    """An agent: a person (curator) or an automation (script, pipeline, service account).

    agent is the parent table of person and automation, which share its id (SCRUM-6995).
    Its label -- the person's curie, or the automation's display_name -- is what
    created_by / updated_by show; the audit columns themselves hold agent.id (SCRUM-6719).
    """
    __tablename__ = "agent"
    __versioned__: Dict = {}

    # the primary key attribute is id in every agent class: Continuum maps the joined-table
    # version classes by column key, and the children's id must combine with the parent's
    id = Column(BigInteger, AGENT_ID_SEQUENCE, server_default=AGENT_ID_SEQUENCE.next_value(), primary_key=True)
    user_id = id_alias()
    agent_type = Column(String, nullable=False, index=True)
    # a person's full name, or an automation's name (unique among automations)
    display_name = Column(String, nullable=False)

    __mapper_args__ = {"polymorphic_on": agent_type}

    __table_args__ = (
        CheckConstraint("agent_type IN ('person', 'automation')", name="ck_agent_agent_type"),
        UniqueConstraint("id", "agent_type", name="uq_agent_id_agent_type"),
        Index(
            "uq_agent_automation_display_name",
            "display_name",
            unique=True,
            postgresql_where=text("agent_type = 'automation'"),
        ),
        Index(
            "ix_agent_display_name_trgm",
            "display_name",
            postgresql_using="gin",
            postgresql_ops={"display_name": "gin_trgm_ops"},
        ),
    )

    def __str__(self) -> str:
        return f"User<{self.display_name}>"


def child_agent_id():
    """The primary key of a child table of agent: its agent's id, mapped onto the parent's id attribute."""
    return Column('id', BigInteger, primary_key=True, autoincrement=False)


def child_agent_type(agent_type: str):
    """The fixed agent_type column of a child table of agent, mapped together with agent.agent_type."""
    return column_property(
        Column('agent_type', String, nullable=False, server_default=agent_type),
        UserModel.agent_type,
    )


def child_agent_constraints(table: str, agent_type: str):
    """The check and the two-column FK that tie a child row to an agent of its own type."""
    return (
        CheckConstraint(f"agent_type = '{agent_type}'", name=f"ck_{table}_agent_type"),
        ForeignKeyConstraint(["id", "agent_type"], ["agent.id", "agent.agent_type"],
                             name=f"fk_{table}_agent", ondelete="CASCADE"),
    )


class AutomationModel(UserModel):
    """A script, pipeline or service account; display_name is its name."""
    __tablename__ = "automation"
    __versioned__: Dict = {"exclude": ["agent_type"]}

    id = child_agent_id()
    agent_type = child_agent_type('automation')

    __mapper_args__ = {
        "polymorphic_identity": "automation",
        "polymorphic_load": "selectin",
        "inherit_condition": id == UserModel.id,
    }
    __table_args__ = child_agent_constraints('automation', 'automation')
