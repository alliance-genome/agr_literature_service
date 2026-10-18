from sqlalchemy import Column, Integer, String, ForeignKey, CheckConstraint, column, func, select, table
from sqlalchemy.orm import relationship
from agr_literature_service.api.database.base import Base


_AGENT = table('agent', column('id'), column('automation_username'), column('person_id'))
_PERSON = table('person', column('id'), column('curie'))


class UserModel(Base):
    """An agent: a person (curator) or an automation (script, pipeline, service account).

    Its label -- automation_username, or the curie of its person -- is what created_by /
    updated_by show; the audit columns themselves hold agent.id (SCRUM-6719).
    """
    __tablename__ = "agent"

    user_id = Column('id', Integer, primary_key=True, autoincrement=True)
    automation_username = Column(String, nullable=True, index=True, unique=True)

    person_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"), nullable=True, index=True)

    # Relationship to PersonModel
    person = relationship(
        "PersonModel",
        foreign_keys=[person_id]
    )

    __table_args__ = (
        CheckConstraint(
            "(person_id IS NULL) <> (automation_username IS NULL)",
            name="ck_agent_exactly_one_of_person_or_automation"
        ),
    )

    def __str__(self) -> str:
        who = self.automation_username or (self.person.display_name if self.person else None) or "unknown"
        return f"User<{who}>"


def agent_label_expression(agent_id_column):
    """Correlated scalar subquery: the label of the agent whose id is agent_id_column.

    Built on lightweight table clauses rather than the models: it is evaluated while
    the audited models -- PersonModel among them -- are being declared. The aliases
    keep it from correlating to the outer row when the audited table is person.
    """
    agent = _AGENT.alias('label_agent')
    person = _PERSON.alias('label_person')
    return (
        select(func.coalesce(agent.c.automation_username, person.c.curie))
        .select_from(agent.outerjoin(person, person.c.id == agent.c.person_id))
        .where(agent.c.id == agent_id_column)
        .correlate_except(agent, person)
        .scalar_subquery()
    )
