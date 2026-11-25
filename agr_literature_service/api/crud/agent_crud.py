"""Operations that change what an agent is (SCRUM-6995).

agent is the parent table of person and automation, which share its id. An automation
agent can stand in for a human who has no person record yet (a WBPerson placeholder, a
curator's login name); once the person exists, the automation is either promoted in
place (it becomes the person, keeping its id) or merged into the person's agent (its
records move to the person and it is deleted).
"""
import logging
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.orm import Session

from agr_literature_service.api.models import PersonModel, UserModel

logger = logging.getLogger(__name__)

AUDIT_ID_COLUMNS = ('created_by_id', 'updated_by_id')

_SQL_AUDITED_TABLES = text("""
    SELECT table_name, array_agg(column_name::text ORDER BY column_name)
    FROM information_schema.columns
    WHERE table_schema = 'public' AND column_name IN ('created_by_id', 'updated_by_id')
      AND table_name NOT LIKE 'ateam\\_%'
    GROUP BY table_name
    ORDER BY table_name
""")


def _agent_type(db: Session, agent_id: int) -> Any:
    return db.execute(text("SELECT agent_type FROM agent WHERE id = :i"), {"i": agent_id}).scalar()


def _forget(db: Session, agent_id: int) -> None:
    """Drop the session's copy of the agent: its class or existence changed under it."""
    instance = db.identity_map.get(db.identity_key(UserModel, (agent_id,)))
    if instance is not None:
        db.expunge(instance)


def merge_agent_into(db: Session, extra_id: int, keeper_id: int) -> Dict[str, int]:
    """Attribute everything the automation agent extra_id recorded to the agent keeper_id
    and delete extra_id: created_by_id / updated_by_id on every audited table and history
    table, and transaction.user_id. Returns the rows rewritten per table (only tables with
    any). Not committed.

    It rewrites every audited table, history included, so it takes minutes on a full
    database: run it from a script, never inside an API request.

    Raises ValueError when extra_id is not an automation agent (merging a person would
    delete the person with its child rows) or keeper_id is not an agent.
    """
    if extra_id == keeper_id:
        raise ValueError(f"cannot merge agent {extra_id} into itself")
    if _agent_type(db, extra_id) != 'automation':
        raise ValueError(f"agent {extra_id} is not an automation agent")
    if _agent_type(db, keeper_id) is None:
        raise ValueError(f"agent {keeper_id} does not exist")
    db.flush()
    params = {"extra": extra_id, "keeper": keeper_id}
    counts: Dict[str, int] = {}
    for table, columns in db.execute(_SQL_AUDITED_TABLES).all():
        columns = [column for column in columns if column in AUDIT_ID_COLUMNS]
        assignments = ', '.join(
            f'{column} = CASE WHEN {column} = :extra THEN :keeper ELSE {column} END' for column in columns)
        condition = ' OR '.join(f'{column} = :extra' for column in columns)
        rewritten = db.execute(text(f'UPDATE {table} SET {assignments} WHERE {condition}'), params).rowcount
        if rewritten:
            counts[table] = rewritten
    rewritten = db.execute(text('UPDATE "transaction" SET user_id = :keeper WHERE user_id = :extra'),
                           params).rowcount
    if rewritten:
        counts['transaction'] = rewritten
    _forget(db, extra_id)
    db.execute(text("DELETE FROM agent WHERE id = :extra"), params)  # its automation row cascades
    db.expire_all()
    logger.info("agent %s merged into agent %s: %s", extra_id, keeper_id, counts)
    return counts


def promote_automation_to_person(db: Session, agent_id: int, display_name: str, curie: str,
                                 **person_columns: Any) -> PersonModel:
    """Turn the automation agent agent_id into a person with the same id, so its records
    show the person without being rewritten: the automation row goes, the agent becomes
    agent_type 'person' named display_name, and a person row (curie plus person_columns,
    e.g. mod_roles) is added. Not committed; the session's copy of the agent is dropped.

    Raises ValueError when agent_id is not an automation agent or a person_columns key
    is not a person column.
    """
    if _agent_type(db, agent_id) != 'automation':
        raise ValueError(f"agent {agent_id} is not an automation agent")
    person_table_columns = {column.name for column in PersonModel.__table__.columns}
    unknown = sorted(set(person_columns) - person_table_columns)
    if unknown:
        raise ValueError(f"not person columns: {unknown}")
    db.flush()
    _forget(db, agent_id)
    db.execute(text("DELETE FROM automation WHERE id = :i"), {"i": agent_id})
    db.execute(text("UPDATE agent SET agent_type = 'person', display_name = :name WHERE id = :i"),
               {"i": agent_id, "name": display_name})
    columns: List[str] = ['id', 'curie'] + sorted(person_columns)
    values = {'id': agent_id, 'curie': curie, **person_columns}
    db.execute(text(f"INSERT INTO person ({', '.join(columns)}) "
                    f"VALUES ({', '.join(':' + column for column in columns)})"), values)
    db.expire_all()
    logger.info("automation agent %s promoted to person %s", agent_id, curie)
    return db.get(PersonModel, agent_id)
