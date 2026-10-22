from datetime import datetime
import pytz

from sqlalchemy import BigInteger, Boolean, Column, ForeignKey, DateTime, event, false, select
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.orm import Session, column_property, deferred
from sqlalchemy.orm.attributes import set_committed_value
from agr_literature_service.api.models.user_model import agent_label_expression
from agr_literature_service.api.user import get_current_agent_id, get_global_user_id, resolve_agent_id

# Internal flag to track if we should skip auto-updating updated_by and date_updated
# Used during validation operations to prevent changing these fields
_SKIP_AUTO_UPDATE_FLAG = '_skip_audit_auto_update'


def get_default_user_value():
    uid = get_global_user_id()
    if uid is None:
        uid = "default_user"
    return uid


def impute_audit_user_ids(created_by, updated_by, default_user):
    """Fill in created_by / updated_by the same way ``before_insert`` does:

    1. If only one of (created_by, updated_by) is set, copy it to the other.
    2. If either side is still None, fall back to ``default_user``.

    Returns the resolved ``(created_by, updated_by)`` tuple. Callers that need
    to predict what an INSERT would store — most importantly the TET
    duplicate-detection filter — must use this helper so they don't drift
    out of sync with ``before_insert`` (see SCRUM-5716 / Gillian's bug
    report: a Path-A vs Path-B mismatch mislabeled a duplicate POST as
    ``reason="different_creator"``).
    """
    if created_by is not None and updated_by is None:
        updated_by = created_by
    elif updated_by is not None and created_by is None:
        created_by = updated_by
    if created_by is None:
        created_by = default_user
    if updated_by is None:
        updated_by = default_user
    return created_by, updated_by


def disable_set_updated_by_onupdate(target):
    """
    Disable automatic update of updated_by field for the target object.
    Used during validation to prevent changing the updated_by field.
    """
    setattr(target, _SKIP_AUTO_UPDATE_FLAG, True)


def enable_set_updated_by_onupdate(target):
    """
    Re-enable automatic update of updated_by field for the target object.
    """
    if hasattr(target, _SKIP_AUTO_UPDATE_FLAG):
        delattr(target, _SKIP_AUTO_UPDATE_FLAG)


def disable_set_date_updated_onupdate(target):
    """
    Disable automatic update of date_updated field for the target object.
    Used during validation to prevent changing the date_updated field.
    Note: This uses the same flag as disable_set_updated_by_onupdate since
    both fields should be disabled/enabled together during validation.
    """
    setattr(target, _SKIP_AUTO_UPDATE_FLAG, True)


def enable_date_updated_onupdate(target):
    """
    Re-enable automatic update of date_updated field for the target object.
    """
    if hasattr(target, _SKIP_AUTO_UPDATE_FLAG):
        delattr(target, _SKIP_AUTO_UPDATE_FLAG)


class AuditedModel(object):
    """The LinkML AuditedObject columns (agr_curation_schema core.yaml), SCRUM-6719.

    created_by_id / updated_by_id hold agent.id. created_by / updated_by are the
    agents' labels (automation name or person curie), read through a correlated
    subquery: callers keep reading and assigning strings, and the listeners below
    turn an assigned string into an agent id.

    The LinkML-only columns are deferred, so a loaded object carries the same
    attributes as before -- jsonable_encoder(obj) builds many API responses, and
    their schemas forbid extra fields.
    """
    __tablename__ = "audited"

    date_created = Column(
        DateTime,
        nullable=False,
        index=True,
    )

    date_updated = Column(
        DateTime,
        nullable=True,
        index=True,
    )

    @declared_attr
    def created_by_id(cls):
        return deferred(Column(BigInteger, ForeignKey('agent.id', name=f'fk_{cls.__tablename__}_created_by_id'[:63]),
                               nullable=True))

    @declared_attr
    def updated_by_id(cls):
        return deferred(Column(BigInteger, ForeignKey('agent.id', name=f'fk_{cls.__tablename__}_updated_by_id'[:63]),
                               nullable=True))

    @declared_attr
    def created_by(cls):
        return column_property(agent_label_expression(cls.created_by_id))

    @declared_attr
    def updated_by(cls):
        return column_property(agent_label_expression(cls.updated_by_id))

    @declared_attr
    def db_date_created(cls):
        return deferred(Column(DateTime, nullable=True))

    @declared_attr
    def db_date_updated(cls):
        return deferred(Column(DateTime, nullable=True))

    @declared_attr
    def internal(cls):
        return deferred(Column(Boolean, nullable=False, default=False, server_default=false()))

    @declared_attr
    def obsolete(cls):
        # the tables that had is_obsolete map their own column onto obsolete
        if 'is_obsolete' in cls.__dict__:
            return None
        return deferred(Column(Boolean, nullable=False, default=False, server_default=false()))


def agent_id_for_label(connection, label):
    """agent.id for a label; the current user's id is already known."""
    if label is None:
        return None
    if label == get_global_user_id():
        agent_id = get_current_agent_id()
        if agent_id is not None:
            return agent_id
    return resolve_agent_id(connection, label)


def _assigned(target, attribute):
    """The label assigned to created_by / updated_by since the object was loaded, if any."""
    history = sa_inspect(target).attrs[attribute].history
    return history.added[0] if history.added else None


@event.listens_for(AuditedModel, "before_insert", propagate=True)
def _set_created_and_updated(mapper, connection, target):
    # If either date is set but not both, set both to the same value
    if target.date_created is not None and target.date_updated is None:
        target.date_updated = target.date_created
    elif target.date_updated is not None and target.date_created is None:
        target.date_created = target.date_updated

    now = datetime.now(tz=pytz.timezone("UTC"))
    if target.date_created is None:
        target.date_created = now
    if target.date_updated is None:
        target.date_updated = now

    target.db_date_created = target.db_date_updated = now

    created_by, updated_by = impute_audit_user_ids(
        _assigned(target, 'created_by'), _assigned(target, 'updated_by'), get_default_user_value()
    )
    # Resolve the labels to agent ids, auto-creating an agent for an explicitly
    # supplied "created by"/"updated by" name (e.g. from an admin-token tag load)
    # so it does not violate the foreign key. The current user's id is already
    # known (it is resolved at request setup in set_global_user_*), which keeps
    # the lookup -- and the agent.id sequence burn of ON CONFLICT -- off the hot
    # path for normal traffic.
    if target.created_by_id is None:
        target.created_by_id = agent_id_for_label(connection, created_by)
    if target.updated_by_id is None:
        target.updated_by_id = agent_id_for_label(connection, updated_by)


@event.listens_for(AuditedModel, "before_update", propagate=True)
def _set_updated(mapper, connection, target):
    """
    Only auto-stamp fields that the caller didn't explicitly set.
    Skip auto-updating if the skip flag is set (e.g., during validation operations).
    """
    now = datetime.now(tz=pytz.timezone("UTC"))
    state = sa_inspect(target)
    # LinkML db_date_updated records the row change itself, validation included
    target.db_date_updated = now

    # Check if auto-update should be skipped (e.g., during validation)
    if hasattr(target, _SKIP_AUTO_UPDATE_FLAG) and getattr(target, _SKIP_AUTO_UPDATE_FLAG):
        _persist_assigned_labels(connection, target)
        return

    # If caller didn't touch date_updated, set it to now
    if not state.attrs.date_updated.history.has_changes():
        target.date_updated = now

    # If caller didn't touch updated_by, fill from global user (or default)
    updated_by = _assigned(target, 'updated_by')
    if updated_by is None and not state.attrs.updated_by_id.history.has_changes():
        updated_by = get_global_user_id() or get_default_user_value()
    # Resolve the (possibly caller-supplied) labels to agent ids before the
    # UPDATE hits the FK constraint, auto-creating unknown ones.
    if updated_by is not None:
        target.updated_by_id = agent_id_for_label(connection, updated_by)
    created_by = _assigned(target, 'created_by')
    if created_by is not None:
        target.created_by_id = agent_id_for_label(connection, created_by)


@event.listens_for(Session, "before_flush")
def _load_deferred_audit_columns(session, _flush_context, _instances):
    """Load the deferred LinkML columns of audited rows about to be updated or deleted.

    Continuum copies every versioned column of a changed row into its history row
    after the flush. A deferred column it reads would be loaded with one query per
    column per row, and on a DELETE -- the row already gone -- recorded as NULL. One
    query per class here, before the statements run, loads them all.
    """
    pending = {}
    for obj in list(session.dirty) + list(session.deleted):
        if not isinstance(obj, AuditedModel):
            continue
        state = sa_inspect(obj)
        if state.key is None:
            continue
        keys = [prop.key for prop in state.mapper.column_attrs if prop.deferred and prop.key in state.unloaded]
        if keys:
            pending.setdefault(state.mapper, []).append((obj, keys))
    for mapper, objects in pending.items():
        deferred_props = [prop for prop in mapper.column_attrs if prop.deferred]
        pk = mapper.primary_key[0]
        ids = [sa_inspect(obj).identity[0] for obj, _keys in objects]
        with session.no_autoflush:
            rows = session.execute(
                select(pk, *[prop.columns[0] for prop in deferred_props]).where(pk.in_(ids))).all()
        values = {row[0]: dict(zip((prop.key for prop in deferred_props), row[1:])) for row in rows}
        for obj, keys in objects:
            row = values.get(sa_inspect(obj).identity[0])
            if row is not None:
                for key in keys:
                    set_committed_value(obj, key, row[key])


def _persist_assigned_labels(connection, target):
    """Store explicitly assigned created_by / updated_by labels without auto-stamping."""
    for label_attribute, id_attribute in (('created_by', 'created_by_id'), ('updated_by', 'updated_by_id')):
        label = _assigned(target, label_attribute)
        if label is not None:
            setattr(target, id_attribute, agent_id_for_label(connection, label))


def audited_columns_sql(model, alias: str) -> str:
    """Raw-SQL select list standing in for <alias>.* on an audited table.

    Rows come back keyed the way the ORM keys the model -- topic_entity_tag_id,
    is_obsolete, and created_by / updated_by as agent labels -- without the deferred
    LinkML columns, so code and API responses built from such rows keep their keys.
    """
    parts = [f'{alias}."{prop.columns[0].name}" AS {prop.key}'
             for prop in sa_inspect(model).column_attrs
             if prop.key not in ('created_by', 'updated_by') and not prop.deferred]
    for key in ('created_by', 'updated_by'):
        parts.append(f'(SELECT COALESCE(a.automation_username, p.curie) FROM agent a '
                     f'LEFT JOIN person p ON p.id = a.person_id WHERE a.id = {alias}.{key}_id) AS {key}')
    return ', '.join(parts)
