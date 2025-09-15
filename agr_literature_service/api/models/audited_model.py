from datetime import datetime
import pytz

from sqlalchemy import Column, ForeignKey, DateTime, event
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.inspection import inspect as sa_inspect
from agr_literature_service.api.user import get_global_user_id


def get_default_user_value():
    uid = get_global_user_id()
    if uid is None:
        uid = "default_user"
    return uid


class AuditedModel(object):
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
    def created_by(cls):
        return Column(
            'created_by',
            ForeignKey('users.id'),
            nullable=True,
        )

    @declared_attr
    def updated_by(cls):
        return Column(
            'updated_by',
            ForeignKey('users.id'),
            nullable=True,
        )


@event.listens_for(AuditedModel, "before_insert", propagate=True)
def _set_created_and_updated(mapper, connection, target):
    now = datetime.now(tz=pytz.timezone("UTC"))
    if target.date_created is None:
        target.date_created = now
    if target.date_updated is None:
        target.date_updated = now
    if target.created_by is None:
        target.created_by = get_default_user_value()
    if target.updated_by is None:
        target.updated_by = get_default_user_value()


@event.listens_for(AuditedModel, "before_update", propagate=True)
def _set_updated(mapper, connection, target):
    """
    Only auto-stamp fields that the caller didn't explicitly set.
    """
    now = datetime.now(tz=pytz.timezone("UTC"))
    state = sa_inspect(target)

    # If caller didn't touch date_updated, set it to now
    if not state.attrs.date_updated.history.has_changes():
        target.date_updated = now

    # If caller didn't touch updated_by, fill from global user (or default)
    if not state.attrs.updated_by.history.has_changes():
        uid = get_global_user_id() or get_default_user_value()
        target.updated_by = uid
