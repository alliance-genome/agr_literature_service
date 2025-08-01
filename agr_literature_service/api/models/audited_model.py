from datetime import datetime
import pytz

from sqlalchemy import Column, ForeignKey, DateTime, event
from sqlalchemy.ext.declarative import declared_attr

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
    target.date_created = now
    target.date_updated = now
    target.created_by = get_default_user_value()
    target.updated_by = get_default_user_value()


@event.listens_for(AuditedModel, "before_update", propagate=True)
def _set_updated(mapper, connection, target):
    now = datetime.now(tz=pytz.timezone("UTC"))
    target.date_updated = now
    target.updated_by = get_default_user_value()
