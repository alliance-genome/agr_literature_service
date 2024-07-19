
from datetime import datetime
import pytz

from sqlalchemy import (Column, ForeignKey, DateTime)
from sqlalchemy.ext.declarative import declared_attr

from agr_literature_service.api.user import get_global_user_id


def get_default_user_value():
    uid = get_global_user_id()
    if uid is None:
        uid = "default_user"
    return uid


class AuditedModel(object):
    __tablename__ = "audited"
    # date created - timestamp
    # date updated - timestamp
    date_created = Column(
        DateTime,
        nullable=False,
        index=True,
        default=lambda: datetime.now(tz=pytz.timezone("UTC"))
    )

    date_updated = Column(
        DateTime,
        nullable=True,
        index=True,
        default=lambda: datetime.now(tz=pytz.timezone("UTC")),
        onupdate=lambda: datetime.now(tz=pytz.timezone("UTC"))
    )

    # created by - id from users table
    # updated by - id from users table

    @declared_attr
    def created_by(cls):
        return Column('created_by', ForeignKey('users.id'), default=get_default_user_value, nullable=True)

    @declared_attr
    def updated_by(cls):
        return Column('updated_by', ForeignKey('users.id'), default=get_default_user_value,
                      onupdate=get_default_user_value, nullable=True)


# Function to disable the `onupdate` behavior
def disable_set_updated_by_onupdate(target):
    target.__table__.columns['updated_by'].onupdate = None


# Function to enable the `onupdate` behavior
def enable_set_updated_by_onupdate(target):
    target.__table__.columns['updated_by'].onupdate = get_default_user_value
