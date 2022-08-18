
from datetime import datetime
import pytz

from sqlalchemy import (Column, ForeignKey, DateTime, String)
# from agr_literature_service.api.database.base import Base
from sqlalchemy.ext.declarative import declared_attr

from agr_literature_service.api.user import get_global_user_id


class AuditedModel(object):
    __tablename__ = "audited"
    # date created - timestamp
    # date updated - timestamp
    date_created = Column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(tz=pytz.timezone("UTC"))
    )

    date_updated = Column(
        DateTime,
        nullable=True,
        default=lambda: datetime.now(tz=pytz.timezone("UTC"))
    )

    # created by - id from users table
    # updated by - id from users table

    @declared_attr
    def created_by(cls):
        return Column('created_by', ForeignKey('users.id'), default=get_global_user_id, nullable=True)

    @declared_attr
    def updated_by(cls):
        return Column('updated_by', ForeignKey('users.id'), default=get_global_user_id, nullable=True)
