
from datetime import datetime
import pytz

from sqlalchemy import (Column, ForeignKey, DateTime)
# from agr_literature_service.api.database.base import Base
from sqlalchemy.ext.declarative import declared_attr


class AuditedModel(object):
    __tablename__ = "audited"
    # date created - timestamp
    # date updated - timestamp
    date_created = Column(
        DateTime,
        nullable=False,
        default=datetime.now(tz=pytz.timezone("UTC"))
    )

    date_updated = Column(
        DateTime,
        nullable=True
    )

    # created by - id from users table
    # updated by - id from users table
    # created_by = Column(
    #    String,
    #    ForeignKey("users.id"),
    #    nullable=False
    # )
    @declared_attr
    def created_by(cls):
        return Column('created_by', ForeignKey('users.id'))

    @declared_attr
    def updated_by(cls):
        return Column('updated_by', ForeignKey('users.id'))
    # updated_by = Column(
    #     String,
    #    ForeignKey("users.id"),
    #    nullable=True
    # )
