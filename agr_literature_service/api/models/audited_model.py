from datetime import datetime
import pytz

from sqlalchemy import Column, ForeignKey, DateTime, event
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.inspection import inspect as sa_inspect
from agr_literature_service.api.user import get_global_user_id

# Internal flag to track if we should skip auto-updating updated_by and date_updated
# Used during validation operations to prevent changing these fields
_SKIP_AUTO_UPDATE_FLAG = '_skip_audit_auto_update'


def get_default_user_value():
    uid = get_global_user_id()
    if uid is None:
        uid = "default_user"
    return uid


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

    # If either user is set but not both, set both to the same value
    if target.created_by is not None and target.updated_by is None:
        target.updated_by = target.created_by
    elif target.updated_by is not None and target.created_by is None:
        target.created_by = target.updated_by

    if target.created_by is None:
        target.created_by = get_default_user_value()
    if target.updated_by is None:
        target.updated_by = get_default_user_value()


@event.listens_for(AuditedModel, "before_update", propagate=True)
def _set_updated(mapper, connection, target):
    """
    Only auto-stamp fields that the caller didn't explicitly set.
    Skip auto-updating if the skip flag is set (e.g., during validation operations).
    """
    # Check if auto-update should be skipped (e.g., during validation)
    if hasattr(target, _SKIP_AUTO_UPDATE_FLAG) and getattr(target, _SKIP_AUTO_UPDATE_FLAG):
        return

    now = datetime.now(tz=pytz.timezone("UTC"))
    state = sa_inspect(target)

    # If caller didn't touch date_updated, set it to now
    if not state.attrs.date_updated.history.has_changes():
        target.date_updated = now

    # If caller didn't touch updated_by, fill from global user (or default)
    if not state.attrs.updated_by.history.has_changes():
        uid = get_global_user_id() or get_default_user_value()
        target.updated_by = uid
