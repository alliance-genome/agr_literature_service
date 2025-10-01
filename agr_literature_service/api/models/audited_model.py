from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Column, DateTime, String, ForeignKey, event
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.inspection import inspect as sa_inspect

from agr_literature_service.api.user import get_global_user_id

try:
    # Optional helper if non-ISO formats arrive
    from dateutil import parser as dateutil_parser  # type: ignore
except Exception:  # pragma: no cover
    dateutil_parser = None


# -------- helpers --------

def get_default_user_value() -> str:
    return get_global_user_id() or "default_user"


def _parse_to_utc(value: Optional[datetime | str]) -> Optional[datetime]:
    """
    Accept datetime | str | None and return tz-aware UTC datetime (or None).
    - Treat naive datetimes as UTC.
    - Handles 'YYYY-MM-DD HH:MM:SS(.ffffff)' and ISO-8601 strings.
    - Falls back to python-dateutil if available.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        s = value.strip()
        if "T" not in s and " " in s:
            s = s.replace(" ", "T", 1)
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            if dateutil_parser is None:
                raise ValueError(f"Unrecognized datetime format: {value!r}")
            dt = dateutil_parser.parse(value)
    else:
        raise TypeError(f"Unsupported datetime value type: {type(value)}")

    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


# ------------------------------ mixin ------------------------------

class AuditedModel(object):
    """
    Mixin for created/updated timestamps and users.

    Semantics:
      - INSERT:
          * date_created := provided (UTC) or now(UTC)
          * date_updated := provided (UTC) or date_created
          * created_by   := provided or current user or 'default_user'
          * updated_by   := provided or created_by
          * Mark instance to skip stamping on the *very next* UPDATE
            (covers post-insert normalization performed by CRUD).
      - UPDATE:
          * If this is the first UPDATE after INSERT (flag set): do not stamp.
            Normalize any explicitly-set date_updated and then clear the flag.
          * Otherwise:
              - If date_updated not explicitly changed -> stamp now(UTC)
              - Else normalize to UTC
              - If updated_by not explicitly changed -> stamp current user or default
    """
    __tablename__ = "audited"  # subclasses have their own tables

    # tz-aware timestamps (Postgres TIMESTAMPTZ)
    date_created = Column(DateTime(timezone=True), nullable=False, index=True)
    date_updated = Column(DateTime(timezone=True), nullable=True, index=True)

    @declared_attr
    def created_by(cls):
        # users.id is TEXT
        return Column(String, ForeignKey("users.id"), nullable=True)

    @declared_attr
    def updated_by(cls):
        # users.id is TEXT
        return Column(String, ForeignKey("users.id"), nullable=True)


# ------------------------------ listeners ------------------------------

@event.listens_for(AuditedModel, "before_insert", propagate=True)
def _audit_before_insert(mapper, connection, target):
    now = datetime.now(timezone.utc)

    # Normalize any provided values
    dc = _parse_to_utc(getattr(target, "date_created", None)) or now
    du = _parse_to_utc(getattr(target, "date_updated", None)) or dc

    cb = getattr(target, "created_by", None) or get_default_user_value()
    ub = getattr(target, "updated_by", None) or cb

    target.date_created = dc
    target.date_updated = du
    target.created_by = cb
    target.updated_by = ub

    # One-shot: skip stamping on the very next UPDATE after this INSERT
    # setattr(target, "_audit_skip_next_update", True)
    sa_inspect(target).info['audit_skip_next_update'] = True


@event.listens_for(AuditedModel, "before_update", propagate=True)
def _audit_before_update(mapper, connection, target):
    state = sa_inspect(target)

    # If this is the immediate post-insert UPDATE, preserve audit values
    if state.info.pop('audit_skip_next_update', None):
        if state.attrs.date_updated.history.has_changes():
            target.date_updated = _parse_to_utc(target.date_updated)
        return

    now = datetime.now(timezone.utc)

    # date_updated: stamp now if not explicitly changed; else normalize
    if state.attrs.date_updated.history.has_changes():
        target.date_updated = _parse_to_utc(target.date_updated)
    else:
        target.date_updated = now

    # updated_by: stamp current user if not explicitly changed
    if not state.attrs.updated_by.history.has_changes():
        target.updated_by = get_default_user_value()
