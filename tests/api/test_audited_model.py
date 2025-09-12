# tests/unit/test_audited_model.py
from datetime import datetime, timedelta
from typing import Generator, Optional

import pytz
import pytest
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import clear_mappers

# Import your project's Base and user helpers
from agr_literature_service.api.models import Base
from agr_literature_service.api.user import (
    set_global_user_id,
    get_global_user_id,
)
from agr_literature_service.api.models.audited_model import AuditedModel


# --- Test model that inherits the audited mixin --------------------------------

class AuditedDummy(Base, AuditedModel):
    __tablename__ = "audited_dummy"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)


# --- Fixtures -------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def _create_tables(db) -> Generator[None, None, None]:
    """Create the temporary table for this test module and drop it afterwards."""
    # Ensure metadata knows about our model
    Base.metadata.create_all(bind=db.get_bind(), tables=[AuditedDummy.__table__])
    try:
        yield
    finally:
        Base.metadata.drop_all(bind=db.get_bind(), tables=[AuditedDummy.__table__])
        clear_mappers()  # be tidy for other tests


@pytest.fixture(autouse=True)
def _reset_global_user(db) -> Generator[None, None, None]:
    """Reset global user before/after each test to avoid cross-test leakage."""
    # Save current user
    prev: Optional[str] = get_global_user_id()
    # Clear to force default behavior unless tests set it
    set_global_user_id(db, None)  # type: ignore[arg-type]
    try:
        yield
    finally:
        # Restore
        if prev is not None:
            set_global_user_id(db, prev)  # type: ignore[arg-type]
        else:
            set_global_user_id(db, None)  # type: ignore[arg-type]


# --- Helpers --------------------------------------------------------------------

def _utc_now():
    return datetime.now(tz=pytz.timezone("UTC"))


def _is_recent(dt: datetime, seconds: int = 5) -> bool:
    return abs((_utc_now() - dt).total_seconds()) < seconds


# --- Tests ----------------------------------------------------------------------

def test_insert_autostamps_when_no_global_user(db):
    """Insert with no fields set -> stamps both dates and default user."""
    obj = AuditedDummy(name="alpha")
    db.add(obj)
    db.commit()
    db.refresh(obj)

    assert isinstance(obj.date_created, datetime)
    assert isinstance(obj.date_updated, datetime)
    assert _is_recent(obj.date_created)
    assert _is_recent(obj.date_updated)

    # Because get_global_user_id() is None -> get_default_user_value() = "default_user"
    assert obj.created_by == "default_user"
    assert obj.updated_by == "default_user"


def test_insert_respects_explicit_created_fields(db):
    """Explicit created fields on insert should NOT be overwritten by before_insert."""
    manual_created = _utc_now() - timedelta(days=10)
    obj = AuditedDummy(
        name="bravo",
        date_created=manual_created,
        created_by="MANUAL_CREATOR",
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)

    # date_created/created_by should remain as explicitly set
    assert obj.date_created == manual_created
    assert obj.created_by == "MANUAL_CREATOR"

    # date_updated/updated_by should still be auto-stamped on insert
    assert isinstance(obj.date_updated, datetime)
    assert _is_recent(obj.date_updated)
    assert obj.updated_by == "default_user"


def test_update_overwrites_to_now_and_global_user(db):
    """
    With the current audited listener, any UPDATE sets:
      - date_updated := now
      - updated_by   := get_default_user_value() (global user or 'default_user')
    """
    # Arrange: create row first
    obj = AuditedDummy(name="charlie")
    db.add(obj)
    db.commit()
    db.refresh(obj)

    prev_updated = obj.date_updated

    # Set the global user to OTTO so the listener uses it on update
    set_global_user_id(db, "OTTO")

    # Act: update any field
    obj.name = "charlie-2"
    db.add(obj)
    db.commit()
    db.refresh(obj)

    # Assert: overwritten by listener
    assert obj.updated_by == "OTTO"
    assert isinstance(obj.date_updated, datetime)
    assert obj.date_updated >= prev_updated
    assert _is_recent(obj.date_updated)


def test_update_overwrites_even_if_manual_values_set(db):
    """
    Current behavior: manual assignments BEFORE flush will be overwritten by the listener.
    """
    obj = AuditedDummy(name="delta")
    db.add(obj)
    db.commit()
    db.refresh(obj)

    # Manually set old values prior to update; they should be clobbered
    manual_dt = _utc_now() - timedelta(days=2)
    obj.date_updated = manual_dt
    obj.updated_by = "MANUAL_USER"
    obj.name = "delta-2"

    # Set global user so listener picks it
    set_global_user_id(db, "OTTO")

    db.add(obj)
    db.commit()
    db.refresh(obj)

    assert obj.updated_by == "OTTO"              # manual value overwritten
    assert obj.date_updated != manual_dt         # manual value overwritten
    assert _is_recent(obj.date_updated)          # set to ~now


def test_update_uses_default_user_when_global_unset(db):
    """
    If global user is not set, updated_by should fall back to 'default_user'.
    """
    obj = AuditedDummy(name="echo")
    db.add(obj)
    db.commit()
    db.refresh(obj)

    obj.name = "echo-2"
    db.add(obj)
    db.commit()
    db.refresh(obj)

    assert obj.updated_by == "default_user"
    assert _is_recent(obj.date_updated)
