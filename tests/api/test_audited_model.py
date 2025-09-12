# generated with AI help
from datetime import datetime, timedelta
from typing import Optional

import pytz
import pytest
from sqlalchemy import Column, Integer, String

from agr_literature_service.api.database.base import Base
from agr_literature_service.api.models.audited_model import AuditedModel
from agr_literature_service.api.models.user_model import UserModel
from agr_literature_service.api.user import (
    set_global_user_id,
    get_global_user_id,
)

from ..fixtures import db  # noqa: F401


class AuditedDummy(Base, AuditedModel):
    __tablename__ = "audited_dummy"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)


def _utc_now():
    return datetime.now(tz=pytz.timezone("UTC"))


def _is_recent(dt: datetime, seconds: int = 5) -> bool:
    return abs((_utc_now() - dt).total_seconds()) < seconds


def _ensure_user(db, uid: str): # noqa
    if uid is None:
        return
    if not db.query(UserModel).filter_by(id=uid).one_or_none():
        db.add(UserModel(id=uid))
        db.commit()


@pytest.fixture(scope="module", autouse=True)
def _create_tables(db): # noqa
    """Create the table and ensure required users exist for FK checks."""
    Base.metadata.create_all(bind=db.get_bind(), tables=[AuditedDummy.__table__])

    for uid in ("default_user", "OTTO", "MANUAL_CREATOR"):
        _ensure_user(db, uid)

    yield

    Base.metadata.drop_all(bind=db.get_bind(), tables=[AuditedDummy.__table__])


@pytest.fixture(autouse=True)
def _reset_global_user(db): # noqa
    """Reset global user between tests to avoid leakage."""
    prev: Optional[str] = get_global_user_id()

    try:
        set_global_user_id(db, None)  # type: ignore[arg-type]
    except Exception:
        set_global_user_id(db, "default_user")

    yield

    if prev is None:
        try:
            set_global_user_id(db, None)  # type: ignore[arg-type]
        except Exception:
            set_global_user_id(db, "default_user")
    else:
        set_global_user_id(db, prev)


def test_insert_autostamps_when_no_global_user(db): # noqa
    """Insert with no fields set -> stamps dates and default user."""
    obj = AuditedDummy(name="alpha")
    db.add(obj)
    db.commit()
    db.refresh(obj)

    assert isinstance(obj.date_created, datetime)
    assert isinstance(obj.date_updated, datetime)
    assert _is_recent(obj.date_created)
    assert _is_recent(obj.date_updated)
    assert obj.created_by == "default_user"
    assert obj.updated_by == "default_user"


def test_insert_respects_explicit_created_fields(db): # noqa
    """Explicit created fields on insert are kept; updated fields auto-stamped."""
    manual_created = _utc_now() - timedelta(days=10)
    _ensure_user(db, "MANUAL_CREATOR")

    obj = AuditedDummy(
        name="bravo",
        date_created=manual_created,
        created_by="MANUAL_CREATOR",
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)

    assert obj.date_created == manual_created
    assert obj.created_by == "MANUAL_CREATOR"
    assert isinstance(obj.date_updated, datetime)
    assert _is_recent(obj.date_updated)
    assert obj.updated_by == "default_user"


def test_update_overwrites_to_now_and_global_user(db): # noqa
    """
    Current listener behavior on UPDATE:
      - date_updated := now
      - updated_by   := get_default_user_value() (global user or 'default_user')
    """
    obj = AuditedDummy(name="charlie")
    db.add(obj)
    db.commit()
    db.refresh(obj)

    prev_updated = obj.date_updated

    _ensure_user(db, "OTTO")
    set_global_user_id(db, "OTTO")

    obj.name = "charlie-2"
    db.add(obj)
    db.commit()
    db.refresh(obj)

    assert obj.updated_by == "OTTO"
    assert obj.date_updated >= prev_updated
    assert _is_recent(obj.date_updated)


def test_update_overwrites_even_if_manual_values_set(db): # noqa
    """Manual pre-flush values get clobbered by the current before_update listener."""
    obj = AuditedDummy(name="delta")
    db.add(obj)
    db.commit()
    db.refresh(obj)

    manual_dt = _utc_now() - timedelta(days=2)
    _ensure_user(db, "MANUAL_CREATOR")
    obj.date_updated = manual_dt
    obj.updated_by = "MANUAL_CREATOR"
    obj.name = "delta-2"

    _ensure_user(db, "OTTO")
    set_global_user_id(db, "OTTO")

    db.add(obj)
    db.commit()
    db.refresh(obj)

    assert obj.updated_by == "OTTO"
    assert obj.date_updated != manual_dt
    assert _is_recent(obj.date_updated)


def test_update_uses_default_user_when_global_unset(db): # noqa
    """If global user is unset, updated_by falls back to 'default_user'."""
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
