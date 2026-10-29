import logging
from datetime import datetime, timedelta
from unittest import mock

import pytest
from fastapi import HTTPException

import agr_literature_service.api.resource_descriptor_cache as rdc
from agr_literature_service.api.crud import resource_descriptor_crud

from ..fixtures import db  # noqa: F401
from .fixtures import descriptor_rows  # noqa: F401


ATEAM_SAMPLE = [
    {
        "prefix": "TESTMOD",
        "name": "Test Mod",
        "synonyms": ["TM", "TMOD"],
        "idExample": "TESTMOD:123",
        "idPattern": r"^TESTMOD:\d+$",
        "defaultUrlTemplate": "http://test.org/[%s]",
        "resourcePages": [
            {"name": "gene", "urlTemplate": "http://test.org/gene/[%s]"},
            {"name": "homepage", "urlTemplate": "http://test.org/"},
        ],
    }
]


def test_normalize_maps_ateam_fields():
    rd = rdc._normalize_ateam_descriptor(ATEAM_SAMPLE[0])
    assert rd == rdc.ResourceDescriptor(
        db_prefix="TESTMOD",
        name="Test Mod",
        aliases=["TM", "TMOD"],
        default_url="http://test.org/[%s]",
        pages=[
            rdc.DescriptorPage(name="gene", url="http://test.org/gene/[%s]"),
            rdc.DescriptorPage(name="homepage", url="http://test.org/"),
        ],
    )


def test_normalize_skips_without_prefix():
    assert rdc._normalize_ateam_descriptor({"name": "no prefix"}) is None


def test_fetch_from_db_reads_ateam_tables(descriptor_rows):  # noqa: F811
    result = rdc._fetch_from_db()
    assert [r.db_prefix for r in result] == ["TESTMOD"]   # the obsolete descriptor is left out
    assert result[0] == rdc._normalize_ateam_descriptor(ATEAM_SAMPLE[0])
    assert [p.name for p in result[0].pages] == ["gene", "homepage"]   # the internal page is left out


@pytest.fixture(autouse=True)
def _reset_cache():
    rdc._reset()
    yield
    rdc._reset()


def _fake_clock(start):
    state = {"now": start}

    def now():
        return state["now"]

    def advance(seconds):
        state["now"] = state["now"] + timedelta(seconds=seconds)

    return now, advance


def _rd(prefix, url):
    return rdc.ResourceDescriptor(db_prefix=prefix, default_url=url)


def test_initial_load_populates(monkeypatch):
    now, _ = _fake_clock(datetime(2026, 1, 1))
    monkeypatch.setattr(rdc, "_now", now)
    monkeypatch.setattr(rdc, "_fetch", lambda: [_rd("MGI", "u1")])
    assert rdc.get_map()["MGI"].default_url == "u1"


def test_within_ttl_does_not_refetch(monkeypatch):
    now, advance = _fake_clock(datetime(2026, 1, 1))
    calls = {"n": 0}

    def fetch():
        calls["n"] += 1
        return [_rd("MGI", "u%d" % calls["n"])]

    monkeypatch.setattr(rdc, "_now", now)
    monkeypatch.setattr(rdc, "_fetch", fetch)
    rdc.get_all()          # load
    advance(60)            # < TTL (900s default)
    rdc.get_all()
    assert calls["n"] == 1


def test_after_ttl_refetches(monkeypatch):
    now, advance = _fake_clock(datetime(2026, 1, 1))
    calls = {"n": 0}

    def fetch():
        calls["n"] += 1
        return [_rd("MGI", "u%d" % calls["n"])]

    monkeypatch.setattr(rdc, "_now", now)
    monkeypatch.setattr(rdc, "_fetch", fetch)
    rdc.get_all()
    advance(1000)          # > TTL
    assert rdc.get_map()["MGI"].default_url == "u2"


def test_refresh_failure_keeps_last_good(monkeypatch):
    now, advance = _fake_clock(datetime(2026, 1, 1))
    state = {"fail": False}

    def fetch():
        if state["fail"]:
            raise RuntimeError("ateam down")
        return [_rd("MGI", "good")]

    monkeypatch.setattr(rdc, "_now", now)
    monkeypatch.setattr(rdc, "_fetch", fetch)
    rdc.get_all()          # good load
    state["fail"] = True
    advance(1000)          # > TTL -> refresh attempted, fails
    assert rdc.get_map()["MGI"].default_url == "good"   # last-good retained


def test_initial_load_failure_is_fail_soft(monkeypatch):
    now, _ = _fake_clock(datetime(2026, 1, 1))
    monkeypatch.setattr(rdc, "_now", now)
    monkeypatch.setattr(rdc, "_fetch", mock.Mock(side_effect=RuntimeError("down")))
    assert rdc.get_all() == []          # empty, no exception
    assert rdc.get_map() == {}


def test_empty_cache_retries_every_read_and_rate_limits_the_error(monkeypatch, caplog):
    now, advance = _fake_clock(datetime(2026, 1, 1))
    state = {"fail": True, "calls": 0}

    def fetch():
        state["calls"] += 1
        if state["fail"]:
            raise RuntimeError("down")
        return [_rd("MGI", "good")]

    monkeypatch.setattr(rdc, "_now", now)
    monkeypatch.setattr(rdc, "_fetch", fetch)
    with caplog.at_level(logging.WARNING, logger=rdc.__name__):
        rdc.get_all()                                     # initial load fails: ERROR
        advance(1)
        rdc.get_all()                                     # retried at once, error not repeated
        advance(rdc.EMPTY_ERROR_LOG_SECONDS)
        rdc.get_all()                                     # still failing a minute later: ERROR again
    assert state["calls"] == 3
    assert [r.levelno for r in caplog.records] == [logging.ERROR, logging.ERROR]
    state["fail"] = False
    advance(1)
    assert rdc.get_map()["MGI"].default_url == "good"   # recovers on the next read


def test_refresh_failure_with_data_logs_warning(monkeypatch, caplog):
    now, advance = _fake_clock(datetime(2026, 1, 1))
    state = {"fail": False}

    def fetch():
        if state["fail"]:
            raise RuntimeError("down")
        return [_rd("MGI", "good")]

    monkeypatch.setattr(rdc, "_now", now)
    monkeypatch.setattr(rdc, "_fetch", fetch)
    rdc.get_all()
    state["fail"] = True
    advance(1000)
    with caplog.at_level(logging.WARNING, logger=rdc.__name__):
        rdc.get_all()
    assert [r.levelno for r in caplog.records] == [logging.WARNING]


def test_force_refresh_replaces_immediately(monkeypatch):
    now, _ = _fake_clock(datetime(2026, 1, 1))
    seq = iter([[_rd("MGI", "u1")], [_rd("MGI", "u2")]])
    monkeypatch.setattr(rdc, "_now", now)
    monkeypatch.setattr(rdc, "_fetch", lambda: next(seq))
    rdc.get_all()
    result = rdc.force_refresh()
    assert result[0].default_url == "u2"


def test_refresh_empty_result_keeps_last_good(monkeypatch):
    now, advance = _fake_clock(datetime(2026, 1, 1))
    state = {"empty": False}

    def fetch():
        if state["empty"]:
            return []
        return [_rd("MGI", "good")]

    monkeypatch.setattr(rdc, "_now", now)
    monkeypatch.setattr(rdc, "_fetch", fetch)
    rdc.get_all()          # good load
    state["empty"] = True
    advance(1000)          # > TTL -> refresh attempted, returns empty
    assert rdc.get_map()["MGI"].default_url == "good"   # last-good retained


def test_crud_update_returns_data_on_success(monkeypatch):
    monkeypatch.setattr(rdc, "_fetch", lambda: [_rd("MGI", "u1")])
    result = resource_descriptor_crud.update()
    assert [r.db_prefix for r in result] == ["MGI"]


def test_crud_update_raises_503_when_the_read_fails(monkeypatch):
    def boom():
        raise RuntimeError("database down")

    monkeypatch.setattr(rdc, "_fetch", boom)
    with pytest.raises(HTTPException) as exc_info:
        resource_descriptor_crud.update()
    assert exc_info.value.status_code == 503


def test_int_env_falls_back_on_unset_blank_and_invalid(monkeypatch):
    monkeypatch.delenv("ATEAM_FETCH_TTL_SECONDS", raising=False)
    assert rdc._int_env("ATEAM_FETCH_TTL_SECONDS", 900) == 900
    for bad in ("", "   ", "notanint"):
        monkeypatch.setenv("ATEAM_FETCH_TTL_SECONDS", bad)
        assert rdc._int_env("ATEAM_FETCH_TTL_SECONDS", 900) == 900
    monkeypatch.setenv("ATEAM_FETCH_TTL_SECONDS", "120")
    assert rdc._int_env("ATEAM_FETCH_TTL_SECONDS", 900) == 120


def test_ttl_reads_env(monkeypatch):
    monkeypatch.setenv("ATEAM_FETCH_TTL_SECONDS", "120")
    assert rdc._ttl() == timedelta(seconds=120)
    monkeypatch.setenv("ATEAM_FETCH_TTL_SECONDS", "")   # blank -> default
    assert rdc._ttl() == timedelta(seconds=900)
