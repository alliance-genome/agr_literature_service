"""
resource_descriptor_cache.py
============================
Process-local, TTL'd in-memory cache of A-team resource descriptors.
A-team is the sole source of truth (no YAML fallback). A failed refresh keeps
the last-good snapshot; startup is fail-soft.
"""
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DescriptorPage:
    name: Optional[str]
    url: Optional[str]


@dataclass(frozen=True)
class ResourceDescriptor:
    db_prefix: str
    name: Optional[str] = None
    aliases: Optional[List[str]] = None
    default_url: Optional[str] = None
    pages: List[DescriptorPage] = field(default_factory=list)


def _first(rd: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        val = rd.get(key)
        if val is not None:
            return val
    return None


def _normalize_ateam_descriptor(rd: Dict[str, Any]) -> Optional[ResourceDescriptor]:
    db_prefix = _first(rd, "prefix", "db_prefix")
    if not db_prefix:
        return None
    pages_raw = _first(rd, "resourcePages", "pages") or []
    pages: List[DescriptorPage] = []
    for p in pages_raw:
        if isinstance(p, dict):
            pages.append(DescriptorPage(name=_first(p, "name"), url=_first(p, "urlTemplate", "url")))
    return ResourceDescriptor(
        db_prefix=db_prefix,
        name=_first(rd, "name", "fullName"),
        aliases=_first(rd, "aliases", "synonyms"),
        default_url=_first(rd, "defaultUrlTemplate", "default_url"),
        pages=pages,
    )


def _fetch_from_ateam() -> List[ResourceDescriptor]:
    from agr_curation_api import AGRCurationAPIClient  # type: ignore
    client = AGRCurationAPIClient()
    raw_descriptors = client.get_resource_descriptors() or []
    out: List[ResourceDescriptor] = []
    for rd in raw_descriptors:
        if isinstance(rd, dict):
            entry = _normalize_ateam_descriptor(rd)
            if entry is not None:
                out.append(entry)
    return out


DEFAULT_TTL_SECONDS = 900
RETRY_BACKOFF_SECONDS = 60


def _ttl() -> timedelta:
    return timedelta(seconds=int(os.getenv("ATEAM_FETCH_TTL_SECONDS", str(DEFAULT_TTL_SECONDS))))


@dataclass
class _State:
    snapshot: Optional[List[ResourceDescriptor]] = None
    fetched_at: Optional[datetime] = None


_state = _State()
_lock = threading.Lock()

# Injectable seams (overridden in tests).
_now: Callable[[], datetime] = datetime.now
_fetch: Callable[[], List[ResourceDescriptor]] = _fetch_from_ateam


def _do_fetch_locked(now: datetime) -> None:
    _state.snapshot = _fetch()
    _state.fetched_at = now


def ensure_fresh() -> None:
    now = _now()
    with _lock:
        if _state.snapshot is None:
            try:
                _do_fetch_locked(now)
            except Exception as e:  # noqa: BLE001
                logger.warning("Resource descriptor initial load failed; starting empty: %s", e)
                _state.snapshot = []
                _state.fetched_at = now - _ttl() + timedelta(seconds=RETRY_BACKOFF_SECONDS)
        elif _state.fetched_at is None or (now - _state.fetched_at) > _ttl():
            try:
                _do_fetch_locked(now)
            except Exception as e:  # noqa: BLE001
                logger.warning("Resource descriptor refresh failed; keeping previous data: %s", e)
                _state.fetched_at = now - _ttl() + timedelta(seconds=RETRY_BACKOFF_SECONDS)


def get_all() -> List[ResourceDescriptor]:
    ensure_fresh()
    return list(_state.snapshot or [])


def get_map(prefixes: Optional[Iterable[str]] = None) -> Dict[str, ResourceDescriptor]:
    all_rd = get_all()
    if prefixes is not None:
        want = set(prefixes)
        return {rd.db_prefix: rd for rd in all_rd if rd.db_prefix in want}
    return {rd.db_prefix: rd for rd in all_rd}


def force_refresh() -> List[ResourceDescriptor]:
    now = _now()
    with _lock:
        _do_fetch_locked(now)
    return list(_state.snapshot or [])


def load_initial() -> None:
    ensure_fresh()


def _seed(descriptors: List[ResourceDescriptor]) -> None:
    with _lock:
        _state.snapshot = list(descriptors)
        _state.fetched_at = _now()


def _reset() -> None:
    with _lock:
        _state.snapshot = None
        _state.fetched_at = None
