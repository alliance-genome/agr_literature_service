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

DEFAULT_TTL_SECONDS = 900
DEFAULT_TIMEOUT_SECONDS = 5
RETRY_BACKOFF_SECONDS = 60


def _int_env(name: str, default: int) -> int:
    """Read an int env var, falling back to default when unset, blank, or invalid."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %d", name, raw, default)
        return default


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
    timeout_seconds = _int_env("ATEAM_FETCH_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
    client = AGRCurationAPIClient(config={"timeout": timedelta(seconds=timeout_seconds), "max_retries": 1})
    raw_descriptors = client.get_resource_descriptors() or []
    out: List[ResourceDescriptor] = []
    for rd in raw_descriptors:
        if isinstance(rd, dict):
            entry = _normalize_ateam_descriptor(rd)
            if entry is not None:
                out.append(entry)
    return out


def _ttl() -> timedelta:
    return timedelta(seconds=_int_env("ATEAM_FETCH_TTL_SECONDS", DEFAULT_TTL_SECONDS))


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
    global _map_cache, _map_cache_stamp
    fetched = _fetch()
    if not fetched:
        raise ValueError("A-team returned no resource descriptors")
    _state.snapshot = fetched
    _state.fetched_at = now
    _map_cache, _map_cache_stamp = None, None


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


def is_absolute_url(value: Optional[str]) -> bool:
    return isinstance(value, str) and value.startswith(("http://", "https://"))


# Memoised prefix->descriptor map. get_map() rebuilds from a fresh list copy on
# every call, and the xref schemas call it once per cross reference, so an
# unbounded person/laboratory name search paid a copy plus a dict comprehension
# over every descriptor per xref. Rebuilt only when the snapshot changes.
_map_cache: Optional[Dict[str, "ResourceDescriptor"]] = None
_map_cache_stamp: Optional[datetime] = None


def full_prefix_map() -> Dict[str, "ResourceDescriptor"]:
    """Prefix->descriptor map for the current snapshot, memoised.

    The returned dict is the live cache -- callers must not mutate it.

    Deliberately lock-free: this runs once per cross reference during
    serialization, and that lock is the contention the memo exists to remove.
    Correctness rests on capturing the stamp BEFORE building from the snapshot.
    Read the other way round, a refresh committing between the two would store
    the pre-refresh map under the post-refresh stamp, and every later call would
    accept it -- serving a stale map, silently, until the next refresh (a 900s
    TTL). Captured first, a losing thread instead stores a stale map under a
    stale stamp, which the next call rejects and rebuilds: duplicated work
    rather than a wrong answer.
    """
    global _map_cache, _map_cache_stamp
    ensure_fresh()
    cached, cached_stamp = _map_cache, _map_cache_stamp
    if cached is not None and cached_stamp == _state.fetched_at:
        return cached
    stamp = _state.fetched_at
    built = {rd.db_prefix: rd for rd in (_state.snapshot or [])}
    _map_cache, _map_cache_stamp = built, stamp
    return built


def resolve_xref_urls(
    curie: str,
    page_names: Optional[Iterable[str]] = None,
    descriptor_map: Optional[Dict[str, ResourceDescriptor]] = None,
) -> "tuple[Optional[str], Optional[List[Dict[str, Optional[str]]]]]":
    """Resolve a cross-reference curie against the A-team resource descriptors.

    Returns ``(default_url, pages)`` where ``pages`` is a list of
    ``{"name": ..., "url": ...}`` dicts, or None when no page names were given.
    The descriptor templates carry a ``[%s]`` placeholder for the local id.

    `descriptor_map` lets a caller that is already resolving a batch of curies
    pass the map it built once (see cross_reference_crud.show_from_curies), so
    sharing this helper does not turn one lookup into one per curie. Callers
    handling a single record can omit it.

    Everything here is best-effort: an unknown prefix, a descriptor with no
    default_url, a page name the descriptor does not define, or a malformed
    curie all yield a None url rather than an error, because a missing link must
    never fail the request that was only trying to show a record.
    """
    names = list(page_names) if page_names is not None else None
    # Most page entries are descriptor page NAMES, resolved against the
    # descriptor's templates below. The SGD person loader instead writes the
    # colleague's absolute obj_url into this column (see
    # lit_processing/oneoff_scripts/load_sgd_colleagues.py), and that URL is the
    # only link those rows carry -- so an already-absolute entry is its own url.
    # Done here, before the prefix/descriptor early returns, so it still applies
    # when the prefix has no descriptor at all.
    pages: Optional[List[Dict[str, Optional[str]]]] = (
        [{"name": n, "url": n if is_absolute_url(n) else None} for n in names]
        if names is not None else None
    )

    prefix, _, local_id = (curie or "").partition(":")
    if not prefix or not local_id:
        return None, pages

    lookup = descriptor_map if descriptor_map is not None else full_prefix_map()
    descriptor = lookup.get(prefix)
    if descriptor is None:
        return None, pages

    default_url = (
        descriptor.default_url.replace("[%s]", local_id) if descriptor.default_url else None
    )

    if pages is not None:
        by_name = {p.name: p.url for p in descriptor.pages}
        for page in pages:
            template = by_name.get(page["name"])
            if template:
                page["url"] = template.replace("[%s]", local_id)

    return default_url, pages


def force_refresh() -> List[ResourceDescriptor]:
    now = _now()
    with _lock:
        _do_fetch_locked(now)
    return list(_state.snapshot or [])


def load_initial() -> None:
    ensure_fresh()


def _seed(descriptors: List[ResourceDescriptor]) -> None:
    global _map_cache, _map_cache_stamp
    with _lock:
        _state.snapshot = list(descriptors)
        _state.fetched_at = _now()
        _map_cache, _map_cache_stamp = None, None


def _reset() -> None:
    global _map_cache, _map_cache_stamp
    with _lock:
        _state.snapshot = None
        _state.fetched_at = None
        _map_cache, _map_cache_stamp = None, None
