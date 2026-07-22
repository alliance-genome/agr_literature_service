"""
resource_descriptor_cache.py
============================
Process-local, TTL'd in-memory cache of A-team resource descriptors.
A-team is the sole source of truth (no YAML fallback). A failed refresh keeps
the last-good snapshot; startup is fail-soft.
"""
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

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
