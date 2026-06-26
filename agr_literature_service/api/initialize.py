# api/initialize.py

import sys
import urllib.request
from urllib.error import URLError
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional
import yaml
import logging
from sqlalchemy.orm import Session

from agr_literature_service.api.config import config
from agr_literature_service.api.models.resource_descriptor_models import (
    ResourceDescriptorModel, ResourceDescriptorPageModel
)
from agr_literature_service.api.database.main import get_db
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import (
    create_postgres_session
)

logger = logging.getLogger(__name__)

# start with a DB session you can override in initialize_database()
db_session: Session = create_postgres_session(False)


def initialize_database():
    global db_session
    # grab the first session from the FastAPI dependency generator
    db_session = next(get_db(), None)


def _first(rd: Dict[str, Any], *keys: str) -> Any:
    """Return the first present, non-null value among the given keys."""
    for key in keys:
        val = rd.get(key)
        if val is not None:
            return val
    return None


def _normalize_ateam_descriptor(rd: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Map a single A-Team resource-descriptor object to the canonical shape
    expected by ``_load_descriptors_into_db`` (the same shape the agr_schemas
    YAML produces): keys are ResourceDescriptor model columns plus ``pages``
    as a list of ``{"name", "url"}`` dicts.

    A-Team field names are confirmed against the agr_curation source
    (ResourceDescriptor / ResourceDescriptorPage entities, SCRUM-5992):
        prefix -> db_prefix, name -> name, synonyms -> aliases,
        idExample -> example_gid, idPattern -> gid_pattern,
        defaultUrlTemplate -> default_url,
        resourcePages[{name, urlTemplate}] -> pages[{name, url}].

    IMPORTANT: these fields (synonyms, idPattern, idExample, resourcePages) are
    NOT in the default ``ForPublic`` JSON view, so the client must request the
    findForPublic endpoint with ``view=ResourceDescriptorView`` (see
    ``_fetch_descriptors_from_ateam``). The canonical YAML keys are kept as
    fallbacks so this stays correct regardless of source quirks.
    """
    db_prefix = _first(rd, "prefix", "db_prefix")
    if not db_prefix:
        # db_prefix is required (unique, NOT NULL); skip unusable entries.
        return None

    pages_raw = _first(rd, "resourcePages", "pages") or []
    pages: List[Dict[str, Any]] = []
    for p in pages_raw:
        if not isinstance(p, dict):
            continue
        pages.append({
            "name": _first(p, "name"),
            "url": _first(p, "urlTemplate", "url"),
        })

    normalized: Dict[str, Any] = {
        "db_prefix": db_prefix,
        "name": _first(rd, "name", "fullName"),
        "aliases": _first(rd, "aliases", "synonyms"),
        "example_gid": _first(rd, "idExample", "example_gid", "example_id"),
        "gid_pattern": _first(rd, "idPattern", "gid_pattern"),
        "default_url": _first(rd, "defaultUrlTemplate", "default_url"),
        "pages": pages,
    }
    return normalized


def _fetch_descriptors_from_ateam() -> List[Dict[str, Any]]:
    """
    Fetch resource descriptors from the A-Team curation API via the shared
    ``agr_curation_api`` client and normalize them to the canonical shape.

    The client's ``get_resource_descriptors()`` must call
    ``POST /api/resourcedescriptor/findForPublic`` with
    ``view=ResourceDescriptorView`` (the default ``ForPublic`` view omits
    resourcePages / synonyms / idPattern / idExample that ABC needs) and a
    ``limit`` large enough to return all descriptors in one page.

    Raises on any failure (import/attribute/API error) so the caller can fall
    back to the YAML source.
    """
    # Lazy import so module import never depends on the client being present.
    from agr_curation_api import AGRCurationAPIClient  # type: ignore

    client = AGRCurationAPIClient()
    raw_descriptors = client.get_resource_descriptors() or []

    normalized: List[Dict[str, Any]] = []
    for rd in raw_descriptors:
        if not isinstance(rd, dict):
            continue
        entry = _normalize_ateam_descriptor(rd)
        if entry is not None:
            normalized.append(entry)
    return normalized


def _fetch_descriptors_from_yaml() -> List[Dict[str, Any]]:
    """
    Fetch resource descriptors from the GitHub YAML at RESOURCE_DESCRIPTOR_URL.
    This is the fallback source during the transition to the A-Team SOT.
    """
    raw = config.RESOURCE_DESCRIPTOR_URL or ""
    # strip whitespace and any angle-brackets or quotes
    url = raw.strip().strip("<>'\" ")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        logger.error(
            f"Invalid RESOURCE_DESCRIPTOR_URL, must start with http:// or https:// — got: {raw!r}"
        )
        sys.exit(-1)

    try:
        with urllib.request.urlopen(url) as resp:
            # read + decode before passing to yaml
            body = resp.read().decode("utf-8")
            return yaml.full_load(body)
    except URLError as e:
        logger.error(f"Could not fetch resource descriptor from {url}: {e}")
        sys.exit(-1)
    except Exception as e:
        logger.error(f"Unable to process resource_descriptor '{url}': {e}")
        sys.exit(-1)


def _load_descriptors_into_db(db: Session, descriptors: List[Dict[str, Any]]) -> None:
    """
    Replace the ResourceDescriptorModel + ResourceDescriptorPageModel tables
    with the given list of (canonical-shape) descriptor dicts.
    """
    # clear out old descriptors
    db.query(ResourceDescriptorModel).delete()

    for rd in descriptors:
        data: Dict[str, Any] = {}
        pages = []
        for key, val in rd.items():
            if key == "pages":
                for p in val:
                    page = ResourceDescriptorPageModel(
                        name=p.get("name"),
                        url=p.get("url")
                    )
                    db.add(page)
                    pages.append(page)
                data["pages"] = pages
            elif key == "example_id":
                data["example_gid"] = val
            else:
                data[key] = val

        obj = ResourceDescriptorModel(**data)
        db.add(obj)

    db.commit()


def update_resource_descriptor(db: Session = None):
    """
    Reload the ResourceDescriptorModel + ResourceDescriptorPageModel tables.

    The A-Team curation API is the source of truth; fetch from there first and
    fall back to the agr_schemas YAML (RESOURCE_DESCRIPTOR_URL) if the A-Team
    fetch fails or returns nothing.
    """
    if db is None:
        db = db_session

    try:
        descriptors = _fetch_descriptors_from_ateam()
        if not descriptors:
            raise ValueError("A-Team returned no resource descriptors")
        logger.info("Loaded %d resource descriptors from A-Team", len(descriptors))
    except Exception as e:
        logger.warning(
            "A-Team resource descriptor fetch failed (%s); falling back to YAML", e
        )
        descriptors = _fetch_descriptors_from_yaml()

    _load_descriptors_into_db(db, descriptors)
    return descriptors


def setup_resource_descriptor():
    initialize_database()
    update_resource_descriptor()


if __name__ == '__main__':
    setup_resource_descriptor()
