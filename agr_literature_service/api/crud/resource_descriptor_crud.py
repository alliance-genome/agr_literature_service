import logging
from typing import List

from fastapi import HTTPException, status

from agr_literature_service.api import resource_descriptor_cache

logger = logging.getLogger(__name__)


def update() -> List[resource_descriptor_cache.ResourceDescriptor]:
    """Force-refresh this worker's in-memory descriptor cache from A-team.

    On failure the previous (last-good) snapshot is retained by the cache; we
    surface a 502 rather than a raw 500 so the caller sees a clear message.
    """
    try:
        return resource_descriptor_cache.force_refresh()
    except Exception as e:  # noqa: BLE001
        logger.warning("PUT /resource_descriptor force-refresh failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=("Failed to refresh resource descriptors from the A-team API; "
                    "the previously loaded descriptors are still in effect."),
        )


def show() -> List[resource_descriptor_cache.ResourceDescriptor]:
    return resource_descriptor_cache.get_all()
