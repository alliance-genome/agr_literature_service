from typing import List

from agr_literature_service.api import resource_descriptor_cache


def update() -> List[resource_descriptor_cache.ResourceDescriptor]:
    """Force-refresh this worker's in-memory descriptor cache from A-team."""
    return resource_descriptor_cache.force_refresh()


def show() -> List[resource_descriptor_cache.ResourceDescriptor]:
    return resource_descriptor_cache.get_all()
