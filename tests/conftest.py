import pytest

import agr_literature_service.api.resource_descriptor_cache as _rdc


@pytest.fixture(autouse=True)
def _isolate_resource_descriptor_cache(monkeypatch):
    """Keep the process-global descriptor cache hermetic: never call A-team in
    tests, and reset between tests. Tests that need descriptors call
    resource_descriptor_cache._seed([...])."""
    _rdc._reset()
    monkeypatch.setattr(_rdc, "_fetch", lambda: [])
    yield
    _rdc._reset()
