from fastapi import APIRouter, Depends
from fastapi_okta import Okta
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from types import SimpleNamespace
import logging

from agr_literature_service.api.config import config

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Authentication"])

# HTTP Bearer scheme for extracting the Authorization header
bearer_scheme = HTTPBearer()
BEARER_DEP = Depends(bearer_scheme)  # <- avoids B008


class _DummyOkta:
    """Fallback stub when real Okta isn't configured – returns a dummy user."""
    def get_user(
        self,
        credentials: HTTPAuthorizationCredentials = BEARER_DEP
    ):
        # Use the raw token as uid/cid or hard‑code a test user
        return SimpleNamespace(
            uid=credentials.credentials,
            cid=credentials.credentials,
            email=None
        )


# Start with the dummy stub
auth: Okta | _DummyOkta = _DummyOkta()
try:
    real_okta = Okta(
        domain=config.OKTA_DOMAIN,
        api_audience=config.OKTA_API_AUDIENCE
    )
    auth = real_okta
except Exception as e:
    logger.error(f"Authentication initialization failed: {e}")
