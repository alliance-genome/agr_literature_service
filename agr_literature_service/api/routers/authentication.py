from fastapi import APIRouter
from fastapi_okta import Okta

from agr_literature_service.api.config import config
import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=['Authentication'])

try:
    auth = {}
    auth = Okta(domain=config.OKTA_DOMAIN,
                api_audience=config.OKTA_API_AUDIENCE
                )
except Exception as e:
    logger.error(f"Authentication failed: Exception {e}")
