from fastapi import APIRouter
from fastapi_okta import Okta

from agr_literature_service.api.config import config

router = APIRouter(tags=['Authentication'])

auth = Okta(domain=config.OKTA_DOMAIN,
            api_audience=config.OKTA_API_AUDIENCE
            )
