from fastapi import APIRouter
#from fastapi_okta import Okta
#from literature.okta_auth0 import Okta
from fastapi_okta import Okta
from literature.config import config

router = APIRouter(tags=['Authentication'])

auth = Okta(domain=config.OKTA_DOMAIN,
            api_audience=config.OKTA_API_AUDIENCE
            )
