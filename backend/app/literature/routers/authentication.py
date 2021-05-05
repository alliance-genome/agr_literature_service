from fastapi import APIRouter
from fastapi_auth0 import Auth0

from literature.config import config

router = APIRouter(tags=['Authentication'])

auth = Auth0(domain=config.AUTH0_DOMAIN,
             api_audience=config.AUTH0_API_AUDIENCE,
             scopes={'read:metadata': ''})
