from fastapi import APIRouter
from agr_cognito_py import CognitoAuth, CognitoConfig

import logging

logger = logging.getLogger(__name__)

router = APIRouter(tags=['Authentication'])

try:
    cognito_config = CognitoConfig()
    auth = CognitoAuth(cognito_config)
except Exception as e:
    logger.error(f"Authentication failed: Exception {e}")
