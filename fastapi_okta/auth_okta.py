import json
import logging
import os
import requests
from typing import Optional, Dict, List, Type
import urllib.parse

from fastapi import HTTPException, Depends, Security, Request
from fastapi.security import SecurityScopes, HTTPBearer, HTTPAuthorizationCredentials
from fastapi.security import OAuth2, OAuth2PasswordBearer, OAuth2AuthorizationCodeBearer, OpenIdConnect
from fastapi.openapi.models import OAuthFlows
from pydantic import BaseModel, Field, ValidationError
from jose import jwt  # type: ignore

okta_rule_namespace: str = os.getenv('OKTA_RULE_NAMESPACE', 'https://github.com/alliance-genome/agr_fastapi_okta')


class OktaUnauthenticatedException(HTTPException):
    def __init__(self, **kwargs):
        super().__init__(401, **kwargs)

class OktaUnauthorizedException(HTTPException):
    def __init__(self, **kwargs):
        super().__init__(403, **kwargs)

class HTTPOktaError(BaseModel):
    detail: str

unauthenticated_response: Dict = {401: {'model': HTTPOktaError}}
unauthorized_response: Dict = {403: {'model': HTTPOktaError}}
security_responses: Dict = {**unauthenticated_response, **unauthorized_response}


class OktaUser(BaseModel):
    id: str = Field(..., alias='cid')
    email: str = Field(..., alias='sub')
    # uid: Optional[str] = Field(None, alias='uid')    

class OktaHTTPBearer(HTTPBearer):
    async def __call__(self, request: Request):
        #logging.debug('Called Auth0HTTPBearer')
        return await super().__call__(request)

class OAuth2ImplicitBearer(OAuth2):
    def __init__(self,
                 authorizationUrl: str,
                 scopes: Dict[str, str]={},
                 scheme_name: Optional[str]=None,
                 auto_error: bool=True):
        flows = OAuthFlows(implicit={"authorizationUrl": authorizationUrl, 'scopes': scopes})
        super().__init__(flows=flows, scheme_name=scheme_name, auto_error=auto_error)

    async def __call__(self, request: Request) -> Optional[str]:
        # Overwrite parent call to prevent useless overhead, the actual auth is done in Auth0.get_user
        # This scheme is just for Swagger UI
        return None

    # TODO: figure out why Auth0HTTPBearer() sub-dependency gets called twice both from scheme dependency
    # in path op decorator and from Auth0.get_user dependency in path op function (fastapi injection system bug?)
    # async def __call__(self,
    #     request: Request,
    #     creds: HTTPAuthorizationCredentials = Depends(Auth0HTTPBearer())
    # ) -> Optional[str]:
    #     logging.debug('Called OAuth2ImplicitBearer')
    #     return creds.credentials


class Okta:
    def __init__(self, domain: str, api_audience: str, scopes: Dict[str, str]={},
                 auto_error: bool=True, scope_auto_error: bool=True, email_auto_error: bool=False,
                 oktauser_model: Type[OktaUser]=OktaUser):
        self.domain = domain
        self.audience = api_audience

        self.auto_error = auto_error
        self.scope_auto_error = scope_auto_error
        self.email_auto_error = email_auto_error

        self.okta_user_model = oktauser_model

        self.algorithms = ['RS256']
        self.jwks: Dict = requests.get(f'https://{domain}/v1/keys').json()


        #authorization_url_qs = urllib.parse.urlencode({"audience": api_audience})
        #authorization_url = f'https://{domain}/v1/authorize'
        #code = requests.get(authorization_url).json()
        #print(code)
        #self.implicit_scheme = OAuth2ImplicitBearer(
        #    authorizationUrl=authorization_url,
        #    scopes=scopes,
        #    scheme_name='Auth0ImplicitBearer')
        #self.password_scheme = OAuth2PasswordBearer(tokenUrl=f'https://{domain}/v1/token', scopes=scopes)
        #self.authcode_scheme = OAuth2AuthorizationCodeBearer(
        #    authorizationUrl=authorization_url,
        #    tokenUrl=f'https://{domain}/v1/token',
        #    scopes=scopes)
        #self.oidc_scheme = OpenIdConnect(openIdConnectUrl=f'https://{domain}/.well-known/openid-configuration')


    async def get_user(self,
                       security_scopes: SecurityScopes,
                       creds: Optional[HTTPAuthorizationCredentials] = Depends(OktaHTTPBearer(auto_error=False)),
                       ) -> Optional[OktaUser]:

        if creds is None:
            if self.auto_error:
                # See HTTPBearer from FastAPI:
                # latest - https://github.com/tiangolo/fastapi/blob/master/fastapi/security/http.py
                # 0.65.1 - https://github.com/tiangolo/fastapi/blob/aece74982d7c9c1acac98e2c872c4cb885677fc7/fastapi/security/http.py
                raise HTTPException(403, detail='Missing bearer token')  # must be 403 until solving https://github.com/tiangolo/fastapi/pull/2120
            else:
                return None
        #print(creds)
        token = creds.credentials
        payload: Dict = {}
        try:
            unverified_header = jwt.get_unverified_header(token)
            #print(unverified_header)
            rsa_key = {}
            for key in self.jwks['keys']:
                if key['kid'] == unverified_header['kid']:
                    rsa_key = {
                        'kty': key['kty'],
                        'kid': key['kid'],
                        'use': key['use'],
                        'n': key['n'],
                        'e': key['e']
                    }
            #print(rsa_key)
                    #break  # TODO: do we still need to iterate all keys after we found a match?
            if rsa_key:

                payload = jwt.decode(
                    token,
                    rsa_key,
                    algorithms=self.algorithms,
                    audience=self.audience,
                    issuer=f'https://{self.domain}'
                )
                #print(payload)
            else:
                if self.auto_error:
                    raise jwt.JWTError

        except jwt.ExpiredSignatureError:
            if self.auto_error:
                raise OktaUnauthenticatedException(detail='Expired token')
            else:
                return None

        except jwt.JWTClaimsError:
            if self.auto_error:
                raise OktaUnauthenticatedException(detail='Invalid token claims (please check issuer and audience)')
            else:
                return None

        except jwt.JWTError:
            if self.auto_error:
                raise OktaUnauthenticatedException(detail='Malformed token')
            else:
                return None

        except Exception as e:
            logging.error(f'Handled exception decoding token: "{e}"')
            if self.auto_error:
                raise OktaUnauthenticatedException(detail='Error decoding token')
            else:
                return None

        if self.scope_auto_error:
            token_scope_str: str = payload.get('scope', '')

            if isinstance(token_scope_str, str):
                token_scopes = token_scope_str.split()

                for scope in security_scopes.scopes:
                    if scope not in token_scopes:
                        raise OktaUnauthorizedException(detail=f'Missing "{scope}" scope',
                                                         headers={'WWW-Authenticate': f'Bearer scope="{security_scopes.scope_str}"'})
            else:
                # This is an unlikely case but handle it just to be safe (perhaps auth0 will change the scope format)
                raise OktaUnauthorizedException(detail='Token "scope" field must be a string')

        try:
            user = self.okta_user_model(**payload)

            if self.email_auto_error and not user.email:
                raise OktaUnauthorizedException(detail=f'Missing email claim (check okta rule "Add email to access token")')
            
            return user

        except ValidationError as e:
            logging.error(f'Handled exception parsing OktaUser: "{e}"')
            if self.auto_error:
                raise OktaUnauthorizedException(detail='Error parsing OktaUser')
            else:
                return None

        return None
