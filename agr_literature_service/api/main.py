"""Main FastAPI application module."""
import argparse
import logging
import sys
import time
from os import environ
from typing import Any, Dict

import uvicorn

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi_health import health

from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_NOPASS
from agr_literature_service.api.database.main import is_database_online
from agr_literature_service.api.routers import (author_router, bulk_downloads_router,
                                                cross_reference_router, curation_status_router,
                                                database_router, editor_router,
                                                indexing_priority_router, mesh_detail_router,
                                                mod_reference_type_router, mod_router,
                                                mod_corpus_association_router,
                                                reference_relation_router,
                                                reference_router, resource_descriptor_router,
                                                resource_router, search_router, sort_router,
                                                workflow_tag_router, topic_entity_tag_router,
                                                referencefile_router, referencefile_mod_router,
                                                copyright_license_router, check_router,
                                                dataset_router, ml_model_router,
                                                manual_indexing_tag_router, person_router,
                                                person_cross_reference_router, email_router,
                                                person_setting_router, ontology_router,
                                                authentication)

TITLE = "Alliance Literature Service"
VERSION = "0.1.0"
DESCRIPTION = "This service provides access to the Alliance Bibliographic Corpus and metadata"


app = FastAPI(title=TITLE,
              version=VERSION,
              description=DESCRIPTION,
              swagger_ui_parameters={
                  "defaultModelsExpandDepth": -1,  # Hide schemas section by default
                  "docExpansion": "none",  # Collapse all endpoints by default
                  "filter": True,  # Add search/filter box
                  "tryItOutEnabled": True,  # Enable "Try it out" by default
                  "syntaxHighlight.theme": "monokai",  # Better syntax highlighting
                  "displayRequestDuration": True,  # Show request duration
              })

app.add_middleware(CORSMiddleware,
                   allow_credentials=True,
                   allow_origins=["*"],
                   allow_methods=["*"],
                   allow_headers=["*"])


# Add request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all HTTP requests with timing information."""
    import os
    start_time = time.time()

    # Process the request
    response = await call_next(request)

    # Calculate duration
    process_time = time.time() - start_time

    # Check if we should log based on LOG_LEVEL
    log_level = environ.get('LOG_LEVEL', 'info').lower()
    should_log = log_level in ('debug', 'info')

    if should_log:
        # Log the request - use print to ensure it goes to stdout
        log_message = (
            f'[PID:{os.getpid()}] {request.client.host}:{request.client.port} - '
            f'"{request.method} {request.url.path}" '
            f'{response.status_code} '
            f'- {process_time:.3f}s'
        )
        print(log_message, flush=True)

        # Also try with logging
        logger = logging.getLogger("uvicorn.access")
        logger.info(log_message)

    return response


def custom_openapi() -> Dict[str, Any]:
    """

    :return:
    """

    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=TITLE,
        version=VERSION,
        description=DESCRIPTION,
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.include_router(resource_router.router)
app.include_router(reference_router.router)
app.include_router(author_router.router)
app.include_router(editor_router.router)
app.include_router(cross_reference_router.router)
app.include_router(resource_descriptor_router.router)
app.include_router(mesh_detail_router.router)
app.include_router(mod_reference_type_router.router)
app.include_router(database_router.router)
app.include_router(reference_relation_router.router)
app.include_router(bulk_downloads_router.router)
app.include_router(mod_router.router)
app.include_router(mod_corpus_association_router.router)
app.include_router(search_router.router)
app.include_router(sort_router.router)
app.include_router(workflow_tag_router.router)
app.include_router(topic_entity_tag_router.router)
app.include_router(referencefile_router.router)
app.include_router(referencefile_mod_router.router)
app.include_router(copyright_license_router.router)
app.include_router(check_router.router)
app.include_router(dataset_router.router)
app.include_router(ml_model_router.router)
app.include_router(curation_status_router.router)
app.include_router(indexing_priority_router.router)
app.include_router(manual_indexing_tag_router.router)
app.include_router(person_router.router)
app.include_router(email_router.router)
app.include_router(person_cross_reference_router.router)
app.include_router(person_setting_router.router)
app.include_router(ontology_router.router)
app.include_router(authentication.router)

app.add_api_route("/health", health([is_database_online]))

app.openapi = custom_openapi  # type: ignore


def check_key_envs():  # pragma: no cover
    """Check that all required environment variables are set."""
    env_to_check = [
        'API_PORT', 'API_SERVER', 'XML_PATH', 'AWS_SECRET_ACCESS_KEY',
        'AWS_ACCESS_KEY_ID', 'ENV_STATE',
        'PSQL_USERNAME', 'PSQL_PASSWORD', 'PSQL_HOST', 'PSQL_PORT', 'PSQL_DATABASE',
        'RESOURCE_DESCRIPTOR_URL', 'HOST', 'ATEAM_API_URL',
        'COGNITO_REGION', 'COGNITO_USER_POOL_ID', 'COGNITO_CLIENT_ID'
    ]
    okay_to_continue = True
    for key in env_to_check:
        value = environ.get(key, "")
        if not value:
            okay_to_continue = False
            logging.error("Environment variable %s has no value or is blank", key)
    if not okay_to_continue:
        logging.error("Exiting initialisation. Please set all envs anf try again.")
    return okay_to_continue


def run(parsed_args):  # pragma: no cover
    """Run the FastAPI application.

    :param parsed_args: Parsed command-line arguments
    """

    if not check_key_envs():
        sys.exit(-1)

    # May put back but for now do not see way to have multiple formats
    #  using the logging.basicConfig
    # LOGGING_CONFIG["formatters"]["default"]["fmt"] = \
    #     "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    # LOGGING_CONFIG["formatters"]["access"]["fmt"] = \
    #     '%(asctime)s [%(name)s] %(levelprefix)s %(client_addr)s - ' \
    #     '"%(request_line)s" %(status_code)s'
    print(f"run: Database details are {SQLALCHEMY_DATABASE_NOPASS}")
    state = environ.get('ENV_STATE')
    if state == 'test':
        log_level = logging.DEBUG
    else:
        log_level = logging.WARNING
    uvicorn.run("main:app",
                port=parsed_args['port'],
                host=parsed_args['ip_address'],
                timeout_keep_alive=5001,
                log_level=log_level)


if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int,
                        help='Port to run the server on',
                        default=8080, nargs='?')
    parser.add_argument('-i', '--ip-address', type=str,
                        help='IP address of the host',
                        default='0.0.0.0', nargs='?')
    parser.add_argument('-v', dest='verbose', action='store_true')

    args = vars(parser.parse_args())
    run(args)
