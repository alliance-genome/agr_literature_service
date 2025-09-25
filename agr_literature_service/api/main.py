from os import environ
import argparse
from typing import Any, Dict
import logging
import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi_health import health

from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_NOPASS
from agr_literature_service.api.database.main import is_database_online
from agr_literature_service.api.database.setup import setup_database
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
                                                manual_indexing_tag_router)

title = "Alliance Literature Service"
version = "0.1.0"
description = "This service provides access to the Alliance Bibliographic Corpus and metadata"

app = FastAPI(title=title,
              version=version,
              description=description)

app.add_middleware(CORSMiddleware,
                   allow_credentials=True,
                   allow_origins=["*"],
                   allow_methods=["*"],
                   allow_headers=["*"])


def custom_openapi() -> Dict[str, Any]:
    """

    :return:
    """

    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=title,
        version=version,
        description=description,
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


@app.on_event('startup')
def init_db():
    setup_database()


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

app.add_api_route("/health", health([is_database_online]))

app.openapi = custom_openapi  # type: ignore


def check_key_envs():
    env_to_check = [
        'API_PORT', 'API_SERVER', 'XML_PATH', 'AWS_SECRET_ACCESS_KEY',
        'AWS_ACCESS_KEY_ID', 'OKTA_CLIENT_ID', 'OKTA_CLIENT_SECRET', 'ENV_STATE',
        'PSQL_USERNAME', 'PSQL_PASSWORD', 'PSQL_HOST', 'PSQL_PORT', 'PSQL_DATABASE',
        'RESOURCE_DESCRIPTOR_URL', 'HOST', 'OKTA_DOMAIN', 'OKTA_API_AUDIENCE', 'ATEAM_API_URL'
    ]
    okay_to_continue = True
    for key in env_to_check:
        value = environ.get(key, "")
        if not value:
            okay_to_continue = False
            logging.error(f"Environment variable {key} has no value or is blank")
    if not okay_to_continue:
        logging.error("Exiting initialisation. Please set all envs anf try again.")
    return okay_to_continue


def run():
    """

    :return:
    """

    if not check_key_envs():
        exit(-1)

    # May put back but for now do not see way to have multiple formats
    #  using the logging.basicConfig
    # LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    # LOGGING_CONFIG["formatters"]["access"]["fmt"] = '%(asctime)s [%(name)s] %(levelprefix)s %(client_addr)s - ' \
    #                                                 '"%(request_line)s" %(status_code)s'
    print(f"run: Database details are {SQLALCHEMY_DATABASE_NOPASS}")
    state = environ.get('ENV_STATE')
    if state == 'test':
        log_level = logging.DEBUG
    else:
        log_level = logging.WARNING
    uvicorn.run("main:app",
                port=args['port'],
                host=args['ip_address'],
                timeout_keep_alive=5001,
                log_level=log_level)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help='Port to run the server on', default=8080, nargs='?')
    parser.add_argument('-i', '--ip-address', type=str, help='IP address of the host', default='0.0.0.0', nargs='?')
    parser.add_argument('-v', dest='verbose', action='store_true')

    args = vars(parser.parse_args())
    run()
