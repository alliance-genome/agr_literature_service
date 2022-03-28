from os import environ
import argparse
from typing import Any, Dict
import logging
import uvicorn

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi_health import health
# from uvicorn.config import LOGGING_CONFIG

from initialize import setup_resource_descriptor
from literature import models

# from literature.config import config
from literature.database.config import SQLALCHEMY_DATABASE_URL
from literature.database.main import engine, is_database_online
from literature.routers import (author_router, bulk_downloads_router,
                                cross_reference_router, database_router,
                                editor_router, file_router, mesh_detail_router,
                                mod_reference_type_router, note_router,
                                person_router,
                                reference_automated_term_tag_router,
                                reference_comment_and_correction_router,
                                reference_manual_term_tag_router,
                                reference_router, resource_descriptor_router,
                                resource_router, search_router)


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


models.Base.metadata.create_all(engine)


@app.on_event('startup')
def setup_database():
    """

    :return:
    """

    setup_resource_descriptor()


app.include_router(resource_router.router)
app.include_router(reference_router.router)
app.include_router(author_router.router)
app.include_router(editor_router.router)
app.include_router(cross_reference_router.router)
app.include_router(resource_descriptor_router.router)
app.include_router(file_router.router)
app.include_router(mesh_detail_router.router)
app.include_router(mod_reference_type_router.router)
app.include_router(person_router.router)
app.include_router(note_router.router)
app.include_router(database_router.router)
app.include_router(reference_comment_and_correction_router.router)
app.include_router(reference_automated_term_tag_router.router)
app.include_router(reference_manual_term_tag_router.router)
app.include_router(bulk_downloads_router.router)
app.include_router(search_router.router)

app.add_api_route("/health", health([is_database_online]))

app.openapi = custom_openapi  # type: ignore


def run():
    """

    :return:
    """

    # May put back but for now do not see way to have multiple formats
    #  using the logging.basicConfig
    # LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    # LOGGING_CONFIG["formatters"]["access"]["fmt"] = '%(asctime)s [%(name)s] %(levelprefix)s %(client_addr)s - ' \
    #                                                 '"%(request_line)s" %(status_code)s'
    print(SQLALCHEMY_DATABASE_URL)
    state = environ.get('ENV_STATE')
    log_filename = './Lit_FastAPI.log'
    if state == 'test':
        log_filename = '/logs/Lit_FastAPI.log'
    uvicorn.run("main:app",
                port=args['port'],
                host=args['ip_address'],
                timeout_keep_alive=5001,
                log_config=logging.basicConfig(
                    filename=log_filename,
                    filemode='w',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help='Port to run the server on', default=8080, nargs='?')
    parser.add_argument('-i', '--ip-address', type=str, help='IP address of the host', default='0.0.0.0', nargs='?')
    parser.add_argument('-v', dest='verbose', action='store_true')

    args = vars(parser.parse_args())
    run()
