import uvicorn

import argparse

from uvicorn.config import LOGGING_CONFIG
from starlette.graphql import GraphQLApp

from sqlalchemy.orm import Session

from fastapi import FastAPI
from fastapi import Depends

from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware

from literature import models
from literature import database
from literature.database.main import engine


from literature.routers import resource_router
from literature.routers import reference_router
from literature.routers import author_router
from literature.routers import note_router
from literature.routers import editor_router
from literature.routers import file_router
from literature.routers import cross_reference_router
from literature.routers import resource_descriptor_router
from literature.routers import mesh_detail_router
from literature.routers import mod_reference_type_router
from literature.routers import person_router
from literature.routers import database_router
from literature.routers import reference_comment_and_correction_router
from literature.routers import reference_automated_term_tag_router
from literature.routers import reference_manual_term_tag_router
from literature.routers import bulk_downloads_router

from literature.config import config
from literature.database.config import SQLALCHEMY_DATABASE_URL

from initialize import setup_resource_descriptor

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', type=int, help='Port to run the server on', default=8080, nargs='?')
parser.add_argument('-i', '--ip-adress', type=str, help='IP address of the host', default='0.0.0.0', nargs='?')
parser.add_argument('-v', dest='verbose', action='store_true')

args = vars(parser.parse_args())

title="Alliance Literature Service"
version="0.1.0"
description="This service provides access to the Alliance Bibliographic Corpus and metadata"

app = FastAPI(title=title,
              version=version,
              description=description)

app.add_middleware(CORSMiddleware,
                   allow_credentials=True,
                   allow_origins=["*"],
                   allow_methods=["*"],
                   allow_headers=["*"])

def custom_openapi():
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

app.openapi = custom_openapi


def run():
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    LOGGING_CONFIG["formatters"]["access"]["fmt"] = '%(asctime)s [%(name)s] %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'

    uvicorn.run("main:app",
                port=args['port'],
                host=args['ip_adress'],
                timeout_keep_alive=5001)


if __name__ == '__main__':
    run()
