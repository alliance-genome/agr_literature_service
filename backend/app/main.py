import uvicorn

import argparse

from uvicorn.config import LOGGING_CONFIG

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi_sqlalchemy import DBSessionMiddleware

from literature import models
from literature.database.main import engine

from literature.routers import resource
from literature.routers import reference
from literature.routers import author
from literature.routers import editor
from literature.routers import s3file
from literature.routers import cross_reference
from literature.routers import resource_descriptor
from literature.routers import mesh_detail
from literature.routers import mod_reference_type
from literature.routers import person

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

app.add_middleware(DBSessionMiddleware,
                   db_url=SQLALCHEMY_DATABASE_URL)

app.add_middleware(CORSMiddleware,
                   allow_credentials=True,
                   allow_origin_regex=".*",
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
async def setup_database():
    await setup_resource_descriptor()

app.include_router(resource.router)
app.include_router(reference.router)
app.include_router(author.router)
app.include_router(editor.router)
app.include_router(cross_reference.router)
app.include_router(resource_descriptor.router)
app.include_router(s3file.router)
app.include_router(mesh_detail.router)
app.include_router(mod_reference_type.router)
app.include_router(person.router)

app.openapi = custom_openapi

def run():
    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"
    LOGGING_CONFIG["formatters"]["access"]["fmt"] = '%(asctime)s [%(name)s] %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s'

    uvicorn.run("main:app", port=args['port'], host=args['ip_adress'])



if __name__ == '__main__':
    run()
