import uvicorn

import argparse

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi_sqlalchemy import DBSessionMiddleware

from literature import  models
from literature.database.main import engine

from literature.routers import resource
from literature.routers import reference
from literature.routers import author
from literature.routers import editor

from literature.config import config
from literature.database.config import SQLALCHEMY_DATABASE_URL

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', type=int, help='Port to run the server on')
parser.add_argument('-i', '--ip-adress', type=str, help='IP address of the host', default='0.0.0.0', nargs='?')
parser.add_argument('-v', dest='verbose', action='store_true')

args = vars(parser.parse_args())


app = FastAPI()
app.add_middleware(DBSessionMiddleware, db_url=SQLALCHEMY_DATABASE_URL)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Alliance Literature Service",
        version="0.1.0",
        description="This service provides access to the Alliance Bibliographic Corpus and metadata",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


models.Base.metadata.create_all(engine)

app.include_router(resource.router)
app.include_router(reference.router)
app.include_router(author.router)
app.include_router(editor.router)

app.openapi = custom_openapi

if __name__ == '__main__':
    uvicorn.run("main:app", port=args['port'], host=args['ip_adress'])
