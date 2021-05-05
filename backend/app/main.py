import uvicorn

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
#from fastapi_sqlalchemy import DBSessionMiddleware

from literature import  models
from literature.database import engine

from literature.routers import resource
from literature.routers import reference

#from literature.config import config

#SQLALCHEMY_DATABASE_URL = "postgresql://" \
#        + config.PSQL_USERNAME + ":" + config.PSQL_PASSWORD \
#        + "@" + config.PSQL_HOST + ":" + config.PSQL_PORT \
#        + "/" + config.PSQL_DATABASE

app = FastAPI()
#app.add_middleware(DBSessionMiddleware, db_url=SQLALCHEMY_DATABASE_URL)





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

app.openapi = custom_openapi

if __name__ == '__main__':
    uvicorn.run("main:app", port=8080, host='0.0.0.0')
