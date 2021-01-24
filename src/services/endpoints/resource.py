import logging
import json
from datetime import datetime
from datetime import timezone

from flask import request
from flask import jsonify

from flask_sqlalchemy import SQLAlchemy

from flask_apispec import marshal_with
from flask_apispec.views import MethodResource
from flask_apispec.annotations import doc

from shared.models import db

from references.models.resource import Resource
from references.models.resource import ResourcePrimaryId

from references.schemas.resource import ResourceSchema


logger = logging.getLogger('literature logger')

@marshal_with(ResourceSchema)
@doc(description='Add resource to database', tags=['resource'])
class AddResourceEndpoint(MethodResource):
    def post(self):
        print("AddResoureEndpoint")
        try:
            data_string = request.form['data']
            data = json.loads(data_string)
        except ValueError as e:
            return print(e)
        print("data")
        #Create and/or Find reference record by ID

        print(data)
        if 'id' in data:
           print("id exists")
           resource = Resource.query.filter_by(id=data['id']).first()
           if not resource:
              return "ERROR: Could not process - Resource id not found " + str(data['id'])
        elif 'primaryId' in data:
           print("primaryId exists")
           primary_id = ResourcePrimaryId.query.filter_by(id=data['primaryId']).first()
           if primary_id:
              print(primary_id)
              resource = Resource.query.filter_by(id=primary_id.resourceId).first()
           else:
              print("Adding primary ID")
              #add primary id

        id = 0
        datetime_now = datetime.now(timezone.utc)

        return 'Created or Updated: AllianceResource:%s' % id

@marshal_with(ResourceSchema, envelope="data")
@doc(description='Get Resource Data', tag=['reference'])
class GetResourceEndpoint(MethodResource):
    def get(self, id):
        resource = Resource.query.filter_by(id=id).one()
        return {'id': resource.id,
                'primaryId': None,
                'title': None,
                'titleSynonym': None,
                'isoAbbreviation': None,
                'medlineAbbreviatoin': None,
                'copywriteDate': None,
                'publisher': None,
                'printISSN': None,
                'onlineISSN': None,
                'editorOrAuthor': None,
                'volumes': None,
                'pages': None,
                'abstractOrSummary': None,
                'crossReferences': None}
