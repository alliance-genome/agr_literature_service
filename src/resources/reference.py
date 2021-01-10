import logging
import json

from flask import request
from flask import jsonify

from flask_sqlalchemy import SQLAlchemy
from flask_apispec.views import MethodResource
from flask_apispec.annotations import doc

from shared.models import db

from references.models import Pubmed
from references.models import Reference
#from references.models import Journal
#from references.models import Title

logger = logging.getLogger('literature logger')

@doc(description='Add reference to resource', tags=['references'])
class AddReferenceResource(MethodResource):
    def post(self):
        data_string = request.form['data']
        try:
            data = json.loads(data_string)
        except ValueError as e:
            return print(e)

        if 'pubmed_id' not in data:
           return "missing pubmed field"

        if 'title' not in data:
            return "missing title field"


        Pubmed_obj_from_db = Pubmed.query.filter_by(id=data['pubmed_id']).first()
        if Pubmed_obj_from_db:
            return 'Pubmed ID exists in DB'
        else:
            #add pubmed_id to database with link to new reference_id
            reference_obj = Reference()
            db.session.add(reference_obj)
            pubmed_obj = Pubmed(id=data['pubmed_id'], reference=reference_obj)
            db.session.add(pubmed_obj)
            db.session.commit()


        return 'added to database'

@doc(description='list all references to resource', tag=['references'])
class ReferenceListResource(MethodResource):
    def get(self):
        return "test"
