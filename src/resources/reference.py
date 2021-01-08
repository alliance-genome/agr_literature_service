import logging
import json

from flask import request
from flask import jsonify

from flask_restful import Resource
from flask_sqlalchemy import SQLAlchemy

from references.models import PubMed_id
from references.models import Reference_id
from references.models import Reference_journal
from references.models import Reference_title

db = SQLAlchemy()

logger = logging.getLogger('literature logger')

class ReferenceEndpoints(Resource):
    def get(self, reference_id):
        titles = [] #Reference_title.query.all()
        return {'reference': 'titles'}

    def post(self, reference_id):
        data_string = request.form['data']
        try:
            data = json.loads(data_string)
        except ValueError as e:
            return print(e)

        if 'title' not in data:
            return "missing title field"

        #TODO need to change the URL structure to not have the pubmed id in the url segment. Provide it in the data instead and then create a Alliance ID if one does not exist for it yet. And then add the data to the other tables. 
        #db.session.add(Reference_title(reference_id=reference_id, title=data['title']))
        db.session.commit()

        return {reference_id: data}
