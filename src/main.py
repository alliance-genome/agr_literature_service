from os import path
import sys
import argparse
import json

import logging
import logging.config

from flask import Flask
from flask import Blueprint

from flask_sqlalchemy import SQLAlchemy
from flask_apispec.extension import FlaskApiSpec

from waitress import serve

from resources.reference import AddReferenceResource
from resources.reference import GetReferenceResource

from shared.models import db


log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', type=int, help='Port to run the server on')
parser.add_argument('-i', '--ip-adress', type=str, help='IP address of the host', default='localhost', nargs='?')
parser.add_argument('--prod', help='Run WSGI server in production environment', action='store_true')
parser.add_argument('-v', dest='verbose', action='store_true')

args = vars(parser.parse_args())

flask_app = Flask(__name__)
flask_app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/test.db'
flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

db.init_app(flask_app)
flask_app.app_context().push()
docs = FlaskApiSpec(flask_app)

reference_bp = Blueprint('references_api', __name__, url_prefix='/reference/')
reference_bp.add_url_rule('/add/', view_func=AddReferenceResource.as_view('AddReferenceResource'))
reference_bp.add_url_rule('/<string:id>/get/',
                           view_func=GetReferenceResource.as_view('GetReferenceResource'))

app = flask_app
app.register_blueprint(reference_bp)
docs.register(AddReferenceResource, blueprint="references_api", endpoint='AddReferenceResource')
docs.register(GetReferenceResource, blueprint="references_api", endpoint='GetReferenceResource')


if __name__ == "__main__":
    """ call main start function """

    db.create_all()
    if args['prod']:
        serve(app, host=args['ip_adress'], port=args['port'])
    else:
        app.run(host=args['ip_adress'], port=args['port'])
