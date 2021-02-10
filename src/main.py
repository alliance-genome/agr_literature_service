from os import path
import argparse

import logging
import logging.config

from flask import Flask
from flask import Blueprint

from flask_apispec.extension import FlaskApiSpec
from flask_continuum import Continuum

from waitress import serve

from services.endpoints.reference import ReferenceEndpoint
from services.endpoints.resource import AddResourceEndpoint
from services.endpoints.resource import GetResourceEndpoint

from shared.models import db


log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', type=int, help='Port to run the server on')
parser.add_argument('-i', '--ip-adress', type=str, help='IP address of the host', default='0.0.0.0', nargs='?')
parser.add_argument('--prod', help='Run WSGI server in production environment', action='store_true')
parser.add_argument('-v', dest='verbose', action='store_true')

args = vars(parser.parse_args())

continuum = Continuum(db=db)

flask_app = Flask(__name__)
flask_app.config.from_object('config.Config')

db.init_app(flask_app)
continuum.init_app(flask_app)

docs = FlaskApiSpec(flask_app)

reference_bp = Blueprint('references_api', __name__, url_prefix='/reference/')
reference_bp.add_url_rule('/', view_func=ReferenceEndpoint.as_view('ReferenceEndpoint'))

resource_bp = Blueprint('resources_api', __name__, url_prefix='/resource/')
resource_bp.add_url_rule('/add/', view_func=AddResourceEndpoint.as_view('AddResourceEndpoint'))
resource_bp.add_url_rule('/<string:id>/get/',
                           view_func=GetResourceEndpoint.as_view('GetResourceEndpoint'))

app = flask_app
app.register_blueprint(reference_bp)
docs.register(ReferenceEndpoint, blueprint="references_api", endpoint='ReferenceEndpoint')

app.register_blueprint(resource_bp)
docs.register(AddResourceEndpoint, blueprint="resources_api", endpoint='AddResourceEndpoint')
docs.register(GetResourceEndpoint, blueprint="resources_api", endpoint='GetResourceEndpoint')


if __name__ == "__main__":
    """ call main start function """

    if args['prod']:
        serve(app, host=args['ip_adress'], port=args['port'])
    else:
        app.run(host=args['ip_adress'], port=args['port'])
