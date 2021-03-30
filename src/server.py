from sys import version_info
import argparse

from flask import Flask
from flask import Blueprint

from flask_apispec.extension import FlaskApiSpec
from flask_continuum import Continuum

from waitress import serve

from services.endpoints.reference import ReferenceEndpoint
from services.endpoints.reference import ReferencesEndpoint
from services.endpoints.resource import AddResourceEndpoint
from services.endpoints.resource import GetResourceEndpoint

from shared.app import app, db

if version_info[0] < 3:
    raise Exception("Must be using Python 3")

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', type=int, help='Port to run the server on')
parser.add_argument('-i', '--ip-adress', type=str, help='IP address of the host', default='0.0.0.0', nargs='?')
parser.add_argument('--prod', help='Run WSGI server in production environment', action='store_true')
parser.add_argument('-v', dest='verbose', action='store_true')

args = vars(parser.parse_args())

docs = FlaskApiSpec(app)

api_blueprint = Blueprint('api', __name__, url_prefix='/api/')

references_view = ReferencesEndpoint.as_view(ReferencesEndpoint.__name__)
api_blueprint.add_url_rule('/references/', view_func=references_view)

reference_view = ReferenceEndpoint.as_view(ReferenceEndpoint.__name__)
api_blueprint.add_url_rule('/reference/<string:reference_id>', view_func=reference_view)

#resource_bp = Blueprint('resources_api', __name__, url_prefix='/resource/')
#resource_bp.add_url_rule('', view_func=AddResourceEndpoint.as_view('AddResourceEndpoint'))
#resource_bp.add_url_rule('/<string:resource_id>/',
#                           view_func=GetResourceEndpoint.as_view('GetResourceEndpoint'))

app.register_blueprint(api_blueprint)
docs.register(ReferenceEndpoint, blueprint=api_blueprint.name, endpoint=ReferenceEndpoint.__name__)
docs.register(ReferencesEndpoint, blueprint=api_blueprint.name, endpoint=ReferencesEndpoint.__name__)

#app.register_blueprint(resource_bp)
#docs.register(AddResourceEndpoint, blueprint="resources_api", endpoint='AddResourceEndpoint')
#docs.register(GetResourceEndpoint, blueprint="resources_api", endpoint='GetResourceEndpoint')


if __name__ == "__main__":
    """ call main start function """

    if args['prod']:
        serve(app, host=args['ip_adress'], port=args['port'])
    else:
        app.run(host=args['ip_adress'], port=args['port'])
