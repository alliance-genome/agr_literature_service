import logging
from os import path
import sys, argparse

from flask import Flask, request
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy
from waitress import serve

app = Flask(__name__)
api = Api(app)

class Reference(Resource):
    def get(self, reference_id):
        return {'reference': reference_id}

    def put(self, reference_id):
        return {reference_id: request.form['data']}

def main(args):
    """  starting app """
    logger.info("Reading args")

    api.add_resource(Reference, '/reference/<reference_id>')

    if args['prod']:
       serve(app, host=args['ip_adress'], port=args['port'])
    else:
       app.run(host=args['ip_adress'], port=args['port'])

if __name__ == "__main__":
    """ this should be in the program's main/start/run function """
    import logging.config

    log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
    logging.config.fileConfig(log_file_path)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help='Port to run the server on')
    parser.add_argument('-i', '--ip-adress', type=str, help='IP address of the host', default='localhost', nargs='?')
    parser.add_argument('--prod', help='Run WSGI server in production environment', action='store_true')
    parser.add_argument('-v', dest='verbose', action='store_true')

    args = vars(parser.parse_args())


    main(args)
