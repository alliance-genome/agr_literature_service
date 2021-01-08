from os import path
import sys
import argparse
import json

import logging
import logging.config

from flask import Flask
from flask import request
from flask import jsonify
from flask_restful import Resource
from flask_restful import Api
from flask_sqlalchemy import SQLAlchemy
from waitress import serve

from resources.reference import ReferenceEndpoints


log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--port', type=int, help='Port to run the server on')
parser.add_argument('-i', '--ip-adress', type=str, help='IP address of the host', default='localhost', nargs='?')
parser.add_argument('--prod', help='Run WSGI server in production environment', action='store_true')
parser.add_argument('-v', dest='verbose', action='store_true')

args = vars(parser.parse_args())

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/test.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

db = SQLAlchemy(app)
db.create_all()

api = Api(app)
api.add_resource(ReferenceEndpoints, '/reference/<reference_id>')

def main(args):
    """ Starting Server """

    if args['prod']:
        serve(app, host=args['ip_adress'], port=args['port'])
    else:
        app.run(host=args['ip_adress'], port=args['port'])

if __name__ == "__main__":
    """ call main start function """
    main(args)
