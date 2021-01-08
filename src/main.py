import logging
from os import path
import sys
import argparse
import json

from flask import Flask
from flask import request
from flask import jsonify
from flask_restful import Resource
from flask_restful import Api
from flask_sqlalchemy import SQLAlchemy
from waitress import serve
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


class Reference(Resource):
    def get(self, reference_id):
        titles = [] #Reference_title.query.all()
        logger.info(str(titles))
        return {'reference': 'titles'}

    def post(self, reference_id):
        data_string = request.form['data']
        is_json(data_string)
        data = json.loads(data_string)

        if 'title' not in data:
            return "missing title field"

        #TODO need to change the URL structure to not have the pubmed id in the url segment. Provide it in the data instead and then create a Alliance ID if one does not exist for it yet. And then add the data to the other tables. 
        #db.session.add(Reference_title(reference_id=reference_id, title=data['title']))
        db.session.commit()

        return {reference_id: data}

app = Flask(__name__)
api = Api(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/test.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
api.add_resource(Reference, '/reference/<reference_id>')
db = SQLAlchemy(app)


def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError as e:
    return False
  return True

class Pubmed_id(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    pubmed_id = db.Column(db.String(20), primary_key=False)
    alliance_id = db.Column(db.String(20), db.ForeignKey('reference_id.alliance_id'), nullable=False)

    def __repr__(self):
        return '<Pubmed_id %r>' % self.pubmed_id

class Reference_id(db.Model):
    alliance_id = db.Column(db.String(20), primary_key=True)
    pubmed_ids = db.relationship('Pubmed_id', backref='reference_id', lazy=True)
    journals = db.relationship('Reference_journal', backref='reference_id', lazy=True)
    titles = db.relationship('Reference_title', backref='reference_id', lazy=True)

    def __repr__(self):
        return '<Reference_id %r>' % self.alliance_id

class Reference_journal(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    alliance_id = db.Column(db.String(20), db.ForeignKey('reference_id.alliance_id'), nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)

    def __repr__(self):
        return '<Reference_journal %r>' % self.name

class Reference_title(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    alliance_id = db.Column(db.String(20), db.ForeignKey('reference_id.alliance_id'), nullable=False)
    title = db.Column(db.String(255), unique=False, nullable=True)

    def __repr__(self):
        return '<Reference_journal %s>' % self.title


db.create_all()

def main(args):
    """  starting app """

    if args['prod']:
        serve(app, host=args['ip_adress'], port=args['port'])
    else:
        app.run(host=args['ip_adress'], port=args['port'])

if __name__ == "__main__":
    """ call main start function """
    main(args)
