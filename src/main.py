import logging
from os import path
import sys, getopt

from flask import Flask, request
from flask_restful import Resource, Api
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
api = Api(app)

class Reference(Resource):
    def get(self, reference_id):
        return {'reference': reference_id}

    def put(self, reference_id):
        return {reference_id: request.form['data']}

def main():
    """  starting app """
    logger.info("Reading args")

    api.add_resource(Reference, '/reference/<reference_id>')


    app.run(host='localhost', port=args[2])

if __name__ == "__main__":
    """ this should be in the program's main/start/run function """
    import logging.config

    log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
    logging.config.fileConfig(log_file_path)
    logger = logging.getLogger(__name__)

    try:
      opts, args = getopt.getopt(sys.argv,"port:")
    except getopt.GetoptError:
      print('python src/main.py -p <port>')
      sys.exit(2)

    main()
