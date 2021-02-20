from os import path
from os import getcwd

import logging
import logging.config

from flask import Flask
from flask import Blueprint

from flask_apispec.extension import FlaskApiSpec
from flask_sqlalchemy import SQLAlchemy
from flask_continuum import Continuum

log_file_path = path.join(getcwd(), 'src', 'logging.conf')
logging.config.fileConfig(log_file_path)

logger = logging.getLogger('literature logger')

app = Flask(__name__)
app.config.from_object('config.Config')

db = SQLAlchemy(app)

continuum = Continuum(app, db)
