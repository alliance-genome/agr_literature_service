"""Flask configuration."""
from os import environ, path
from dotenv import load_dotenv

basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, '.env'))


class Config:
    """Set Flask config variables."""

    FLASK_ENV = 'development'
    TESTING = True
    SECRET_KEY = environ.get('SECRET_KEY')

    # Database
    psql_username = environ.get('PSQL_USERNAME')
    psql_password = environ.get('PSQL_PASSWORD')
    psql_host = environ.get('PSQL_HOST')
    psql_port = environ.get('PSQL_PORT')
    psql_database = environ.get('PSQL_DATABASE')
    
    SQLALCHEMY_DATABASE_URI = "postgresql://" + psql_username + ":" + psql_password + "@" + psql_host + ":" + psql_port + "/" + psql_database

    SQLALCHEMY_TRACK_MODIFICATIONS = False
