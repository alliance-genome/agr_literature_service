from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand

app = Flask(__name__)
app.config.from_object('config.Config')
#app.config.from_pyfile('config.py')
db = SQLAlchemy(app)

migrate = Migrate(app, db)
manager = Manager(app)

manager.add_command('db', MigrateCommand)

from references.models.reference import Pubmed
from references.models.reference import Pubmod
from references.models.reference import Author
from references.models.reference import Page
from references.models.reference import Keyword
from references.models.reference import ModReferenceType
from references.models.reference import Tag
from references.models.reference import MeshTerm
from references.models.reference import Reference

from references.models.resource import ResourceTitleSynonym
from references.models.resource import ResourceAuthor
from references.models.resource import ResourceEditor
from references.models.resource import ResourceVolume
from references.models.resource import Resource


if __name__ == '__main__':
    manager.run()
