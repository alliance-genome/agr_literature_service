from datetime import datetime

from shared.models import db

from references.schemas.referenceTag import TagName
from references.schemas.referenceTag import TagSource
from references.schemas.allianceCategory import AllianceCategory

class ResourcePrimaryId(db.Model):
    id = db.Column(db.String(20), primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceTitle(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    title = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceTitleSynonyms(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    syonym = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceIsoAbbreviation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    iso = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceMedlineAbbreviation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    abbreviation = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceCopyrightDate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    date = db.Column(db.DateTime)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourcePublisher(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    publisher = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourcePrintISSN(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    printISSN = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceOnlineISSN(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    onlineISSN = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

#Author field
class ResourceMiddleName(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    authorId = db.Column(db.Integer, db.ForeignKey('resource_editor_or_author.id'), nullable=False)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceEditorOrAuthor(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)
    firstName = db.Column(db.String(255), unique=False, nullable=True)
    lastName = db.Column(db.String(255), unique=False, nullable=True)
    middleNames = db.relationship('ResourceMiddleName' , backref='resourceEditorOrAuthor', lazy=True)
    #crossreferences
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceVolumes(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    volume = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourcePages(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    pages = db.Column(db.Integer, unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceAbstractOrSummary(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    abstractOrSummary = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

# crossReferences

class Resource(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    primaryId = db.relationship('ResourcePrimaryId', backref='resource', lazy=True)
    title = db.relationship('ResourceTitle' , backref='resource', lazy=True)
    titleSynonyms = db.relationship('ResourceTitleSynonyms' , backref='resource', lazy=True)
    isoAbbreviation = db.relationship('ResourceIsoAbbreviation' , backref='resource', lazy=True)
    medlineAbbreviation = db.relationship('ResourceMedlineAbbreviation' , backref='resource', lazy=True)
    copyrightDate = db.relationship('ResourceCopyrightDate' , backref='resource', lazy=True) #datetime
    publisher = db.relationship('ResourcePublisher' , backref='resource', lazy=True)
    printISSN = db.relationship('ResourcePrintISSN' , backref='resource', lazy=True)
    onlineISSN = db.relationship('ResourceOnlineISSN' , backref='resource', lazy=True)
    editorOrAuthors = db.relationship('ResourceEditorOrAuthor' , backref='resource', lazy=True) #author schema
    volumes = db.relationship('ResourceVolumes' , backref='resource', lazy=True)
    pages = db.relationship('ResourcePages' , backref='resource', lazy=True) #int
    abstractOrSummary = db.relationship('ResourceAbstractOrSummary' , backref='resource', lazy=True)
    #crossReferences
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
