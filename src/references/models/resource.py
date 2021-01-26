from datetime import datetime

from shared.models import db

from references.schemas.allianceCategory import AllianceCategory

class ResourceTitleSynonym(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    resourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    title = db.Column(db.String(255), unique=False, nullable=True)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

#class ResourceMiddleName(db.Model):
#    id = db.Column(db.Integer, primary_key=True)
#    name = db.Column(db.String(255), nullable=False)
#    authorId = db.Column(db.Integer, db.ForeignKey('resource_author.id'), nullable=False)
#    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceAuthor(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)
    firstName = db.Column(db.String(255), unique=False, nullable=True)
    lastName = db.Column(db.String(255), unique=False, nullable=True)
 #   middleNames = db.relationship('ResourceMiddleName' , backref='resourceAuthor', lazy=True)
    #crossreferences
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ResourceEditor(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sourceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)
    firstName = db.Column(db.String(255), unique=False, nullable=True)
    lastName = db.Column(db.String(255), unique=False, nullable=True)
 #   middleNames = db.relationship('ResourceMiddleName' , backref='resourceAuthor', lazy=True)
    #crossreferences
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)



class ResourceVolume(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    volume = db.Column(db.String(255), unique=False, nullable=True)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

# crossReferences

class Resource(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    primaryId = db.Column(db.Integer, db.ForeignKey('resource.id'), nullable=False)
    title = db.relationship('ResourceTitle' , backref='resource', lazy=True)
    titleSynonyms = db.relationship('ResourceTitleSynonym' , backref='resource', lazy=True)
    isoAbbreviation = db.Column(db.String(255), unique=False, nullable=True)
    medlineAbbreviation = db.Column(db.String(255), unique=False, nullable=True)
    copyrightDate = db.Column(db.DateTime)
    publisher = db.Column(db.String(255), unique=False, nullable=True)
    printISSN = db.Column(db.String(255), unique=False, nullable=True)
    onlineISSN = db.Column(db.String(255), unique=False, nullable=True)
    editors = db.relationship('ResourceAuthor' , backref='resource', lazy=True)
    authors = db.relationship('ResourceEditor' , backref='resource', lazy=True)
    volumes = db.relationship('ResourceVolume' , backref='resource', lazy=True)
    pages = db.Column(db.Integer, unique=False, nullable=True)
    abstract = db.Column(db.String(255), unique=False, nullable=True)
    summary = db.Column(db.String(255), unique=False, nullable=True)
    #crossReferences
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
