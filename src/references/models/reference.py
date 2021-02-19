from datetime import datetime

from flask_continuum import VersioningMixin

from shared.app import db

from references.schemas.allianceCategory import AllianceCategory

from enum import Enum


class TagName(Enum):
      canShowImages = 1
      PMCOpenAccess = 2
      inCorpus = 3
      notRelevant = 4

class TagSource(Enum):
      SGD = 1
      ZFIN = 2
      RGD = 3
      WB = 4
      MGI = 5
      FB = 6

class Pubmed(db.Model):
    id = db.Column(db.String(10), primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Pubmod(db.Model):
    id = db.Column(db.String(10), primary_key=True)
    mod = db.Column(db.String(20), primary_key=False)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

#Author field
#class MiddleName(db.Model):
#    id = db.Column(db.Integer, primary_key=True)
#    string = db.Column(db.String(255), nullable=False)
#    authorId = db.Column(db.Integer, db.ForeignKey('author.id'), nullable=False)
#    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Author(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)
    firstName = db.Column(db.String(255), unique=False, nullable=True)
    lastName = db.Column(db.String(255), unique=False, nullable=True)
    # Rank
    # Institutions
    # middleNames = db.relationship('MiddleName' , backref='author', lazy=True)
    # crossreferences
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Page(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Keyword(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ModReferenceType(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    referenceType = db.Column(db.String(255), unique=False, nullable=True)
    source = db.Column(db.String(255), unique=False, nullable=True)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Tag(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    tagName = db.Column(db.Enum(TagName),
                        unique=False, nullable=False)
    tagSource = db.Column(db.Enum(TagSource), unique=False, nullable=False)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class MeshTerm(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    meshHeadingTerm = db.Column(db.String(255), unique=False, nullable=True)
    meshQualifierTerm = db.Column(db.String(255), unique=False, nullable=True)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

# crossReferences

class Reference(db.Model, VersioningMixin):
    id = db.Column(db.Integer, primary_key=True)
    primaryId = db.Column(db.String, unique=True, nullable=True)
    #pubmedIds = db.relationship('Pubmed', backref='reference', lazy=True)
    #pubmodIds = db.relationship('Pubmod', backref='reference', lazy=True)
    title = db.Column(db.String, unique=False, nullable=True)
    authors = db.relationship('Author' , backref='reference', lazy=True)
    datePublished = db.Column(db.String(255), unique=False, nullable=True)
    dateArrivedInPubMed = db.Column(db.String(255), unique=False, nullable=True)
    dateLastModified = db.Column(db.String(255), unique=False, nullable=True)
    volume = db.Column(db.String(255), unique=False, nullable=True)
    pages = db.relationship('Page' , backref='reference', lazy=True)
    abstract = db.Column(db.String(255), unique=False, nullable=True)
    citation = db.Column(db.String(255), unique=False, nullable=True)
    keywords = db.relationship('Keyword' , backref='reference', lazy=True)
    pubMedType = db.Column(db.String(255), unique=False, nullable=True)
    publisher = db.Column(db.String(255), unique=False, nullable=True)
    allianceCategory = db.Column(db.Enum(AllianceCategory), unique=False, nullable=True)
    modReferenceTypes = db.relationship('ModReferenceType' , backref='reference', lazy=True)
    issueName = db.Column(db.String(255), unique=False, nullable=True)
    issueDate = db.Column(db.String(255), unique=False, nullable=True)
    tags = db.relationship('Tag' , backref='reference', lazy=True)
    meshTerms = db.relationship('MeshTerm' , backref='reference', lazy=True)
    #crossReferences
    resourceAbbreviation = db.Column(db.String(255), unique=False, nullable=True)
    updatedBy = db.Column(db.String(255), unique=False, nullable=True)
    dateUpdated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
