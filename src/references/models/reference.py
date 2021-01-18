from datetime import datetime

from shared.models import db

from references.schemas.referenceTag import TagName
from references.schemas.referenceTag import TagSource
from references.schemas.allianceCategory import AllianceCategory

class PrimaryId(db.Model):
    id = db.Column(db.String(20), primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Title(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

#Author field
class MiddleName(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    string = db.Column(db.String(255), nullable=False)
    authorId = db.Column(db.Integer, db.ForeignKey('author.id'), nullable=False)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Author(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)
    firstName = db.Column(db.String(255), unique=False, nullable=True)
    lastName = db.Column(db.String(255), unique=False, nullable=True)
    middleNames = db.relationship('MiddleName' , backref='author', lazy=True)
    #crossreferences
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class DatePublished(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class DateArrivedInPubMed(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    dateTime = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class DateLastModified(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    dateTime = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Volume(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Pages(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Abstract(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Citation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Keywords(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class PubMedType(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Publisher(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class AllianceCategory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.Enum(AllianceCategory), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class ModReferenceTypes(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    referenceType = db.Column(db.String(255), unique=False, nullable=True)
    source = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class IssueName(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class IssueDate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    datetime = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    #dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class Tags(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    tagName = db.Column(db.Enum(TagName),
                        unique=False, nullable=False)
    tagSource = db.Column(db.Enum(TagSource), unique=False, nullable=False)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class MeshTerms(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    meshHeadingTerm = db.Column(db.String(255), unique=False, nullable=True)
    meshQualifierTerm = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

# crossReferences

class Pubmed(db.Model):
    id = db.Column(db.String(10), primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)



class Pubmod(db.Model):
    id = db.Column(db.String(10), primary_key=True)
    mod = db.Column(db.String(20), primary_key=False)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class ResourceAbbreviation(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    referenceId = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    string = db.Column(db.String(255), unique=False, nullable=True)
    valid = db.Column(db.Boolean)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class Reference(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    primaryId = db.relationship('PrimaryId', backref='reference', lazy=True)
    title = db.relationship('Title' , backref='reference', lazy=True)
    authors = db.relationship('Author' , backref='reference', lazy=True)
    datePublished = db.relationship('DatePublished' , backref='reference', lazy=True)
    dateArrivedInPubMed = db.relationship('DateArrivedInPubMed' , backref='reference', lazy=True)
    dateLastModified = db.relationship('DateLastModified' , backref='reference', lazy=True)
    volume = db.relationship('Volume' , backref='reference', lazy=True)
    pages = db.relationship('Pages' , backref='reference', lazy=True)
    abstract = db.relationship('Abstract' , backref='reference', lazy=True)
    citation = db.relationship('Citation' , backref='reference', lazy=True)
    keywords = db.relationship('Keywords' , backref='reference', lazy=True)
    pubMedType = db.relationship('PubMedType' , backref='reference', lazy=True)
    publisher = db.relationship('Publisher' , backref='reference', lazy=True)
    allianceCategory = db.relationship('AllianceCategory' , backref='reference', lazy=True)
    modReferenceTypes = db.relationship('ModReferenceTypes' , backref='reference', lazy=True)
    issueName = db.relationship('IssueName' , backref='reference', lazy=True)
    issueDate = db.relationship('IssueDate' , backref='reference', lazy=True)
    tags = db.relationship('Tags' , backref='reference', lazy=True)
    meshTerms = db.relationship('MeshTerms' , backref='reference', lazy=True)
    #crossReferences
    pubmeds = db.relationship('Pubmed', backref='reference', lazy=True)
    pubmods = db.relationship('Pubmod', backref='reference', lazy=True)
    resourceAbbreviation = db.relationship('ResourceAbbreviation' , backref='reference', lazy=True)
    dateCreated = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
