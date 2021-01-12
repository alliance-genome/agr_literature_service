from shared.models import db

class Pubmed(db.Model):
    id = db.Column(db.String(10), primary_key=True)
    reference_id = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)

class Pubmod(db.Model):
    id = db.Column(db.String(10), primary_key=True)
    mod = db.Column(db.String(20), primary_key=False)
    reference_id = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)

class Journal(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    reference = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)

class Title(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    reference_id = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    title = db.Column(db.String(255), unique=False, nullable=True)
#   valid = bool
#    curator = 
#    date = 
#   notes

class Reference(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pubmeds = db.relationship('Pubmed', backref='reference', lazy=True)
    pubmods = db.relationship('Pubmod', backref='reference', lazy=True)
#    journals = db.relationship('Jurnal', backref='reference', lazy=True)
    titles = db.relationship('Title' , backref='reference', lazy=True)
