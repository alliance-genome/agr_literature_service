from shared.models import db

class Pubmed(db.Model):
    id = db.Column(db.String(10), primary_key=True)
    reference_id = db.Column(db.Integer, db.ForeignKey('reference.id'),
        nullable=False)

class Reference(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pubmeds = db.relationship('Pubmed', backref='reference', lazy=True)
#    journals = db.relationship('Jurnal', backref='reference', lazy=True)
#    titles = db.relationship('Title', backref='reference', lazy=True)


class Journal(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    reference = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)


class Title(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    reference = db.Column(db.Integer, db.ForeignKey('reference.id'), nullable=False)
    title = db.Column(db.String(255), unique=False, nullable=True)
