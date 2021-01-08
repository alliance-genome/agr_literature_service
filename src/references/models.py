from shared.models import db

class PubMed_id(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    pubmed_id = db.Column(db.String(20), primary_key=False)
    alliance_id = db.Column(db.String(20), db.ForeignKey('reference_id.alliance_id'), nullable=False)

    def __repr__(self):
        return '<PubMed_id %r>' % self.pubmed_id


class Reference_id(db.Model):
    alliance_id = db.Column(db.String(20), primary_key=True)
    pubmed_ids = db.relationship('PubMed_id', backref='reference_id', lazy=True)
    journals = db.relationship('Reference_journal', backref='reference_id', lazy=True)
    titles = db.relationship('Reference_title', backref='reference_id', lazy=True)

    def __repr__(self):
        return '<Reference_id %r>' % self.alliance_id


class Reference_journal(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    alliance_id = db.Column(db.String(20), db.ForeignKey('reference_id.alliance_id'), nullable=False)
    name = db.Column(db.String(255), unique=False, nullable=True)

    def __repr__(self):
        return '<Reference_journal %r>' % self.name


class Reference_title(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    alliance_id = db.Column(db.String(20), db.ForeignKey('reference_id.alliance_id'), nullable=False)
    title = db.Column(db.String(255), unique=False, nullable=True)

    def __repr__(self):
        return '<Reference_journal %s>' % self.title
