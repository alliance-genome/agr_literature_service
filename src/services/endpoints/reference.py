import logging
import json
from datetime import datetime
from datetime import timezone

from flask import request
from flask import jsonify

from flask_sqlalchemy import SQLAlchemy

from flask_apispec import marshal_with
from flask_apispec import use_kwargs
from flask_apispec.views import MethodResource
from flask_apispec.annotations import doc

from shared.models import db

from references.models.reference import Reference
from references.models.reference import Pubmed
from references.models.reference import Pubmod

from references.schemas.reference import ReferenceSchemaIn
from references.schemas.reference import ReferenceSchemaOut


logger = logging.getLogger('literature logger')


@marshal_with(ReferenceSchemaOut)
@doc(description='Reference', tags=['reference'])
class ReferenceEndpoint(MethodResource):

    @use_kwargs(ReferenceSchemaIn)
    def post(self, **data):
        print('data')
        print(data)
        
        # Create and/or Find reference record by ID
        reference_obj_from_db = Reference.query.filter_by(primaryId=data['primaryId']).first()
        if not reference_obj_from_db:
            reference = Reference(**data)
            db.session.add(reference)
            db.session.commit()
            reference_obj_from_db = Reference.query.filter_by(primaryId=data['primaryId']).first()

        return reference_obj_from_db
        #if id is None and 'pubmedId' in data:
        #    pubmed_obj_from_db = Pubmed.query.filter_by(id=data['pubmedId']).first()
        #    if pubmed_obj_from_db:
        #        id = pubmed_obj_from_db.referenceId
        #elif id is None and 'pubmodId' in data:
        #    pubmod_obj_from_db = Pubmod.query.filter_by(id=data['pubmodId']).first()
        #    if pubmod_obj_from_db:
        #        id = pubmod_obj_from_db.referenceId

        datetime_now = datetime.now(timezone.utc)
        if id is None:
            reference = Reference(data)
            print("created reference")
            print(reference)
        else:
            reference = Reference.query.filter_by(id=id).first()

        # add secondary IDs to database
        if 'pubmedId' in data:
            pubmed = Pubmed.query.filter_by(id=data['pubmedId']).first()
            if not pubmed:
                print("Adding pubmed_id to database")
                db.session.add(reference)
                pubmed = Pubmed(id=data['pubmedId'], reference=reference)
                db.session.add(pubmed)
                db.session.commit()

                id = Pubmed.query.filter_by(id=data['pubmedId']).first().referenceId
        if 'pubmodId' in data:
            if 'mod' not in data:
                return "'mod' field required if adding 'pubmod_id'"
            print(data['pubmodId'])
            pubmod = Pubmod.query.filter_by(id=data['pubmodId']).first()
            if not pubmod:
                print("Adding PubMod ID to Database")
                logger.info("Adding PubMod ID to database: ", data['pubmodId'])
                db.session.add(reference)
                pubmod = Pubmod(id=data['pubmod_id'], mod=data['mod'], reference=reference)
                db.session.add(pubmod)
                db.session.commit()
                id = Pubmod.query.filter_by(id=data['pubmodId']).first().referenceId

        if not 'pubmedId' in data and not 'pubmodId' in data:
            db.session.add(reference)
            db.session.commit()
            



        reference = Reference.query.filter_by(id=id).first()
        #if reference is None:
             


        update_reference = False
        print(data)
        print(str(type(data)))
        print(reference)
        if 'primaryId' in data:
             reference.primaryId = data['primaryId']
             update_reference = True
        if 'title' in data:
             reference.title = data['title']
             update_reference = True
        if 'datePublished' in data:
             reference.datePublished = data['datePublished']
             upate_reference = True
        if 'dateArrivedInPubMed' in data:
             refrence.dateArrivedInPubMed = data['dateArrivedInPubMed']
             update_reference = True
        if 'dateLastModified' in data:
             reference.dateLastModified = data['dateLastModified']
             update_reference = True
        if 'volume' in data:
             reference.volume = data['volume']
             update_reference = True
        if 'abstract' in data:
             reference.abstract = data['abstract']
             update_reference = True
        if 'citation' in data:
             reference.citation = data['citation']
             update_reference = True
        if 'pubMedType' in data:
             reference.pubMedType = data['pubMedType']
             update_reference = True
        if 'publisher' in data:
             reference.publisher = data['publisher']
             update_reference = True
        if 'allianceCategory' in data:
             reference.allianceCategory = data['allianceCategory']
             update_reference = True
        if 'issueName' in data:
             reference.issueName = data['issueName']
             update_reference = True
        if 'issueDate' in data:
             reference.issueDate = data['issueDate']
             update_reference = True
        if 'resourceAbbreviation' in data:
             reference.resourceAbbreviation = data['resourceAbbreviation']
             update_reference = True
        if 'updatedBy' in data:
             reference.updatedBy = data['updatedBy']
             update_reference = True

        #relation fields to add: author, pages, modReferenceTypes, meshTerms, tags, crossReferences

        if 'authors' in data:
             if not isinstance(data['authors'], list):
                return "Make sure 'authors' field is populated with a list"
             for author in data['authors']:
                 print(author)

        if update_reference:
             db.session.add(reference)
             db.session.commit()

        return reference

    def get(self):
        reference = Reference.query.filter_by(id=id).one()
        return {'id': reference.id,
                'primaryId': None,
                'title': None,
                'authors': None,
                'datePublished': None,
                'dateArrivedInPubMed': None,
                'dateLastModified': None,
                'volume': None,
                'pages': None,
                'abstract': None,
                'citation': None,
                'keywords': None,
                'pubMedType': None,
                'publisher': None,
                'allianceCategory': None,
                'modReferenceTypes': None,
                'issueName': None,
                'issueDate': None,
                'tags': None,
                'meshTerms': None,
                # crossReferences
                'pubmedIDs': Pubmed.query.filter_by(referenceId=id),
                'pubmodIDs': Pubmod.query.filter_by(referenceId=id),
                'resourceAbbreviation': None,
                'dateTimeCreated': None}
