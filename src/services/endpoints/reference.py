import logging
import json
from datetime import datetime
from datetime import timezone

from flask import request
from flask import jsonify
from flask import make_response


from flask_sqlalchemy import SQLAlchemy

from flask_apispec import marshal_with
from flask_apispec import use_kwargs
from flask_apispec.views import MethodResource
from flask_apispec.annotations import doc

from sqlalchemy.exc import IntegrityError

from shared.app import db

from marshmallow import fields


from references.models.reference import Reference
from references.models.reference import Pubmed
from references.models.reference import Pubmod

from references.schemas.reference import ReferenceSchemaIn
from references.schemas.reference import ReferenceSchemaOut


logger = logging.getLogger('literature logger')


#@marshal_with(ReferenceSchemaOut)
@doc(description='References', tags=['references'])
class ReferencesEndpoint(MethodResource):

   # @use_kwargs(ReferenceSchemaIn)
   # @marshal_with({"status": fields.String(),
          #         "location": fields.String(),
    #               "id": fields.String()}, code=201)
    def post(self, **data):
        print('data')


        last_reference_id = "AGR-Reference-000000001"
        reference_id = "1"

        location =  "/api/reference/1"# + reference_id
        response = make_response(jsonify({"status": "created",
                                          "id": reference_id,
                                          "location": location}), 201)
        response.headers["Content-Type"] = "application/json"
        response.headers["Location"] = "/api/reference/" + reference_id
        return response
        # Create and/or Find reference record by ID
"""
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
"""
# might want to use obj = DBOccurence.query.get_or_404(occ_id)

@doc(description='Reference',
     tags=['reference'],
     params={"reference_id": {"description": "Alliance Reference ID",
                              "schema": {"type": "string"},
                              "example": "AGRReference:<zero-padded-8-digits>",
                              "required": True}})
class ReferenceEndpoint(MethodResource):
    @marshal_with(ReferenceSchemaOut)
    def get(self, reference_id):
        reference = Reference.query.filter_by(primaryId=reference_id).first()
        return reference

    @marshal_with(ReferenceSchemaOut)
    @use_kwargs(ReferenceSchemaOut)
    def put(self, reference_id, **kwargs):
        reference = Reference.query.filter_by(primaryId=reference_id).first()
        for key, value in kwargs.items():
            setattr(reference, key, value)
        session.add(reference)
        session.commit()

    #@marshal_with({"message": fields.String()})
    @marshal_with(None, code=204)
    def delete(self, reference_id):
        reference = Reference.query.filter_by(primaryId=reference_id).first()
        if not reference:
            return {"message": "Reference could not be found:" + reference_id }, 404
        #db.session.delete(reference)
        #db.session.commit()
        return None, 204
