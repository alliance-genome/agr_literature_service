from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder

from literature.schemas import CrossReferenceSchema
from literature.schemas import CrossReferenceSchemaUpdate

from literature.models import CrossReferenceModel
from literature.models import ReferenceModel
from literature.models import ResourceModel
from literature.models import ResourceDescriptorModel
from literature.crud.lookup import add_reference_resource


def create(db: Session, cross_reference: CrossReferenceSchema) -> str:
    cross_reference_data = jsonable_encoder(cross_reference)

    if db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == cross_reference_data['curie']).first():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail=f"CrossReference with curie {cross_reference_data['curie']} already exists")

    db_obj = CrossReferenceModel(**cross_reference_data)
    add_reference_resource(db, cross_reference, db_obj)

    db.add(db_obj)
    db.commit()

    return "created"


def destroy(db: Session, curie: str) -> None:
    cross_reference = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")
    db.delete(cross_reference)
    db.commit()

    return None


def patch(db: Session, curie: str, cross_reference_update: CrossReferenceSchemaUpdate) -> dict:

    cross_reference_db_obj = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} not found")
    add_reference_resource(db, cross_reference_update, cross_reference_db_obj)

    for field, value in cross_reference_update.dict().items():
        setattr(cross_reference_db_obj, field, value)

    cross_reference_db_obj.date_updated = datetime.utcnow()
    db.commit()

    return {"message": "updated"}


def show(db: Session, curie: str, indirect=True) -> dict:
    cross_reference = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"CrossReference with the curie {curie} is not available")

    cross_reference_data = jsonable_encoder(cross_reference)
    if cross_reference_data['resource_id']:
        cross_reference_data['resource_curie'] = db.query(ResourceModel.curie).filter(ResourceModel.resource_id == cross_reference_data['resource_id']).first().curie
    del cross_reference_data['resource_id']

    if cross_reference_data['reference_id']:
        cross_reference_data['reference_curie'] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == cross_reference_data['reference_id']).first().curie
    del cross_reference_data['reference_id']

    author_ids = []
    editor_ids = []
    if not indirect:
        for author in cross_reference.authors:
            author_ids.append(author.author_id)

        for editor in cross_reference.editors:
            editor_ids.append(editor.editor_id)
    cross_reference_data['author_ids'] = author_ids
    cross_reference_data['editor_ids'] = editor_ids

    [db_prefix, local_id] = curie.split(":", 1)
    resource_descriptor = db.query(ResourceDescriptorModel).filter(ResourceDescriptorModel.db_prefix == db_prefix).first()
    if resource_descriptor:
        default_url = resource_descriptor.default_url.replace("[%s]", local_id)
        cross_reference_data['url'] = default_url

        if cross_reference_data['pages']:
            pages_data = []
            for cr_page in cross_reference_data['pages']:
                page_url = ""
                for rd_page in resource_descriptor.pages:
                    if rd_page.name == cr_page:
                        page_url = rd_page.url
                        break
                pages_data.append({"name": cr_page,
                                   "url": page_url.replace("[%s]", local_id)})
            cross_reference_data['pages'] = pages_data
    elif cross_reference_data['pages']:
        pages_data = []
        for cr_page in cross_reference_data['pages']:
            pages_data.append({"name": cr_page})
        cross_reference_data['pages'] = pages_data

    return cross_reference_data


def show_changesets(db: Session, curie: str):
    cross_reference = db.query(CrossReferenceModel).filter(CrossReferenceModel.curie == curie).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie {curie} is not available")

    history = []
    for version in cross_reference.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
