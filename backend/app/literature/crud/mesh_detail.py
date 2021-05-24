import sqlalchemy
from sqlalchemy.orm import Session
from datetime import datetime

from fastapi import HTTPException
from fastapi import status
from fastapi.encoders import jsonable_encoder
from fastapi_sqlalchemy import db

from literature.schemas import MeshDetailSchemaPost
from literature.schemas import MeshDetailSchemaUpdate

from literature.models import Reference
from literature.models import MeshDetail


def create(mesh_detail: MeshDetailSchemaPost):
    mesh_detail_data = jsonable_encoder(mesh_detail)

    if 'reference_curie' in mesh_detail_data:
        reference_curie = mesh_detail_data['reference_curie']
        del mesh_detail_data['reference_curie']

    reference = db.session.query(Reference).filter(Reference.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                               detail=f"Reference with curie {reference_curie} does not exist")

    db_obj = MeshDetail(**mesh_detail_data)
    db_obj.reference = reference
    db.session.add(db_obj)
    db.session.commit()

    return db_obj


def destroy(mesh_detail_id: int):
    mesh_detail = db.session.query(MeshDetail).filter(MeshDetail.mesh_detail_id == mesh_detail_id).first()
    if not mesh_detail:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"MeshDetail with mesh_detail_id {mesh_detail_id} not found")
    db.session.delete(mesh_detail)
    db.session.commit()

    return None


def update(mesh_detail_id: int, mesh_detail_update: MeshDetailSchemaUpdate):

    mesh_detail_db_obj = db.session.query(MeshDetail).filter(MeshDetail.mesh_detail_id == mesh_detail_id).first()
    if not mesh_detail_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"MeshDetail with mesh_detail_id {mesh_detail_id} not found")


    if not mesh_detail_update.reference_curie:
       raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                           detail=f"Only supply either resource_curie or reference_curie")

    for field, value in vars(mesh_detail_update).items():
        if field == 'reference_curie' and value:
            reference_curie = value
            reference = db.session.query(Reference).filter(Reference.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                  detail=f"Reference with curie {reference_curie} does not exist")
            mesh_detail_db_obj.reference = reference
            mesh_detail_db_obj.resource = None
        else:
            setattr(mesh_detail_db_obj, field, value)

    mesh_detail_db_obj.dateUpdated = datetime.utcnow()
    db.session.commit()

    return db.session.query(MeshDetail).filter(MeshDetail.mesh_detail_id == mesh_detail_id).first()


def show(mesh_detail_id: int):
    mesh_detail = db.session.query(MeshDetail).filter(MeshDetail.mesh_detail_id == mesh_detail_id).first()
    mesh_detail_data = jsonable_encoder(mesh_detail)

    if not mesh_detail:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"MeshDetail with the mesh_detail_id {mesh_detail_id} is not available")

    if mesh_detail_data['reference_id']:
        mesh_detail_data['reference_curie'] = db.session.query(Reference.curie).filter(Reference.reference_id == mesh_detail_data['reference_id']).first()[0]
    del mesh_detail_data['reference_id']

    return mesh_detail_data


def show_changesets(mesh_detail_id: int):
    mesh_detail = db.session.query(MeshDetail).filter(MeshDetail.mesh_detail_id == mesh_detail_id).first()
    if not mesh_detail:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"MeshDetail with the mesh_detail_id {mesh_detail_id} is not available")

    history = []
    for version in mesh_details.versions:
        tx = version.transaction
        history.append({'transaction': {'id': tx.id,
                                        'issued_at': tx.issued_at,
                                        'user_id': tx.user_id},
                        'changeset': version.changeset})

    return history
