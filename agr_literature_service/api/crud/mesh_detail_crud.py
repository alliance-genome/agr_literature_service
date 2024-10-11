"""
mesh_detail_crud.py
===================
"""


from datetime import datetime
from typing import List

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.models import MeshDetailModel, ReferenceModel
from agr_literature_service.api.schemas import MeshDetailSchemaPost


def create(db: Session, mesh_detail: MeshDetailSchemaPost) -> int:
    """
    Create a new mesh detail
    :param db:
    :param mesh_detail:
    :return:
    """

    mesh_detail_data = jsonable_encoder(mesh_detail)

    reference_curie = mesh_detail_data["reference_curie"]
    del mesh_detail_data["reference_curie"]

    reference = (
        db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    )
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")

    db_obj = MeshDetailModel(**mesh_detail_data)
    db_obj.reference = reference
    db.add(db_obj)
    db.commit()

    return int(db_obj.mesh_detail_id)


def destroy(db: Session, mesh_detail_id: int) -> None:
    """

    :param db:
    :param mesh_detail_id:
    :return:
    """

    mesh_detail = db.query(MeshDetailModel).filter(MeshDetailModel.mesh_detail_id == mesh_detail_id).first()
    if not mesh_detail:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"MeshDetail with mesh_detail_id {mesh_detail_id} not found")
    db.delete(mesh_detail)
    db.commit()

    return None


def patch(db: Session, mesh_detail_id: int, mesh_detail_update) -> dict:
    """

    :param db:
    :param mesh_detail_id:
    :param mesh_detail_update:
    :return:
    """
    mesh_detail_data = jsonable_encoder(mesh_detail_update)
    mesh_detail_db_obj = db.query(MeshDetailModel).filter(MeshDetailModel.mesh_detail_id == mesh_detail_id).first()
    if not mesh_detail_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"MeshDetail with mesh_detail_id {mesh_detail_id} not found")

    for field, value in mesh_detail_data.items():
        if field == "reference_curie" and value:
            reference_curie = value
            reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
            if not reference:
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                    detail=f"Reference with curie {reference_curie} does not exist")
            mesh_detail_db_obj.reference = reference
        else:
            setattr(mesh_detail_db_obj, field, value)

    mesh_detail_db_obj.dateUpdated = datetime.utcnow()
    db.add(mesh_detail_db_obj)
    db.commit()

    return {"message": "updated"}


def show(db: Session, mesh_detail_id: int) -> dict:
    """

    :param db:
    :param mesh_detail_id:
    :return:
    """

    mesh_detail = db.query(MeshDetailModel).filter(MeshDetailModel.mesh_detail_id == mesh_detail_id).first()
    mesh_detail_data = jsonable_encoder(mesh_detail)

    if not mesh_detail:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"MeshDetail with the mesh_detail_id {mesh_detail_id} is not available")

    if mesh_detail_data["reference_id"]:
        mesh_detail_data["reference_curie"] = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == mesh_detail_data["reference_id"]).first()[0]
        del mesh_detail_data['reference_id']

    return mesh_detail_data


def show_changesets(db: Session, mesh_detail_id: int) -> List:
    """

    :param db:
    :param mesh_detail_id:
    :return:
    """

    mesh_detail = db.query(MeshDetailModel).filter(MeshDetailModel.mesh_detail_id == mesh_detail_id).first()
    if not mesh_detail:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"MeshDetail with the mesh_detail_id {mesh_detail_id} is not available")

    history = []
    for version in mesh_detail.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history
