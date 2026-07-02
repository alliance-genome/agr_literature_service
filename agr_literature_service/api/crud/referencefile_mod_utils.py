from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.referencefile_utils import remove_from_s3_and_db
from agr_literature_service.api.models import ReferencefileModAssociationModel, ReferencefileModel, ModModel
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost


def reject_direct_embedding_access_change(referencefile: ReferencefileModel) -> None:
    """Embedding parquets' access is derived from their source file and kept in
    sync by embedding_file_crud — the only writer allowed to touch it. Reject
    changes coming through the public referencefile_mod surface: a direct edit
    could make a derived embedding downloadable more broadly than the text it
    was computed from. (Internal syncs write the rows directly, so they are not
    affected by this guard.)"""
    if referencefile.file_class == "embedding":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Access to embedding files is derived from their source file and cannot be "
                   "changed directly. Change the source file's access instead, or delete the "
                   "embedding by deleting its parquet referencefile.")


def resync_derived_embeddings(db: Session, referencefile_id: int) -> None:
    """After an access (referencefile_mod) change on a referencefile, re-sync
    the access of any embedding parquets derived from it so a derived embedding
    is never downloadable more broadly (or narrowly) than its source text.
    No-op for files that are not an embedding source."""
    # Local import: embedding_file_crud imports referencefile_crud, which
    # imports this module.
    from agr_literature_service.api.crud.embedding_file_crud import resync_embeddings_access_for_source
    resync_embeddings_access_for_source(db, referencefile_id)


def create(db: Session, request: ReferencefileModSchemaPost):
    if db.query(ReferencefileModel.referencefile_id).filter(
            ReferencefileModel.referencefile_id == request.referencefile_id).one_or_none() is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Referencefile with referencefile_id {str(request.referencefile_id)} "
                                   f"is not available")
    referencefile = db.query(ReferencefileModel).filter(
        ReferencefileModel.referencefile_id == request.referencefile_id).one_or_none()
    if referencefile and any(
            (ref_file_mod.mod.abbreviation if ref_file_mod.mod else ref_file_mod.mod) == request.mod_abbreviation for
            ref_file_mod in referencefile.referencefile_mods):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="The specified mod and reference file are already associated")
    if request.mod_abbreviation:
        mod_id = db.query(ModModel.mod_id).filter(ModModel.abbreviation == request.mod_abbreviation).one_or_none()
        if mod_id is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Mod {request.mod_abbreviation} is not available")

        new_referencefile_mod = ReferencefileModAssociationModel(mod_id=mod_id[0], referencefile_id=request.referencefile_id)
    else:
        new_referencefile_mod = ReferencefileModAssociationModel(referencefile_id=request.referencefile_id)
    db.add(new_referencefile_mod)
    db.commit()
    resync_derived_embeddings(db, request.referencefile_id)
    return new_referencefile_mod.referencefile_mod_id


def read_referencefile_mod_obj_from_db(db: Session, referencefile_mod_id: int):
    referencefile_mod = db.query(ReferencefileModAssociationModel).filter(
        ReferencefileModAssociationModel.referencefile_mod_id == referencefile_mod_id).one_or_none()
    if referencefile_mod is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ReferencefileMod with referencefile_mod_id {str(referencefile_mod_id)} "
                                   f"is not avaliable")
    return referencefile_mod


def destroy(db: Session, referencefile_mod_id: int):
    referencefile_mod = read_referencefile_mod_obj_from_db(db, referencefile_mod_id)
    reject_direct_embedding_access_change(referencefile_mod.referencefile)
    referencefile_id = referencefile_mod.referencefile_id
    if len(referencefile_mod.referencefile.referencefile_mods) == 1:
        # Removing the last association deletes the whole referencefile. If it
        # is an embedding source, clean up the derived embeddings FIRST: the
        # ON DELETE CASCADE would drop the embedding_file rows and strand
        # their parquets (separate referencefiles the cascade can't reach).
        # Local import: embedding_file_crud imports referencefile_crud, which
        # imports this module.
        from agr_literature_service.api.crud.embedding_file_crud import delete_embeddings_for_source
        delete_embeddings_for_source(db, referencefile_id)
        remove_from_s3_and_db(db, referencefile_mod.referencefile)
        db.commit()
    else:
        db.delete(referencefile_mod)
        db.commit()
        resync_derived_embeddings(db, referencefile_id)
