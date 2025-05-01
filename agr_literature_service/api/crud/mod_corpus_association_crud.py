"""
mod_corpus_association_crud.py
===========================
"""

from datetime import datetime

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from agr_literature_service.api.crud.cross_reference_crud import check_xref_and_generate_mod_id, \
    set_mod_curie_to_invalid
from agr_literature_service.api.models import ModCorpusAssociationModel, ReferenceModel, \
    ModModel, WorkflowTagModel, CurationStatusModel
from agr_literature_service.api.schemas import ModCorpusAssociationSchemaPost
from agr_literature_service.api.crud.workflow_tag_crud import transition_to_workflow_status, \
    get_current_workflow_status, delete_workflow_tags
from agr_literature_service.api.crud.topic_entity_tag_utils import delete_non_manual_tets, \
    delete_manual_tets, has_manual_tet
from agr_literature_service.api.crud.ateam_db_helpers import name_to_atp, search_topic

file_needed_tag_atp_id = "ATP:0000141"  # file needed
manual_indexing_needed_tag_atp_id = "ATP:0000274"


def create(db: Session, mod_corpus_association: ModCorpusAssociationSchemaPost) -> int:
    """
    Create a new mod_corpus_association
    :param db:
    :param mod_corpus_association:
    :return:
    """

    mod_corpus_association_data = jsonable_encoder(mod_corpus_association)

    reference_curie = mod_corpus_association_data["reference_curie"]
    del mod_corpus_association_data["reference_curie"]
    mod_abbreviation = mod_corpus_association_data["mod_abbreviation"]
    del mod_corpus_association_data["mod_abbreviation"]

    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not reference:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {reference_curie} does not exist")

    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Mod with abbreviation {mod_abbreviation} does not exist")
    mod_corpus_association_db_obj = db.query(ModCorpusAssociationModel).filter(
        ModCorpusAssociationModel.reference_id == reference.reference_id).filter(
        ModCorpusAssociationModel.mod_id == mod.mod_id).first()
    if mod_corpus_association_db_obj:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"ModCorpusAssociation with the reference_curie {reference_curie} "
                                   f"and mod_abbreviation {mod_abbreviation} already exist, "
                                   f"with id:{mod_corpus_association_db_obj.mod_corpus_association_id} can not "
                                   f"create duplicate record.")
    db_obj = ModCorpusAssociationModel(**mod_corpus_association_data)
    db_obj.reference = reference
    db_obj.mod = mod
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)  # This refreshes the object and ensures that the ID is populated

    if "corpus" in mod_corpus_association_data and mod_corpus_association_data["corpus"] is True:
        check_xref_and_generate_mod_id(db, reference, mod_abbreviation)
        add_topic_list(db, reference_curie, mod_abbreviation)
        if get_current_workflow_status(db, reference_curie, "ATP:0000140",
                                       mod_abbreviation) is None:
            transition_to_workflow_status(db, reference_curie, mod_abbreviation, file_needed_tag_atp_id)
        if mod_abbreviation == 'SGD':
            wft_obj = WorkflowTagModel(reference_id=reference.reference_id,
                                       mod_id=mod.mod_id,
                                       workflow_tag_id=manual_indexing_needed_tag_atp_id)
            db.add(wft_obj)
            db.commit()
    return int(db_obj.mod_corpus_association_id)


def delete_workflow_tag_if_file_needed(db, reference, mod):
    if mod:
        current_file_upload_status = get_current_workflow_status(db, str(reference.reference_id),
                                                                 "ATP:0000140",
                                                                 mod_abbreviation=mod.abbreviation)
        if current_file_upload_status == file_needed_tag_atp_id:
            cur_workflow_tag = db.query(WorkflowTagModel).filter(
                WorkflowTagModel.reference_id == reference.reference_id,
                WorkflowTagModel.mod_id == mod.mod_id,
                WorkflowTagModel.workflow_tag_id == file_needed_tag_atp_id).first()
            db.delete(cur_workflow_tag)
            db.commit()


def destroy(db: Session, mod_corpus_association_id: int) -> None:
    """

    :param db:
    :param mod_corpus_association_id:
    :return:
    """

    mod_corpus_association = db.query(ModCorpusAssociationModel).filter(ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
    if not mod_corpus_association:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with mod_corpus_association_id {mod_corpus_association_id} not found")
    delete_workflow_tag_if_file_needed(db, mod_corpus_association.reference, mod_corpus_association.mod)
    db.delete(mod_corpus_association)
    db.commit()
    return None


def patch(db: Session, mod_corpus_association_id: int, mod_corpus_association_update):
    """
    Update a mod_corpus_association
    :param db:
    :param mod_corpus_association_id:
    :param mod_corpus_association_update:
    :return:
    """
    mod_corpus_association_data = jsonable_encoder(mod_corpus_association_update)
    mod_corpus_association_db_obj: ModCorpusAssociationModel = db.query(ModCorpusAssociationModel).filter(ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
    if not mod_corpus_association_db_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with mod_corpus_association_id {mod_corpus_association_id} not found")

    for field, value in mod_corpus_association_data.items():
        if field == "reference_curie":
            if value is not None:
                reference_curie = value
                new_reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
                if not new_reference:
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Reference with curie {reference_curie} does not exist")
                mod_corpus_association_db_obj.reference = new_reference
        elif field == "corpus":
            reference_obj = mod_corpus_association_db_obj.reference
            if "reference_curie" in mod_corpus_association_data and mod_corpus_association_data["reference_curie"] is not None:
                reference_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == mod_corpus_association_data["reference_curie"]).first()
            mod_abbreviation = mod_corpus_association_db_obj.mod.abbreviation
            if "mod_abbreviation" in mod_corpus_association_data and mod_corpus_association_data["mod_abbreviation"] is not None:
                db_mod = db.query(ModModel).filter(ModModel.abbreviation == mod_corpus_association_data["mod_abbreviation"]).first()
                mod_abbreviation = db_mod.abbreviation
            if value is True and mod_corpus_association_db_obj.corpus is not True:
                check_xref_and_generate_mod_id(db, reference_obj, mod_abbreviation)
                add_topic_list(db, reference_obj.curie, mod_abbreviation)
                if get_current_workflow_status(db, str(reference_obj.reference_id),
                                               "ATP:0000140",
                                               mod_abbreviation=mod_abbreviation) is None:
                    transition_to_workflow_status(db, reference_obj.curie, mod_abbreviation, file_needed_tag_atp_id)
                if mod_abbreviation == 'ZFIN':
                    wft_obj = WorkflowTagModel(reference_id=mod_corpus_association_db_obj.reference_id,
                                               mod_id=mod_corpus_association_db_obj.mod_id,
                                               workflow_tag_id=name_to_atp["pre-indexing prioritization needed"])
                    db.add(wft_obj)
                if mod_abbreviation == 'SGD' and mod_corpus_association_data.get('index_wft_id'):
                    wft_id = mod_corpus_association_data['index_wft_id']
                    wft_obj = WorkflowTagModel(reference_id=mod_corpus_association_db_obj.reference_id,
                                               mod_id=mod_corpus_association_db_obj.mod_id,
                                               workflow_tag_id=wft_id)
                    db.add(wft_obj)
            elif (value is False or value is None) and mod_corpus_association_db_obj.corpus is True:
                has_manual_tags = has_manual_tet(db, str(mod_corpus_association_db_obj.reference_id),
                                                 mod_abbreviation)
                if has_manual_tags and not mod_corpus_association_data.get('force_out'):
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                        detail=f"Curated topic and entity tags or automated tags generated from your MOD are associated with this reference. Please check with the curator who added these tags. mod_corpus_association_id = {mod_corpus_association_db_obj.mod_corpus_association_id}")
                delete_non_manual_tets(db, str(mod_corpus_association_db_obj.reference_id), mod_abbreviation)
                if has_manual_tags:
                    delete_manual_tets(db, str(mod_corpus_association_db_obj.reference_id), mod_abbreviation)
                delete_workflow_tags(db, str(mod_corpus_association_db_obj.reference_id), mod_abbreviation)
                set_mod_curie_to_invalid(db, reference_obj.reference_id, mod_corpus_association_db_obj.mod.abbreviation)
            setattr(mod_corpus_association_db_obj, field, value)
        else:
            setattr(mod_corpus_association_db_obj, field, value)

    mod_corpus_association_db_obj.dateUpdated = datetime.utcnow()
    db.add(mod_corpus_association_db_obj)
    db.commit()

    return {"message": "updated"}


def show(db: Session, mod_corpus_association_id: int):
    """

    :param db:
    :param mod_corpus_association_id:
    :return:
    """

    mod_corpus_association = db.query(ModCorpusAssociationModel).filter(ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
    if not mod_corpus_association:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with the mod_corpus_association_id {mod_corpus_association_id} is not available")

    mod_corpus_association_data = jsonable_encoder(mod_corpus_association)
    if mod_corpus_association_data["reference_id"]:
        mod_corpus_association_data["reference_curie"] = db.query(ReferenceModel).filter(ReferenceModel.reference_id == mod_corpus_association_data["reference_id"]).first().curie
    del mod_corpus_association_data["reference_id"]
    if mod_corpus_association_data["mod_id"]:
        mod_corpus_association_data["mod_abbreviation"] = db.query(ModModel).filter(ModModel.mod_id == mod_corpus_association_data["mod_id"]).first().abbreviation
    del mod_corpus_association_data["mod_id"]

    return mod_corpus_association_data


def show_by_reference_mod_abbreviation(db: Session, reference_curie: str, mod_abbreviation: str) -> int:
    """

    :param db:
    :param mod_corpus_association_id:
    :return:
    """

    mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).first()
    reference = db.query(ReferenceModel).filter(ReferenceModel.curie == reference_curie).first()
    if not mod:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Mod with the abbreviation {mod_abbreviation} is not available")
    elif not reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Reference with the curie {reference_curie} is not available")
    else:
        mod_corpus_association_db_obj = db.query(ModCorpusAssociationModel).filter(
            ModCorpusAssociationModel.reference_id == reference.reference_id).filter(
            ModCorpusAssociationModel.mod_id == mod.mod_id).first()
        if not mod_corpus_association_db_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"ModCorpusAssociation with the reference_curie {reference_curie} "
                                       f"and mod_abbreviation {mod_abbreviation} is not available")
        else:
            return mod_corpus_association_db_obj.mod_corpus_association_id
    return 200


def show_changesets(db: Session, mod_corpus_association_id: int):
    """

    :param db:
    :param mod_corpus_association_id:
    :return:
    """

    mod_corpus_association = db.query(ModCorpusAssociationModel).filter(
        ModCorpusAssociationModel.mod_corpus_association_id == mod_corpus_association_id).first()
    if not mod_corpus_association:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"ModCorpusAssociation with the mod_corpus_association_id {mod_corpus_association_id} is not available")

    history = []
    for version in mod_corpus_association.versions:
        tx = version.transaction
        history.append({"transaction": {"id": tx.id,
                                        "issued_at": tx.issued_at,
                                        "user_id": tx.user_id},
                        "changeset": version.changeset})

    return history


def add_topic_list(db: Session, reference_curie: str, mod_abbr: str):
    try:
        reference_id = db.query(ReferenceModel).filter_by(curie=reference_curie).one().reference_id
        mod_id = db.query(ModModel).filter_by(abbreviation=mod_abbr).one().mod_id
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"reference {reference_curie} or mod {mod_abbr} is not in the database: {e}")
    topic_data = search_topic(topic=None, mod_abbr=mod_abbr)
    try:
        for row in topic_data:
            x = CurationStatusModel(reference_id=reference_id,
                                    mod_id=mod_id,
                                    topic=row['curie'])
            db.add(x)
        db.commit()
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"An error occurred when adding topic data into curation table: {e}")
