"""
cross_reference_crud.py
=======================
"""
import os
from typing import List, Dict

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import or_, and_, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, subqueryload

from agr_literature_service.api.crud.reference_resource import (add_reference_resource,
                                                                create_obj)
from agr_literature_service.api.models import (
    CrossReferenceModel,
    ReferenceModel,
    ResourceDescriptorModel
)
# from agr_literature_service.api.models.cross_reference_model import sgd_id_seq


def set_curie_prefix(xref_db_obj: CrossReferenceModel):
    xref_db_obj.curie_prefix = xref_db_obj.curie.split(":")[0]


def get_cross_reference(db: Session, curie_or_id: str) -> CrossReferenceModel:
    cross_reference_id = int(curie_or_id) if curie_or_id.isdigit() else None
    cross_reference = db.query(CrossReferenceModel).filter(
        or_(CrossReferenceModel.curie == curie_or_id,
            CrossReferenceModel.cross_reference_id == cross_reference_id)).order_by(
        CrossReferenceModel.is_obsolete).first()
    if not cross_reference:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Cross Reference with curie or id {curie_or_id} not found")
    return cross_reference


def create(db: Session, cross_reference, mod_abbreviation=None) -> int:
    cross_reference_data = jsonable_encoder(cross_reference)
    db_obj = create_obj(db, CrossReferenceModel, cross_reference_data)
    set_curie_prefix(db_obj)

    try:
        db.add(db_obj)
        db.commit()
    except IntegrityError as e:
        db.rollback()
        orig_args = getattr(e.orig, 'args', None)
        if orig_args:
            error_details = f"Error details: {str(orig_args[0])}"
        else:
            error_details = f"Error details: {str(e)}"
        if (
            mod_abbreviation and mod_abbreviation in ['WB', 'SGD']
            and 'constraint "idx_curie_prefix_ref_no_cgc"' in error_details
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Another curator has added this paper to the {mod_abbreviation} corpus. "
                    "Please reload the page and try again."
                )
            )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Cannot add cross-reference with CURIE {cross_reference_data['curie']}. Error: {error_details}"
            )
        )
    return int(db_obj.cross_reference_id)


def destroy(db: Session, cross_reference_id: int) -> None:
    cross_reference = get_cross_reference(db, str(cross_reference_id))
    db.delete(cross_reference)
    db.commit()
    return None


def patch(db: Session, cross_reference_id: int, cross_reference_update) -> dict:
    cross_reference_data = jsonable_encoder(cross_reference_update)
    cross_reference_db_obj = get_cross_reference(db, str(cross_reference_id))
    add_reference_resource(db, cross_reference_db_obj, cross_reference_update, non_fatal=True)
    for field, value in cross_reference_data.items():
        setattr(cross_reference_db_obj, field, value)
    if "curie" in cross_reference_update:
        set_curie_prefix(cross_reference_db_obj)
    db.add(cross_reference_db_obj)
    db.commit()
    return {"message": "updated"}


def show_from_curies(db: Session, curies: List[str]) -> List[dict]:
    cross_references = db.query(CrossReferenceModel).options(subqueryload(CrossReferenceModel.reference)).options(
        subqueryload(CrossReferenceModel.resource)).filter(
        CrossReferenceModel.curie.in_(curies)).all()
    unique_cross_refs: Dict[str, CrossReferenceModel] = {}
    for xref in cross_references:
        if xref.curie not in unique_cross_refs or unique_cross_refs[xref.curie].is_obsolete is True:
            unique_cross_refs[xref.curie] = xref
    resource_descriptors = db.query(ResourceDescriptorModel).filter(
        ResourceDescriptorModel.db_prefix.in_([curie.split(":")[0] for curie in curies])).all()
    resource_desc_prefix_obj_map = {rd.db_prefix: rd for rd in resource_descriptors}
    formatted_cross_references = []
    for cross_reference in unique_cross_refs.values():
        cross_reference_data = jsonable_encoder(cross_reference)
        formatted_cross_references.append(format_cross_reference_data(db, cross_reference, cross_reference_data,
                                                                      resource_desc_prefix_obj_map))
    return formatted_cross_references


def format_cross_reference_data(db: Session, cross_reference_object: CrossReferenceModel,
                                cross_reference_data: dict, resource_desc_prefix_obj_map: dict) -> dict:
    if cross_reference_data["resource_id"]:
        cross_reference_data["resource_curie"] = cross_reference_object.resource.curie
    del cross_reference_data["resource_id"]

    if cross_reference_data["reference_id"]:
        cross_reference_data["reference_curie"] = cross_reference_object.reference.curie
    del cross_reference_data["reference_id"]

    [db_prefix, local_id] = cross_reference_object.curie.split(":", 1)
    resource_descriptor = resource_desc_prefix_obj_map[db_prefix] if db_prefix in resource_desc_prefix_obj_map else None
    if resource_descriptor:
        default_url = resource_descriptor.default_url.replace("[%s]", local_id)
        cross_reference_data["url"] = default_url

        if cross_reference_data["pages"]:
            pages_data = []
            for cr_page in cross_reference_data["pages"]:
                page_url = ""
                for rd_page in resource_descriptor.pages:
                    if rd_page.name == cr_page:
                        page_url = rd_page.url
                        break
                pages_data.append({
                    "name": cr_page,
                    "url": page_url.replace("[%s]", local_id)
                })
            cross_reference_data["pages"] = pages_data
    elif cross_reference_data["pages"]:
        pages_data = []
        for cr_page in cross_reference_data["pages"]:
            pages_data.append({"name": cr_page})
        cross_reference_data["pages"] = pages_data
    return cross_reference_data


def show(db: Session, curie_or_cross_reference_id: str) -> dict:
    cross_reference = get_cross_reference(db, curie_or_cross_reference_id)
    cross_reference_data = jsonable_encoder(cross_reference)
    db_prefix = cross_reference.curie.split(":")[0]
    resource_descriptor = db.query(ResourceDescriptorModel).filter(
        ResourceDescriptorModel.db_prefix == db_prefix).first()
    resource_desc_prefix_obj_map = {db_prefix: resource_descriptor}
    return format_cross_reference_data(db=db, cross_reference_object=cross_reference,
                                       cross_reference_data=cross_reference_data,
                                       resource_desc_prefix_obj_map=resource_desc_prefix_obj_map)


def check_xref_and_generate_mod_id(db: Session, reference_obj: ReferenceModel, mod_abbreviation: str):
    cross_reference = db.query(CrossReferenceModel).filter(
        and_(CrossReferenceModel.reference_id == reference_obj.reference_id,
             CrossReferenceModel.is_obsolete.is_(False),
             CrossReferenceModel.curie_prefix == mod_abbreviation)).order_by(
        CrossReferenceModel.is_obsolete).first()
    if cross_reference:
        return
    env_state = os.environ.get("ENV_STATE", "")
    if env_state == "prod":
        return
    if mod_abbreviation not in ['WB', 'SGD']:
        return

    new_mod_curie = generate_new_mod_curie(db, mod_abbreviation, reference_obj.curie)
    create(db, new_mod_curie, mod_abbreviation)


def generate_new_mod_curie(db: Session, mod_abbreviation, ref_curie):

    if mod_abbreviation == 'WB':
        new_wbpaper_number = 1
        cross_reference = db.query(CrossReferenceModel.curie).filter(
            and_(CrossReferenceModel.curie.startswith("WB:WBPaper0"),
                 CrossReferenceModel.curie_prefix == mod_abbreviation)).order_by(
            CrossReferenceModel.curie.desc()).first()
        if cross_reference:
            new_wbpaper_number = int(cross_reference.curie[11:]) + 1
        new_wbpaper_string = str(new_wbpaper_number).zfill(8)
        new_wbpaper_curie = f"WB:WBPaper{new_wbpaper_string}"
        new_wbpaper_xref = {
            "curie": new_wbpaper_curie,
            "pages": [
                "reference"
            ],
            "reference_curie": ref_curie
        }
        return new_wbpaper_xref

    if mod_abbreviation == 'SGD':
        row = db.execute(text("SELECT nextval('sgd_id_seq')")).fetchone()
        if row:
            sgdid_number = row[0]
            new_sgdid = f"SGD:S{sgdid_number}"
            new_xref = {
                "curie": new_sgdid,
                "pages": [
                    "reference"
                ],
                "reference_curie": ref_curie
            }
            return new_xref
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Cannot create a new SGDID")


def set_mod_curie_to_invalid(db, reference_id, mod_abbreviation):
    try:
        curie_prefix = "Xenbase" if mod_abbreviation == 'XB' else mod_abbreviation
        cr = db.query(CrossReferenceModel).filter_by(
            reference_id=reference_id,
            curie_prefix=curie_prefix,
            is_obsolete=False
        ).one_or_none()

        if cr:
            cr.is_obsolete = True
            db.add(cr)
            db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Error setting {mod_abbreviation} MOD ID to invalid for reference_id = {reference_id}. Error={str(e)}")


def show_changesets(db: Session, cross_reference_id: int):
    cross_reference = get_cross_reference(db, str(cross_reference_id))
    history = []
    for version in cross_reference.versions:
        tx = version.transaction
        history.append(
            {
                "transaction": {
                    "id": tx.id,
                    "issued_at": tx.issued_at,
                    "user_id": tx.user_id,
                },
                "changeset": version.changeset,
            }
        )
    return history


def autocomplete_on_id(prefix: str, query: str, return_prefix: bool, db: Session):
    string_before_id = ""
    if prefix == "WB":
        string_before_id = "WBPaper"
    if query.startswith(string_before_id):
        query = query[len(string_before_id)]
    matching_xrefs_query = db.query(CrossReferenceModel.curie).filter(
        CrossReferenceModel.curie.like(f"{prefix}:{string_before_id}{query}%")
    )
    matching_xrefs_count = matching_xrefs_query.count()
    matching_xrefs = matching_xrefs_query.order_by(CrossReferenceModel.curie).limit(20).all()
    matching_curies = ["".join(matching_xref.curie.split(":")[1:]) if not return_prefix else matching_xref.curie for
                       matching_xref in matching_xrefs]
    if matching_xrefs_count < 20:
        matching_xrefs_query_expanded = db.query(CrossReferenceModel.curie).filter(
            and_(
                CrossReferenceModel.curie.like(f"{prefix}:{string_before_id}%{query}%"),
                CrossReferenceModel.curie.notin_([matching_xref.curie for matching_xref in matching_xrefs])
            )
        )
        matching_xrefs_expanded = matching_xrefs_query_expanded.order_by(CrossReferenceModel.curie).limit(
            20 - matching_xrefs_count).all()
        matching_xrefs_count = matching_xrefs_count + matching_xrefs_query_expanded.count()
        matching_curies.extend(["".join(matching_xref_expanded.curie.split(":")[1:]) if not return_prefix else
                                matching_xref_expanded.curie for matching_xref_expanded in matching_xrefs_expanded])
    if matching_xrefs_count > 20:
        matching_curies.append("more ...")
    matching_curies_plain_text = "\n".join(matching_curies)
    return matching_curies_plain_text
