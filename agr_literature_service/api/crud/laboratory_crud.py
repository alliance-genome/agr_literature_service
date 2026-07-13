import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from agr_literature_service.api.models import (
    LaboratoryModel,
    LaboratoryCrossReferenceModel,
    LaboratoryAlleleDesignationModel,
    LaboratoryPersonModel,
    ModModel,
)
from agr_literature_service.api.schemas import LaboratorySchemaCreate
from agr_literature_service.api.crud.user_utils import map_to_user_id
from agr_literature_service.global_utils import get_next_laboratory_curie

logger = logging.getLogger(__name__)


def resolve_laboratory_id(db: Session, curie_or_laboratory_id: str) -> int:
    laboratory_id = int(curie_or_laboratory_id) if curie_or_laboratory_id.isdigit() else None
    lab = (
        db.query(LaboratoryModel.laboratory_id)
        .filter(
            or_(
                LaboratoryModel.curie == curie_or_laboratory_id,
                LaboratoryModel.laboratory_id == laboratory_id,
            )
        )
        .one_or_none()
    )
    if not lab:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with curie or laboratory_id {curie_or_laboratory_id} not found",
        )
    return lab[0]


def _curie_prefix_from(curie: str) -> str:
    curie = curie.strip()
    if curie.count(":") != 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid CURIE '{curie}': expected exactly one colon",
        )
    return curie.split(":", 1)[0]


def create(db: Session, payload: LaboratorySchemaCreate) -> LaboratoryModel:
    data: Dict[str, Any] = jsonable_encoder(payload)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    xrefs_data = data.pop("cross_references", None)
    alleles_data = data.pop("allele_designations", None)

    # Resolve + validate inline allele designations BEFORE creating anything, so an
    # invalid mod_abbreviation (or a duplicated MOD in the request) fails the whole
    # request atomically — no laboratory, cross-references, or alleles are created.
    resolved_alleles = []
    if alleles_data:
        seen_mod_ids: set = set()
        for al in alleles_data:
            mod_abbreviation = al["mod_abbreviation"]
            row = (
                db.query(ModModel.mod_id)
                .filter(ModModel.abbreviation == mod_abbreviation)
                .one_or_none()
            )
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"MOD with abbreviation '{mod_abbreviation}' not found",
                )
            mod_id = row[0]
            if mod_id in seen_mod_ids:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"Multiple allele designations for MOD '{mod_abbreviation}' "
                        "in the request; at most one per MOD is allowed."
                    ),
                )
            seen_mod_ids.add(mod_id)
            resolved_alleles.append((mod_id, al["allele_designation"]))

    # Validate cross-references against the two unique constraints on
    # laboratory_cross_reference: curie is globally unique, and
    # (laboratory_id, curie_prefix) is unique per-laboratory.
    if xrefs_data:
        seen_curies: set = set()
        seen_prefixes: set = set()
        for xr in xrefs_data:
            curie = xr["curie"].strip()
            curie_prefix = _curie_prefix_from(curie)

            if curie in seen_curies:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Cross-reference '{curie}' is duplicated in the request",
                )
            seen_curies.add(curie)

            if curie_prefix in seen_prefixes:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"Multiple cross-references with prefix '{curie_prefix}' "
                        "in the request; at most one per prefix is allowed."
                    ),
                )
            seen_prefixes.add(curie_prefix)

            existing = (
                db.query(LaboratoryCrossReferenceModel.laboratory_cross_reference_id)
                .filter(LaboratoryCrossReferenceModel.curie == curie)
                .first()
            )
            if existing:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Cross-reference '{curie}' already exists",
                )

    # Allocate the curie from MATI (like reference/resource/person). Done after all
    # validation above so a rejected request doesn't waste an external MATI id.
    data["curie"] = get_next_laboratory_curie(db)

    obj = LaboratoryModel(**data)
    db.add(obj)
    # Wrap flush and commit: DB constraints (e.g. ck_laboratory_name_or_strain,
    # curie NOT NULL/unique) can fire at the flush that assigns laboratory_id, not
    # just at commit. Convert any such violation into a 422 instead of a raw 500.
    try:
        db.flush()  # get laboratory_id for the inline children
        new_laboratory_id = obj.laboratory_id

        if xrefs_data:
            for xr in xrefs_data:
                curie = xr["curie"].strip()
                curie_prefix = _curie_prefix_from(curie)
                db.add(
                    LaboratoryCrossReferenceModel(
                        laboratory_id=obj.laboratory_id,
                        curie=curie,
                        curie_prefix=curie_prefix,
                        pages=xr.get("pages"),
                        is_obsolete=bool(xr.get("is_obsolete", False)),
                    )
                )

        for mod_id, allele_designation in resolved_alleles:
            db.add(
                LaboratoryAlleleDesignationModel(
                    laboratory_id=obj.laboratory_id,
                    mod_id=mod_id,
                    allele_designation=allele_designation,
                )
            )

        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Database constraint violation; please verify input and retry.",
        )
    # Return via show() so the response eager-loads cross_references and
    # allele_designations (consistent with patch(), avoids N+1 on serialization).
    return show(db, str(new_laboratory_id))


def destroy(db: Session, curie_or_laboratory_id: str) -> None:
    laboratory_id = resolve_laboratory_id(db, curie_or_laboratory_id)
    obj: Optional[LaboratoryModel] = (
        db.query(LaboratoryModel).filter(LaboratoryModel.laboratory_id == laboratory_id).first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with curie or laboratory_id {curie_or_laboratory_id} not found",
        )
    db.delete(obj)
    db.commit()


def patch(db: Session, curie_or_laboratory_id: str, patch_dict: Dict[str, Any]) -> Dict[str, Any]:
    laboratory_id = resolve_laboratory_id(db, curie_or_laboratory_id)
    obj: Optional[LaboratoryModel] = (
        db.query(LaboratoryModel).filter(LaboratoryModel.laboratory_id == laboratory_id).first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with curie or laboratory_id {curie_or_laboratory_id} not found",
        )

    data = jsonable_encoder(patch_dict)

    if "created_by" in data and data["created_by"] is not None:
        data["created_by"] = map_to_user_id(data["created_by"], db)
    if "updated_by" in data and data["updated_by"] is not None:
        data["updated_by"] = map_to_user_id(data["updated_by"], db)

    ALLOWED = {
        "name", "strain_designation", "institution", "webpage",
        "city", "state", "postal_code", "country", "street_address",
        "email", "email_visibility", "lab_is_open", "status",
        "research_area", "short_research_description",
        "additional_information", "private_note",
    }
    # NOT NULL columns must not be set to null via PATCH.
    NOT_NULL = {"lab_is_open", "status"}
    for field, value in data.items():
        if field not in ALLOWED:
            continue
        if field in NOT_NULL and value is None:
            continue
        setattr(obj, field, value)

    # e.g. clearing both name and strain_designation violates
    # ck_laboratory_name_or_strain; surface as 422, not a raw 500.
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Database constraint violation; please verify input and retry.",
        )
    return {"message": "updated"}


def show(db: Session, curie_or_laboratory_id: str) -> LaboratoryModel:
    laboratory_id = resolve_laboratory_id(db, curie_or_laboratory_id)
    obj: Optional[LaboratoryModel] = (
        db.query(LaboratoryModel)
        .options(
            selectinload(LaboratoryModel.cross_references),
            selectinload(LaboratoryModel.allele_designations).selectinload(
                LaboratoryAlleleDesignationModel.mod
            ),
            selectinload(LaboratoryModel.lab_persons).selectinload(LaboratoryPersonModel.person),
        )
        .filter(LaboratoryModel.laboratory_id == laboratory_id)
        .first()
    )
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Laboratory with curie or laboratory_id {curie_or_laboratory_id} not found",
        )
    return obj


def find_by_name_or_strain_designation(db: Session, query: str) -> List[LaboratoryModel]:
    """Resolve a free-text laboratory lookup with a fixed precedence:

    1. exact, case-insensitive match on strain_designation (a short code) — return
       all such labs (normally one; more only if a code is shared);
    2. otherwise a case-insensitive substring match on name, ordered by name;
    3. otherwise an empty list.

    Matching strain exactly (never as a substring) keeps a short code from
    polluting name results. Each result eager-loads the same joins as show().
    """
    query = (query or "").strip()
    if not query:
        return []

    options = (
        selectinload(LaboratoryModel.cross_references),
        selectinload(LaboratoryModel.allele_designations).selectinload(
            LaboratoryAlleleDesignationModel.mod
        ),
        selectinload(LaboratoryModel.lab_persons).selectinload(LaboratoryPersonModel.person),
    )

    strain_matches = (
        db.query(LaboratoryModel)
        .options(*options)
        .filter(func.lower(LaboratoryModel.strain_designation) == query.lower())
        .order_by(LaboratoryModel.name.asc())
        .all()
    )
    if strain_matches:
        return strain_matches

    return (
        db.query(LaboratoryModel)
        .options(*options)
        .filter(LaboratoryModel.name.ilike(f"%{query}%"))
        .order_by(LaboratoryModel.name.asc())
        .all()
    )
