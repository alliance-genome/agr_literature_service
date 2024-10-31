import copy
import gzip
import hashlib
import logging
import os
import shutil
import tarfile
import tempfile
from itertools import count
from typing import List, Union

import boto3  # type: ignore
from fastapi import HTTPException, status, UploadFile
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, or_, text, select
from sqlalchemy.orm import Session, subqueryload, joinedload
from starlette.background import BackgroundTask
from starlette.responses import FileResponse

from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.crud.referencefile_utils import read_referencefile_db_obj, \
    get_s3_folder_from_md5sum, remove_from_s3_and_db
from agr_literature_service.api.crud.referencefile_mod_utils import create as create_mod_connection, \
    destroy as destroy_mod_association
from agr_literature_service.api.crud.workflow_tag_crud import get_current_workflow_status, \
    transition_to_workflow_status, is_file_upload_blocked, reset_workflow_tags_after_deleting_main_pdf
from agr_literature_service.api.crud.topic_entity_tag_utils import delete_non_manual_tets
from agr_literature_service.api.models import ReferenceModel, ReferencefileModel, ReferencefileModAssociationModel, \
    ModModel, CopyrightLicenseModel, CrossReferenceModel
from agr_literature_service.api.routers.okta_utils import OktaAccess, OKTA_ACCESS_MOD_ABBR
from agr_literature_service.api.s3.upload import upload_file_to_bucket
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost, \
    ReferencefileSchemaRelated, ReferencefileSchemaUpdate
from agr_literature_service.api.schemas.response_message_schemas import messageEnum
from agr_literature_service.lit_processing.utils.s3_utils import download_file_from_s3

logger = logging.getLogger(__name__)

file_upload_process_atp_id = "ATP:0000140"
file_uploaded_tag_atp_id = "ATP:0000134"
file_upload_in_progress_tag_atp_id = "ATP:0000139"
text_conversion_process_atp_id = "ATP:0000161"


def get_main_pdf_referencefile_id(db: Session, curie_or_reference_id: str,
                                  mod_abbreviation: str = None) -> Union[int, None]:
    logger.info("Getting main pdf referencefile")
    reference: ReferenceModel = get_reference(db=db, curie_or_reference_id=str(curie_or_reference_id),
                                              load_referencefiles=True)
    main_pdf_referencefiles = [referencefile for referencefile in reference.referencefiles if
                               referencefile.file_class == "main" and referencefile.file_publication_status == "final"
                               and (referencefile.pdf_type is None or referencefile.pdf_type == "pdf")]
    if mod_abbreviation is not None:
        for main_pdf_ref_file in main_pdf_referencefiles:
            for ref_file_mod in main_pdf_ref_file.referencefile_mods:
                if ref_file_mod.mod and ref_file_mod.mod.abbreviation == mod_abbreviation:
                    return main_pdf_ref_file.referencefile_id
    for main_pdf_ref_file in main_pdf_referencefiles:
        for ref_file_mod in main_pdf_ref_file.referencefile_mods:
            if ref_file_mod.mod is None:
                return main_pdf_ref_file.referencefile_id
    return None


def get_main_pdf_referencefile_ids_for_ref_curies_list(db: Session, curies: List[str], mod_abbreviation: str):
    ref_id_curie_map = {ref.reference_id: ref.curie for ref in db.execute(
        select(ReferenceModel.reference_id, ReferenceModel.curie).where(ReferenceModel.curie.in_(curies))
    ).all()}

    all_ref_files = db.execute(
        select(ReferencefileModel)
        .where(ReferencefileModel.reference_id.in_(list(ref_id_curie_map.keys())))
        .options(joinedload(ReferencefileModel.referencefile_mods))
    ).unique().scalars().all()

    curie_main_ref_file_map = {}

    for ref_file in all_ref_files:
        if ref_file.file_class == "main" and ref_file.file_publication_status == "final" and (ref_file.pdf_type is None or ref_file.pdf_type == "pdf"):
            main_pdf_reffile_id = None
            pmc_main_pdf_reffile_id = None
            for ref_file_mod in ref_file.referencefile_mods:
                if ref_file_mod.mod and ref_file_mod.mod.abbreviation == mod_abbreviation:
                    main_pdf_reffile_id = ref_file.referencefile_id
                    break
                if (ref_file_mod.mod is None and main_pdf_reffile_id is None):
                    pmc_main_pdf_reffile_id = ref_file.referencefile_id
            main_pdf_reffile_id = main_pdf_reffile_id or pmc_main_pdf_reffile_id
            if main_pdf_reffile_id:
                curie_main_ref_file_map[ref_id_curie_map[ref_file.reference_id]] = main_pdf_reffile_id
    return curie_main_ref_file_map


def set_referencefile_mods(referencefile_obj, referencefile_dict):
    del referencefile_dict["reference_id"]
    referencefile_dict["referencefile_mods"] = []
    if referencefile_obj.referencefile_mods:
        for ref_file_mod in referencefile_obj.referencefile_mods:
            ref_file_mod_dict = jsonable_encoder(ref_file_mod)
            del ref_file_mod_dict["mod_id"]
            del ref_file_mod_dict["referencefile_id"]
            if ref_file_mod.mod is not None:
                ref_file_mod_dict["mod_abbreviation"] = ref_file_mod.mod.abbreviation
            else:
                ref_file_mod_dict["mod_abbreviation"] = None
            referencefile_dict["referencefile_mods"].append(ref_file_mod_dict)


def show(db: Session, referencefile_id: int):
    referencefile = read_referencefile_db_obj(db, referencefile_id)
    referencefile_dict = jsonable_encoder(referencefile)
    referencefile_dict["reference_curie"] = db.query(ReferenceModel.curie).filter(
        ReferenceModel.reference_id == referencefile_dict["reference_id"]).one()[0]
    set_referencefile_mods(referencefile_obj=referencefile, referencefile_dict=referencefile_dict)
    return referencefile_dict


def show_all(db: Session, curie_or_reference_id: str) -> List[ReferencefileSchemaRelated]:
    logger.info("Show all referencefiles")
    reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id, load_referencefiles=True)
    reference_files = []
    if reference.referencefiles:
        for ref_file in reference.referencefiles:
            ref_file_dict = jsonable_encoder(ref_file)
            set_referencefile_mods(referencefile_obj=ref_file, referencefile_dict=ref_file_dict)
            reference_files.append(ref_file_dict)
    return reference_files


def patch(db: Session, referencefile_id: int, request):
    referencefile: ReferencefileModel = read_referencefile_db_obj(db, referencefile_id)
    if "display_name" in request or "file_extension" in request or "reference_curie" in request:
        if "display_name" not in request:
            request["display_name"] = referencefile.display_name
        if "file_extension" not in request:
            request["file_extension"] = referencefile.file_extension
        if "reference_curie" not in request:
            request["reference_curie"] = referencefile.reference.curie
        request["display_name"] = find_first_available_display_name(display_name=request["display_name"],
                                                                    file_extension=request["file_extension"],
                                                                    reference_curie=request["reference_curie"], db=db)
    if "reference_curie" in request:
        res = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == request["reference_curie"]).one_or_none()
        if res is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Reference with curie {request.reference_curie} is not available")
        request["reference_id"] = res[0]
        del request["reference_curie"]
    for field, value in request.items():
        setattr(referencefile, field, value)
    db.commit()
    return {"message": messageEnum.updated}


def destroy(db: Session, referencefile_id: int, mod_access: OktaAccess):
    referencefile: ReferencefileModel = read_referencefile_db_obj(db, referencefile_id)
    reference_id = referencefile.reference_id
    file_class = referencefile.file_class
    file_publication_status = referencefile.file_publication_status
    pdf_type = referencefile.pdf_type
    all_mods = set()
    if mod_access == OktaAccess.ALL_ACCESS:
        remove_from_s3_and_db(db, referencefile)
    elif mod_access != OktaAccess.NO_ACCESS:
        for referencefile_mod in referencefile.referencefile_mods:
            if referencefile_mod.mod_id is None:
                all_mods.add('PMC')
            else:
                all_mods.add(referencefile_mod.mod.abbreviation)
                if referencefile_mod.mod.abbreviation == OKTA_ACCESS_MOD_ABBR[mod_access]:
                    destroy_mod_association(db, referencefile_mod.referencefile_mod_id)
    else:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="You are not signed in. Please sign in to delete a file.")

    if file_class == 'main' and file_publication_status == 'final' and (pdf_type is None or pdf_type == 'pdf'):
        cleanup_wft_tet_tags_for_deleted_main_pdf(db, reference_id, all_mods,
                                                  OKTA_ACCESS_MOD_ABBR[mod_access])


def cleanup_wft_tet_tags_for_deleted_main_pdf(db: Session, reference_id, all_mods, access_level):

    mods = set()
    if access_level != 'all_access':
        mods.add(access_level)
    elif len(all_mods) > 0 and 'PMC' not in all_mods:
        mods = all_mods
    else:
        sql_query = text("""
        SELECT m.abbreviation
        FROM mod_corpus_association mca
        JOIN mod m ON mca.mod_id = m.mod_id
        WHERE mca.reference_id = :reference_id
        AND mca.corpus = TRUE
        """)
        rows = db.execute(sql_query, {'reference_id': reference_id}).fetchall()
        mods.update(row[0] for row in rows)

    for mod_abbreviation in mods:
        reset_workflow_tags_after_deleting_main_pdf(db, str(reference_id), mod_abbreviation)
        manual_tet_count = delete_non_manual_tets(db, str(reference_id), mod_abbreviation)
        if manual_tet_count > 0:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="Curated topic and entity tags or automated tags generated from your MOD are associated with this reference. Please check with the curator who added these tags.")


def merge_referencefiles(db: Session,
                         curie_or_reference_id: str,
                         losing_referencefile_id: int,
                         winning_referencefile_id: int):
    """
    :param db:
    :param curie_or_reference_id:
    :param losing_referencefile_id:
    :param winning_referencefile_id:
    :return:

    Transfer referencefile_mods from losing referencefile to winning referencefile, unless already has a referencefile_mod
    with that mod.
    Then delete losing_referencefile.
    Then attach winning referencefile to reference, if it's not already attached to it.
    """
    logger.info("Merging referencefiles")
    reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id, load_referencefiles=True)

    # Lookup both referencefiles
    losing_referencefile = read_referencefile_db_obj(db, losing_referencefile_id)
    winning_referencefile = read_referencefile_db_obj(db, winning_referencefile_id)

    winning_mod_set = {referencefile_mod.mod.abbreviation if referencefile_mod.mod is not None else None
                       for referencefile_mod in winning_referencefile.referencefile_mods}

    for referencefile_mod in losing_referencefile.referencefile_mods:
        mod_abbreviation = referencefile_mod.mod.abbreviation if referencefile_mod.mod is not None else None
        if mod_abbreviation not in winning_mod_set:
            referencefile_mod.referencefile_id = winning_referencefile.referencefile_id
            db.add(referencefile_mod)
    db.commit()
    # call destroy on losing_referencefile or something else because it needs mod_access, and that will remove from s3 ?
    db.delete(losing_referencefile)

    if winning_referencefile.reference_id != reference.reference_id:
        patch(db, winning_referencefile_id, ReferencefileSchemaUpdate(reference_curie=reference.curie).dict(
            exclude_unset=True))

    db.commit()


def file_paths_in_dir(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            file_path = os.path.abspath(os.path.join(dirpath, f))
            if os.path.isfile(file_path):
                yield file_path


def file_upload(db: Session, metadata: dict, file: UploadFile, upload_if_already_converted: bool = False):  # pragma: no cover
    if not metadata["reference_curie"].startswith("AGRKB:101"):
        ref_curie_res = db.query(ReferenceModel.curie).filter(
            ReferenceModel.cross_reference.any(CrossReferenceModel.curie == metadata["reference_curie"])).one_or_none()
        if ref_curie_res is not None:
            metadata["reference_curie"] = ref_curie_res.curie
        else:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="The specified curie is not in the standard Alliance format and no cross "
                                       "references match the specified value.")
        metadata["reference_curie"] = ref_curie_res.curie
    if metadata["mod_abbreviation"]:
        job_type = is_file_upload_blocked(db, metadata["reference_curie"], metadata["mod_abbreviation"])
        if job_type:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"The {job_type} for reference {metadata['reference_curie']} is currently in progress. Please wait until the {job_type} process is complete before uploading any files for this paper.")

    if not upload_if_already_converted and metadata["mod_abbreviation"] and metadata["file_extension"] == 'pdf' and metadata['file_class'] == 'main' and metadata['file_publication_status'] == 'final':
        workflow_tag_atp_id = get_current_workflow_status(db,
                                                          metadata["reference_curie"],
                                                          text_conversion_process_atp_id,
                                                          metadata["mod_abbreviation"])
        if workflow_tag_atp_id == "ATP:0000163":  # file converted to text
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="File already converted to text, use UI if you really need to replace the file.")

    if metadata["file_extension"] in ["tgz", "tar.gz"]:
        temp_dir = tempfile.mkdtemp()
        file_tar = tarfile.open(fileobj=file.file)
        file_tar.extractall(temp_dir)
        error_message = ""
        for file_path in file_paths_in_dir(temp_dir):
            file_name = os.path.basename(file_path)
            single_file_metadata = copy.deepcopy(metadata)
            if file_name.lower().endswith(".tar.gz"):
                single_file_metadata["display_name"] = ".".join(file_name.split(".")[0:-2])
                single_file_metadata["file_extension"] = ".".join(file_name.split(".")[-2:])
            else:
                single_file_metadata["display_name"] = ".".join(file_name.split(".")[0:-1])
                single_file_metadata["file_extension"] = file_name.split(".")[-1]
            try:
                with open(file_path, "rb") as f_in:
                    file_upload_single(db, single_file_metadata, UploadFile(filename=file_name, file=f_in))
            except HTTPException as e:
                error_message += single_file_metadata["display_name"] + "." + single_file_metadata[
                    "file_extension"] + " upload failed: " + e.detail
                if not error_message.endswith("."):
                    error_message += "."
                error_message += " "
        shutil.rmtree(temp_dir, ignore_errors=True)
        if error_message:
            error_message += "Any other files were uploaded"
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=error_message)
    else:
        file_upload_single(db, metadata, file)
    mod_abbreviation = metadata["mod_abbreviation"] if "mod_abbreviation" in metadata else None
    cleanup_old_pdf_file(db, metadata["reference_curie"], mod_abbreviation)
    transition_WFT_for_uploaded_file(db, metadata["reference_curie"], mod_abbreviation,
                                     metadata["file_class"], metadata["file_extension"],
                                     metadata["file_publication_status"])


def transition_WFT_for_uploaded_file(db, reference_curie, mod_abbreviation, file_class, file_extension, file_publication_status):
    logger.info("Transition WFT for uploaded file")
    if file_class == 'main' and file_extension == 'pdf' and file_publication_status == 'final':
        wft_tag_atp_id = file_uploaded_tag_atp_id
    else:
        wft_tag_atp_id = file_upload_in_progress_tag_atp_id

    ref = get_reference(db=db, curie_or_reference_id=reference_curie)

    if mod_abbreviation is None:
        rows = db.execute(text(f"SELECT m.abbreviation "
                               f"FROM mod m, mod_corpus_association mca "
                               f"WHERE m.mod_id = mca.mod_id "
                               f"AND mca.reference_id = {ref.reference_id} "
                               f"AND mca.corpus is True")).mappings().fetchall()
        mods = {x['abbreviation'] for x in rows}
    else:
        mods = {mod_abbreviation}

    for mod in mods:
        try:
            curr_tag_atp_id = get_current_workflow_status(db, reference_curie,
                                                          file_upload_process_atp_id, mod)
            if curr_tag_atp_id is None or (curr_tag_atp_id != wft_tag_atp_id and curr_tag_atp_id != file_uploaded_tag_atp_id):
                transition_to_workflow_status(db, reference_curie, mod, wft_tag_atp_id)
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"An error occurred when transitioning file_upload WFT for reference_curie = {reference_curie}, mod={mod}. error={e}")
    db.commit()


def cleanup_old_pdf_file(db: Session, ref_curie: str, mod_abbreviation):  # pragma: no cover
    ref = db.query(ReferenceModel).filter_by(curie=ref_curie).one_or_none()
    if ref:
        reffiles = db.query(ReferencefileModel).filter_by(
            reference_id=ref.reference_id, file_class='main', file_extension='pdf', file_publication_status='final').order_by(
                ReferencefileModel.file_publication_status, ReferencefileModel.date_updated.desc()).all()

        if len(reffiles) >= 2:
            mod_ids_with_final = {mod.mod_id for reffile in reffiles for mod in reffile.referencefile_mods if
                                  reffile.file_publication_status == 'final'}
            final_files_to_keep = set()
            temp_files_to_delete = []
            final_files_with_none = None

            for reffile in reffiles:
                if reffile.file_publication_status == 'temp':
                    if None in mod_ids_with_final or all(mod.mod_id in mod_ids_with_final for mod in reffile.referencefile_mods):
                        temp_files_to_delete.append(reffile.referencefile_id)
                elif reffile.file_publication_status == 'final':
                    if any(mod.mod_id is None for mod in reffile.referencefile_mods):
                        if final_files_with_none is not None:
                            temp_files_to_delete.append(reffile.referencefile_id)
                        else:
                            final_files_with_none = reffile.referencefile_id
                    for mod in reffile.referencefile_mods:
                        if mod.mod_id is not None:
                            if mod.mod_id in final_files_to_keep:
                                temp_files_to_delete.append(reffile.referencefile_id)
                            else:
                                final_files_to_keep.add(mod.mod_id)

            # Delete the temp files and older final files
            for file_id in temp_files_to_delete:
                destroy(db, file_id, [okta_access for okta_access, mod_abbr in OKTA_ACCESS_MOD_ABBR.items() if
                                      mod_abbr == mod_abbreviation][0])


def create_metadata(db: Session, request: ReferencefileSchemaPost):
    request_dict = request.dict()
    ref_obj = db.query(ReferenceModel).filter(ReferenceModel.curie == request.reference_curie).one_or_none()
    if ref_obj is None:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Reference with curie {request.reference_curie} does not exist")
    del request_dict["reference_curie"]
    request_dict["reference_id"] = ref_obj.reference_id
    mod_abbreviation = request_dict["mod_abbreviation"]
    if mod_abbreviation is not None:
        mod = db.query(ModModel).filter(ModModel.abbreviation == mod_abbreviation).one_or_none()
        if mod is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Mod with abbreviation {request.mod_abbreviation} does not exist")
    del request_dict["mod_abbreviation"]
    new_ref_file_obj = ReferencefileModel(**request_dict)
    db.add(new_ref_file_obj)
    db.commit()
    create_mod_connection(db, ReferencefileModSchemaPost(referencefile_id=new_ref_file_obj.referencefile_id,
                                                         mod_abbreviation=mod_abbreviation))
    return new_ref_file_obj.referencefile_id


def find_first_available_display_name(display_name: str, file_extension: str, reference_curie: str, db: Session):
    original_name = display_name
    for counter in count(start=1, step=1):
        ref_file_same_name = db.query(ReferencefileModel).filter(
            and_(
                ReferencefileModel.display_name == display_name,
                ReferencefileModel.file_extension == file_extension,
                ReferencefileModel.reference.has(ReferenceModel.curie == reference_curie)
            )
        ).one_or_none()
        if ref_file_same_name is None:
            break
        display_name = f"{original_name}_{counter}"
    return display_name


def file_upload_single(db: Session, metadata: dict, file: UploadFile):  # pragma: no cover
    mod_abbreviation = metadata["mod_abbreviation"] if "mod_abbreviation" in metadata else None
    file.file.seek(0)
    md5sum_hash = hashlib.md5()
    for byte_block in iter(lambda: file.file.read(4096), b""):
        md5sum_hash.update(byte_block)
    md5sum = md5sum_hash.hexdigest()
    folder = get_s3_folder_from_md5sum(md5sum)
    referencefile_instance: ReferencefileModel = db.query(ReferencefileModel).filter(
        and_(
            ReferencefileModel.md5sum == md5sum,
            ReferencefileModel.reference.has(ReferenceModel.curie == metadata["reference_curie"])
        )
    ).one_or_none()
    if referencefile_instance is not None:
        # the file already exists, and it's already associated with the provided reference, but the metadata in the
        # request may be incompatible with the one in the db. The metadata in the db will not be modified and a new
        # connection between the file and the mod will be created. See below for special cases for WB
        if mod_abbreviation == "WB":
            # if a final file is uploaded by WB and the same file is in the system as temp, then set it to final
            if metadata["file_publication_status"] == "final" and referencefile_instance.file_publication_status == "temp":
                referencefile_instance.file_publication_status = "final"  # type: ignore
            # If WB uploads a temp and the same file is already present but not for WB, then set the status to temp
            elif "WB" not in {referencefile_mod.mod.abbreviation for referencefile_mod in
                              referencefile_instance.referencefile_mods if referencefile_mod.mod is not None} and \
                    metadata["file_publication_status"] == "temp" and referencefile_instance.file_publication_status == "final":
                referencefile_instance.file_publication_status = "temp"  # type: ignore
            db.commit()
        if all(referencefile_mod.mod.abbreviation != mod_abbreviation for referencefile_mod in
               referencefile_instance.referencefile_mods if referencefile_mod.mod is not None):
            create_mod_connection(db, ReferencefileModSchemaPost(referencefile_id=referencefile_instance.referencefile_id,
                                                                 mod_abbreviation=mod_abbreviation))
    else:
        # 2 possible cases here: i) an entry with the same md5sum does not exist; ii) same md5sum exists, but it's
        # associated with a different curie (same file content for different files or same files). In both cases we need
        # to create a new referencefile and associate it with the specified ref and mod

        # check if a different version of the same file (same display_name and file_extension) is already associated
        # with the same reference. Add _num to the display name to upload different versions of the same file
        metadata["display_name"] = find_first_available_display_name(
            display_name=metadata["display_name"], file_extension=metadata["file_extension"],
            reference_curie=metadata["reference_curie"], db=db)
        create_request = ReferencefileSchemaPost(md5sum=md5sum, **metadata)
        create_metadata(db, create_request)
        # check if md5sum is the only one in the db before uploading to s3
        ref_file_by_md5sum_count = db.query(ReferencefileModel).filter(ReferencefileModel.md5sum == md5sum).count()
        if ref_file_by_md5sum_count == 1:
            file.file.seek(0)
            temp_file_name = metadata["display_name"] + "." + metadata["file_extension"] + ".gz"
            with gzip.open(temp_file_name, 'wb') as f_out:
                shutil.copyfileobj(file.file, f_out)
            client = boto3.client('s3')
            env_state = os.environ.get("ENV_STATE", "")
            extra_args = {'StorageClass': 'GLACIER_IR'} if env_state == "prod" else {'StorageClass': 'STANDARD'}
            with open(temp_file_name, 'rb') as gzipped_file:
                upload_file_to_bucket(s3_client=client, file_obj=gzipped_file, bucket="agr-literature", folder=folder,
                                      object_name=md5sum + ".gz", ExtraArgs=extra_args)
            os.remove(temp_file_name)
    return md5sum


def download_file(db: Session, referencefile_id: int, mod_access: OktaAccess,  # pragma: no cover
                  use_in_api: bool = True):  # pragma: no cover
    referencefile = read_referencefile_db_obj(db, referencefile_id)

    user_permission = False
    if referencefile.reference.copyright_license:
        user_permission = referencefile.reference.copyright_license.open_access

    if user_permission is False:
        if mod_access != OktaAccess.NO_ACCESS:
            if mod_access == OktaAccess.ALL_ACCESS or any(
                    ref_file_mod.mod.abbreviation == OKTA_ACCESS_MOD_ABBR[mod_access] if ref_file_mod.mod is not None else
                    True for ref_file_mod in referencefile.referencefile_mods):
                user_permission = True

    if user_permission is True:
        md5sum = referencefile.md5sum
        display_name = referencefile.display_name + "." + referencefile.file_extension
        folder = get_s3_folder_from_md5sum(md5sum)
        object_name = folder + "/" + md5sum + ".gz"
        download_file_from_s3(md5sum + ".gz", bucketname="agr-literature", s3_file_location=object_name)
        with gzip.open(md5sum + ".gz", 'rb') as f_in, open(display_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(md5sum + ".gz")
        if use_in_api:
            return FileResponse(path=display_name, filename=display_name, media_type="application/octet-stream",
                                background=BackgroundTask(cleanup, display_name))
        else:
            with open(display_name, 'rb') as file:
                file_content = file.read()
            os.remove(display_name)
            return file_content

    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                        detail="The current user does not have permissions to get the requested file url. "
                               "The associated paper is not available for free access.")


def cleanup(file_path):
    os.remove(file_path)


def download_additional_files_tarball(db: Session, reference_id, mod_access: OktaAccess):
    ref_curie = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == reference_id).one_or_none()
    if not ref_curie:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="No reference found for the specified reference_id")
    ref_curie = ref_curie.curie
    all_referencefile_supp = db.query(ReferencefileModel).options(
        subqueryload(
            ReferencefileModel.referencefile_mods)
    ).filter(
        and_(
            ReferencefileModel.reference_id == reference_id,
            ReferencefileModel.file_class != "main",
            ReferencefileModel.file_class != "correspondence",
            or_(
                ReferencefileModel.reference.has(ReferenceModel.copyright_license.has(
                    CopyrightLicenseModel.open_access == True)), # noqa
                ReferencefileModel.referencefile_mods.any(
                    or_(
                        ReferencefileModAssociationModel.mod == None, # noqa
                        ReferencefileModAssociationModel.mod.has(
                            ModModel.abbreviation == OKTA_ACCESS_MOD_ABBR[mod_access])
                    )
                )
            )
        )
    ).all()

    tar_file_path = ref_curie.replace(":", "_") + "_additional_files.tar.gz"
    os.makedirs("tarball_tmp", exist_ok=True)
    with tarfile.open(tar_file_path, "w:gz") as tar:
        for referencefile in all_referencefile_supp:
            md5sum = referencefile.md5sum
            folder = get_s3_folder_from_md5sum(md5sum)
            object_name = folder + "/" + md5sum + ".gz"
            tmp_file_gz_path = "tarball_tmp/" + referencefile.display_name + ".gz"
            tmp_file_name = referencefile.display_name + "." + referencefile.file_extension
            tmp_file_path = "tarball_tmp/" + tmp_file_name
            download_file_from_s3(tmp_file_gz_path, "agr-literature", object_name)
            with gzip.open(tmp_file_gz_path, 'rb') as f_in, open(tmp_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            tar.add(tmp_file_path, arcname=tmp_file_name)
            os.remove(tmp_file_path)
    shutil.rmtree("tarball_tmp")
    return FileResponse(path=tar_file_path, filename=tar_file_path, media_type="application/gzip",
                        background=BackgroundTask(cleanup, tar_file_path))
