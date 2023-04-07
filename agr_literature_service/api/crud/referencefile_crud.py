import copy
import gzip
import hashlib
import logging
import os
import shutil
import tarfile
import tempfile
from typing import List

import boto3
from fastapi import HTTPException, status, UploadFile
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session, subqueryload
from starlette.background import BackgroundTask
from starlette.responses import FileResponse

from agr_literature_service.api.crud.reference_utils import get_reference
from agr_literature_service.api.crud.referencefile_utils import read_referencefile_db_obj, \
    create as create_metadata, get_s3_folder_from_md5sum
from agr_literature_service.api.crud.referencefile_mod_utils import create as create_mod_connection
from agr_literature_service.api.models import ReferenceModel, ReferencefileModel, ReferencefileModAssociationModel, \
    ModModel, CopyrightLicenseModel, CrossReferenceModel
from agr_literature_service.api.routers.okta_utils import OktaAccess, OKTA_ACCESS_MOD_ABBR
from agr_literature_service.api.s3.delete import delete_file_in_bucket
from agr_literature_service.api.s3.upload import upload_file_to_bucket
from agr_literature_service.api.schemas.referencefile_mod_schemas import ReferencefileModSchemaPost
from agr_literature_service.api.schemas.referencefile_schemas import ReferencefileSchemaPost, ReferencefileSchemaRelated
from agr_literature_service.api.schemas.response_message_schemas import messageEnum
from agr_literature_service.lit_processing.utils.s3_utils import download_file_from_s3

logger = logging.getLogger(__name__)


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
    reference = get_reference(db=db, curie_or_reference_id=curie_or_reference_id, load_referencefiles=True)
    reference_files = []
    if reference.referencefiles:
        for ref_file in reference.referencefiles:
            ref_file_dict = jsonable_encoder(ref_file)
            set_referencefile_mods(referencefile_obj=ref_file, referencefile_dict=ref_file_dict)
            reference_files.append(ref_file_dict)
    return reference_files


def patch(db: Session, referencefile_id: int, request):
    referencefile = read_referencefile_db_obj(db, referencefile_id)
    if "reference_curie" in request:
        res = db.query(ReferenceModel.reference_id).filter(
            ReferenceModel.curie == request.reference_curie).one_or_none()
        if res is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=f"Reference with curie {request.reference_curie} is not available")

        request["reference_id"] = res[0]
        del request["reference_curie"]
    for field, value in request.items():
        setattr(referencefile, field, value)
    db.commit()
    return {"message": messageEnum.updated}


def remove_file_from_s3(md5sum: str):  # pragma: no cover
    folder = get_s3_folder_from_md5sum(md5sum)
    client = boto3.client('s3')
    if not delete_file_in_bucket(s3_client=client, bucket="agr-literature", folder=folder, object_name=md5sum + ".gz"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"File with md5sum {md5sum} is not available")


def destroy(db: Session, referencefile_id: int):
    referencefile = read_referencefile_db_obj(db, referencefile_id)
    if len(referencefile.reference.referencefiles) == 1:
        if os.environ.get("ENV_STATE", "test") != "test":
            remove_file_from_s3(referencefile.md5sum)
    db.delete(referencefile)
    db.commit()


def file_paths_in_dir(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            file_path = os.path.abspath(os.path.join(dirpath, f))
            if os.path.isfile(file_path):
                yield file_path


def file_upload(db: Session, metadata: dict, file: UploadFile):  # pragma: no cover
    if not metadata["reference_curie"].startswith("AGRKB:101"):
        ref_curie_res = db.query(ReferenceModel.curie).filter(
            ReferenceModel.cross_reference.any(CrossReferenceModel.curie == metadata["reference_curie"])).one_or_none()
        if metadata["reference_curie"] is None:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail="The specified curie is not in the standard Alliance format and no cross "
                                       "references match the specified value.")
        metadata["reference_curie"] = ref_curie_res.curie
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

    cleanup_temp_file(db, metadata["reference_curie"])


def cleanup_temp_file(db: Session, ref_curie: str):  # pragma: no cover
    ref = db.query(ReferenceModel).filter_by(curie=ref_curie).one_or_none()
    if ref:
        reffiles = db.query(ReferencefileModel).filter_by(
            reference_id=ref.reference_id, file_class='main', file_extension='pdf').order_by(
                ReferencefileModel.file_publication_status).all()

        if len(reffiles) >= 2:
            modsWithFinal = []
            pmcFinalPDF = False
            for x in reffiles:
                if x.file_publication_status == 'final':
                    for m in x.referencefile_mods:
                        if m.mod_id:
                            if m.mod_id not in modsWithFinal:
                                modsWithFinal.append(m.mod_id)
                        else:
                            pmcFinalPDF = True
                if x.file_publication_status == 'temp':
                    toDelete = False
                    if pmcFinalPDF is True:
                        toDelete = True
                    else:
                        for m in x.referencefile_mods:
                            if m.mod_id in modsWithFinal:
                                toDelete = True
                    if toDelete is True:
                        try:
                            db.delete(x)
                            remove_file_from_s3(x.md5sum)
                            db.commit()
                        except Exception as e:
                            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                                detail=f"An error occurred when deleting temp pdf file. {e}")


def file_upload_single(db: Session, metadata: dict, file: UploadFile):  # pragma: no cover
    file.file.seek(0)
    md5sum_hash = hashlib.md5()
    for byte_block in iter(lambda: file.file.read(4096), b""):
        md5sum_hash.update(byte_block)
    md5sum = md5sum_hash.hexdigest()
    folder = get_s3_folder_from_md5sum(md5sum)
    referencefile = db.query(ReferencefileModel).filter(
        and_(
            ReferencefileModel.md5sum == md5sum,
            ReferencefileModel.reference.has(ReferenceModel.curie == metadata["reference_curie"])
        )
    ).one_or_none()
    if referencefile is not None:
        # the file already exists, and it's already associated with the provided reference, but the metadata in the
        # request may be incompatible with the one in the db. If the metadata is not compatible, reject the request,
        # otherwise add the mod association
        if referencefile.file_class != metadata["file_class"] or \
                referencefile.file_publication_status != metadata["file_publication_status"] or \
                referencefile.file_extension != metadata["file_extension"] or \
                referencefile.pdf_type != metadata["pdf_type"]:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                                detail=f"Metadata for the provided md5sum and reference curie "
                                       f"{referencefile.reference.curie} is already present in the"
                                       f" system with name {referencefile.display_name}.{referencefile.file_extension} "
                                       f"but it's not compatible with the provided metadata, so no new "
                                       "connection to the provided mod has been created.")
        mod_abbreviation = metadata["mod_abbreviation"] if "mod_abbreviation" in metadata else None
        create_mod_connection(db, ReferencefileModSchemaPost(referencefile_id=referencefile.referencefile_id,
                                                             mod_abbreviation=mod_abbreviation))
    else:
        # 2 possible cases here: i) an entry with the same md5sum does not exist; ii) same md5sum exists, but it's
        # associated with a different curie (same file content for different files or same files). In both cases we need
        # to create a new referencefile and associate it with the specified ref and mod
        create_request = ReferencefileSchemaPost(md5sum=md5sum, **metadata)
        create_metadata(db, create_request)
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


def download_file(db: Session, referencefile_id: int, mod_access: OktaAccess):  # pragma: no cover
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
        return FileResponse(path=display_name, filename=display_name, media_type="application/octet-stream",
                            background=BackgroundTask(cleanup, display_name))

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
