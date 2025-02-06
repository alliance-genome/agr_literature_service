import logging
import os
from io import BytesIO

import requests
from fastapi import UploadFile
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from lxml import etree

from agr_literature_service.api.crud.referencefile_crud import get_main_pdf_referencefile_id, download_file, file_upload
from agr_literature_service.api.crud.workflow_tag_crud import get_jobs, job_change_atp_code
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.models import ModModel, ReferencefileModel, ReferenceModel, CrossReferenceModel
from agr_literature_service.api.routers.okta_utils import OktaAccess
from agr_literature_service.lit_processing.utils.report_utils import send_report


logger = logging.getLogger(__name__)


def convert_pdf_with_grobid(file_content):
    grobid_api_url = os.environ.get("PDF2TEI_API_URL",
                                    "https://grobid.alliancegenome.org/api/processFulltextDocument")
    # Send the file content to the GROBID API
    response = requests.post(grobid_api_url, files={'input': ("file", file_content)})
    return response


def main():
    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()
    limit = 1000
    offset = 0
    all_jobs = []
    logger.info("Started loading all text conversion jobs.")
    seen_wf_tag_ids = set()
    while jobs := get_jobs(db, "text_convert_job", limit, offset):
        for job in jobs:
            if job["reference_workflow_tag_id"] in seen_wf_tag_ids:
                logger.warning("Duplicate job found. Skipping.")
            else:
                all_jobs.append(job)
                seen_wf_tag_ids.add(job["reference_workflow_tag_id"])
        offset += limit
        logger.info(f"Loaded batch of {str(len(jobs))} jobs. Total jobs loaded: {str(len(all_jobs))}")
    logger.info("Finished loading all text conversion jobs.")
    mod_abbreviation_from_mod_id = {}
    objects_with_errors = []
    for job in all_jobs:
        add_to_error_list = True
        ref_id = job['reference_id']
        reference_workflow_tag_id = job['reference_workflow_tag_id']
        mod_id = job['mod_id']
        reference_curie = job['reference_curie']
        if mod_id not in mod_abbreviation_from_mod_id:
            mod_abbreviation = db.query(ModModel.abbreviation).filter(ModModel.mod_id == mod_id).one().abbreviation
            mod_abbreviation_from_mod_id[mod_id] = mod_abbreviation
        else:
            mod_abbreviation = mod_abbreviation_from_mod_id[mod_id]
        ref_file_id_to_convert = get_main_pdf_referencefile_id(db=db, curie_or_reference_id=ref_id,
                                                               mod_abbreviation=mod_abbreviation)
        logger.info(f"processing reference {reference_curie}")
        if ref_file_id_to_convert:
            ref_file_obj: ReferencefileModel = db.query(ReferencefileModel).filter(
                ReferencefileModel.referencefile_id == ref_file_id_to_convert).one()
            file_content = download_file(db=db, referencefile_id=ref_file_id_to_convert,
                                         mod_access=OktaAccess.ALL_ACCESS, use_in_api=False)
            response = convert_pdf_with_grobid(file_content)
            # Check the response
            if response.status_code == 200:
                logger.info(f"referencefile with ID {str(ref_file_id_to_convert)} successfully processed by GROBID.")
                metadata = {
                    "reference_curie": reference_curie,
                    "display_name": ref_file_obj.display_name,
                    "file_class": "tei",
                    "file_publication_status": "final",
                    "file_extension": "tei",
                    "pdf_type": None,
                    "is_annotation": None,
                    "mod_abbreviation": mod_abbreviation
                }
                root = etree.fromstring(response.content)  # Check for empty elements that indicate failure
                title = root.xpath('//tei:title[@level="a"]', namespaces={'tei': 'http://www.tei-c.org/ns/1.0'})
                if (response.content == "[NO_BLOCKS] PDF parsing resulted in empty content" or title is None
                        or title[0].text is None):
                    job_change_atp_code(db, reference_workflow_tag_id, "on_failed")
                else:
                    file_upload(db=db, metadata=metadata, file=UploadFile(file=BytesIO(response.content),
                                                                          filename=ref_file_obj.display_name),
                                upload_if_already_converted=True)
                    add_to_error_list = False
                    job_change_atp_code(db, reference_workflow_tag_id, "on_success")
            elif response.status_code == 500:
                logger.error(f"Cannot convert referencefile with ID {str(ref_file_id_to_convert)}: {response.text}")
                job_change_atp_code(db, reference_workflow_tag_id, "on_failed")
            else:
                logger.error(f"Failed to process referencefile with ID {ref_file_id_to_convert}. "
                             f"Will retry in the future. Status code: {response.status_code}")
            if add_to_error_list:
                mod_cross_ref = db.query(CrossReferenceModel).join(
                    ReferenceModel, CrossReferenceModel.reference_id == ReferenceModel.reference_id
                ).filter(
                    ReferenceModel.curie == reference_curie,
                    CrossReferenceModel.curie_prefix == mod_abbreviation
                ).one()
                error_object = {
                    "reference_curie": reference_curie,
                    "display_name": ref_file_obj.display_name,
                    "file_extension": "tei",
                    "mod_abbreviation": mod_abbreviation,
                    "mod_cross_ref": mod_cross_ref.curie
                }
                objects_with_errors.append(error_object)
    error_message = ''
    for error_object in objects_with_errors:
        error_message += f"{error_object['mod_abbreviation']}\t{error_object['mod_cross_ref']}\t"
        error_message += f"{error_object['reference_curie']}\t{error_object['display_name']}.{error_object['file_extension']}\n"
    if error_message != '':
        subject = "pdf2tei conversion errors"
        send_report(subject, error_message)


if __name__ == '__main__':
    main()
