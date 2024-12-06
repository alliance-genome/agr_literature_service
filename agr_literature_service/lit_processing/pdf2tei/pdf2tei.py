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
from agr_literature_service.api.models import ModModel, ReferencefileModel, ReferenceModel
from agr_literature_service.api.routers.okta_utils import OktaAccess

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
    jobs = get_jobs(db, "text_convert_job")
    for job in jobs:
        ref_id = job['reference_id']
        reference_workflow_tag_id = job['reference_workflow_tag_id']
        mod_id = job['mod_id']
        reference_curie = db.query(ReferenceModel.curie).filter(ReferenceModel.reference_id == ref_id).one().curie
        mod_abbreviation = db.query(ModModel.abbreviation).filter(ModModel.mod_id == mod_id).one().abbreviation
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
                    job_change_atp_code(db, reference_workflow_tag_id, "on_success")
            else:
                logger.error(f"Failed to process referencefile with ID {ref_file_id_to_convert}. "
                             f"Status code: {response.status_code}")


if __name__ == '__main__':
    main()
