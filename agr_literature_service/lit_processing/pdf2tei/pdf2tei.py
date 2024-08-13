import logging
import os
from typing import List

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from agr_literature_service.api.crud.referencefile_crud import get_main_pdf_referencefile_id, download_file
from agr_literature_service.api.crud.workflow_tag_crud import get_ref_ids_with_workflow_status
from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.models import ModModel
from agr_literature_service.api.routers.okta_utils import OktaAccess


logger = logging.getLogger(__name__)


def main():
    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()
    mod_abbreviations = [mod.abbreviation for mod in db.query(ModModel.abbreviation).all()]
    for mod_abbreviation in mod_abbreviations:
        for ref_id in get_ref_ids_with_workflow_status(db=db, workflow_atp_id="", mod_abbreviation=mod_abbreviation):
            ref_file_id_to_convert = get_main_pdf_referencefile_id(db=db, curie_or_reference_id=ref_id,
                                                                   mod_abbreviation=mod_abbreviation)
            file_content = download_file(db=db, referencefile_id=ref_file_id_to_convert,
                                         mod_access=OktaAccess.ALL_ACCESS, use_in_api=False)
            # Define the GROBID API endpoint
            grobid_api_url = os.environ.get("PDF2TEI_API_URL",
                                            "https://grobid.alliancegenome.org/api/processFulltextDocument")

            # Send the file content to the GROBID API
            response = requests.post(grobid_api_url, files={'input': ("file", file_content)})

            # Check the response
            if response.status_code == 200:
                logger.info("File successfully processed by GROBID.")
                return response.content
            else:
                logger.error(f"Failed to process file. Status code: {response.status_code}")
                return None


if __name__ == '__main__':
    main()
