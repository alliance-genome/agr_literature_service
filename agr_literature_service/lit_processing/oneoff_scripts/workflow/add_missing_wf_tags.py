import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from agr_literature_service.api.database.config import SQLALCHEMY_DATABASE_URL
from agr_literature_service.api.models import WorkflowTagModel


logging.basicConfig(level=logging.INFO)


logger = logging.getLogger(__name__)

batch_size = 200


def add_missing_text_conversion_needed_tags(db: Session):
    count = 0
    new_tags = []

    existing_text_conversion_needed_tags = db.query(WorkflowTagModel.reference_id, WorkflowTagModel.mod_id).filter(
        WorkflowTagModel.workflow_tag_id == "ATP:0000162"
    ).all()
    existing_ref_id_mod_id_set = set([(existing_tag.reference_id, existing_tag.mod_id) for existing_tag in
                                      existing_text_conversion_needed_tags])

    query = db.query(
        WorkflowTagModel.reference_id,
        WorkflowTagModel.mod_id
    ).filter(WorkflowTagModel.workflow_tag_id == "ATP:0000134")

    for existing_file_uploaded_wf_tag in query.all():
        if ((existing_file_uploaded_wf_tag.reference_id, existing_file_uploaded_wf_tag.mod_id) not in
                existing_ref_id_mod_id_set):
            text_conv_needed_wf_tag = WorkflowTagModel(
                reference_id=existing_file_uploaded_wf_tag.reference_id,
                mod_id=existing_file_uploaded_wf_tag.mod_id,
                workflow_tag_id="ATP:0000162")
            new_tags.append(text_conv_needed_wf_tag)
            count += 1
            if count % batch_size == 0:
                logger.info(f"Committing batch of {batch_size}")
                db.bulk_save_objects(new_tags)
                db.commit()
                new_tags = []
    if new_tags:
        db.bulk_save_objects(new_tags)
        db.commit()


def main():
    engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"options": "-c timezone=utc"})
    new_session = sessionmaker(bind=engine, autoflush=True)
    db = new_session()

    add_missing_text_conversion_needed_tags(db=db)


if __name__ == '__main__':
    main()
