"""Backfill file_classes column on existing ml_model rows.

Rules:
  - task_type = 'biocuration_topic_classification' → ['main']
  - task_type = 'biocuration_entity_extraction'    → ['main', 'supplement']
  - all other task_types                           → NULL (metadata-only models)
"""
import logging
from os import environ

from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TASK_TYPE_FILE_CLASSES = {
    "biocuration_topic_classification": ["main"],
    "biocuration_entity_extraction": ["main", "supplement"],
}


def backfill_file_classes():
    db_host = environ.get("PSQL_HOST", "localhost")
    db_port = environ.get("PSQL_PORT", "5432")
    db_name = environ.get("PSQL_DATABASE", "literature")
    db_user = environ.get("PSQL_USERNAME", "")
    db_pass = environ.get("PSQL_PASSWORD", "")
    url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(url)

    with engine.begin() as conn:
        for task_type, file_classes in TASK_TYPE_FILE_CLASSES.items():
            result = conn.execute(
                text(
                    "UPDATE ml_model SET file_classes = :file_classes "
                    "WHERE task_type = :task_type AND file_classes IS NULL"
                ),
                {"file_classes": file_classes, "task_type": task_type}
            )
            logger.info(
                "Set file_classes=%s for task_type='%s': %d rows updated",
                file_classes, task_type, result.rowcount
            )

        # Report any rows that remain NULL (metadata-only models)
        remaining = conn.execute(
            text("SELECT ml_model_id, task_type, topic FROM ml_model WHERE file_classes IS NULL")
        ).fetchall()
        if remaining:
            logger.info(
                "%d models left with file_classes=NULL (metadata-only):", len(remaining)
            )
            for row in remaining:
                logger.info("  ml_model_id=%s task_type='%s' topic='%s'", row[0], row[1], row[2])
        else:
            logger.info("All models now have file_classes set.")


if __name__ == "__main__":
    backfill_file_classes()
