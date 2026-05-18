"""
Update image permissions based on 2026 publisher permission grants.

This script updates the image_permission table with:
1. Permission name and can_display_images changes
2. permission_doc_url links to Google Drive documentation

Run after applying the migration that adds permission_doc_url column:
    alembic upgrade head

Usage:
    python update_image_permissions_2026.py
"""

import logging
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Permission updates based on 2026 grants
PERMISSION_UPDATES = [
    {
        "image_permission_id": 474,
        "name": "Cold Spring Harbor Laboratory Press - Blanket Permission",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/1lDAT5CzRsnq5MSOIqV_EB_rrV7-5PbuQ1UEa5qEfwis/edit?tab=t.0",
    },
    {
        "image_permission_id": 478,
        "name": "Cold Spring Harbor Lab Press - Blanket Permission",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/1lDAT5CzRsnq5MSOIqV_EB_rrV7-5PbuQ1UEa5qEfwis/edit?tab=t.0",
    },
    {
        "image_permission_id": 460,
        "name": "Charles Univ Prague (Folia Biologica) - CC BY | Open Access",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/1JXYczY7PJ812Wu0aB2OSULrfVht6HkEUS_fGLHJ1j5Y/edit?tab=t.0",
    },
    {
        "image_permission_id": 430,
        "name": "Portland Press LTD - Blanket Permission",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/1gsllpniLR4fxNGb1SKznUMOLG3pBCPGh6N_YCuNjtPs/edit?tab=t.0",
    },
    {
        "image_permission_id": 477,
        "name": "Genetic Society of America (GSA), Oxford University Press? - Blanket Permission",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/1MsriIc9nXmu5ne0h3rrwp2V_afKXD02LltcfxsqBdQU/edit?tab=t.0",
    },
    {
        "image_permission_id": 490,
        "name": "The Rockefeller University Press - Contract",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/1tJEAGm7a-Ugl6rzKxJeoknq1Pu02KkuPpvzTZ6AD350/edit?tab=t.0",
    },
    {
        "image_permission_id": 495,
        "name": "The Rockefeller University Press - Open Access",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/1tJEAGm7a-Ugl6rzKxJeoknq1Pu02KkuPpvzTZ6AD350/edit?tab=t.0",
    },
    {
        "image_permission_id": 499,
        "name": "Society for Neuroscience - CC BY 4.0 and CC-BY-NC-SA | Open Access",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/1hHdHAvaWE0zjVa2QcLqMa40pUpKy0U9rFMSKQnCyfZ4/edit?tab=t.0",
    },
    {
        "image_permission_id": 459,
        "name": "Society for Neuroscience - Open Access",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/1hHdHAvaWE0zjVa2QcLqMa40pUpKy0U9rFMSKQnCyfZ4/edit?tab=t.0",
    },
    {
        "image_permission_id": 491,
        "name": "The Company of Biologists - Contract",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/12e6R6i4R2AHCwD-sjQs0MZqhSr-RPIJkO9wHEQFhKIk/edit?tab=t.0",
    },
    {
        "image_permission_id": 429,
        "name": "The Company of Biologists - Open Access",
        "can_display_images": True,
        "permission_doc_url": "https://docs.google.com/document/d/12e6R6i4R2AHCwD-sjQs0MZqhSr-RPIJkO9wHEQFhKIk/edit?tab=t.0",
    },
    {
        "image_permission_id": 450,
        "name": "The Company of Biologists - Permission Granted (subset)",
        "can_display_images": False,  # Keep as subset
        "permission_doc_url": "https://docs.google.com/document/d/12e6R6i4R2AHCwD-sjQs0MZqhSr-RPIJkO9wHEQFhKIk/edit?tab=t.0",
    },
]

# New permission to add
NEW_PERMISSION = {
    "name": "WormBook - Blanket Permission",
    "permission_text": "Permission granted by WormBook (Paul Sternberg) for reproduction of figures.",
    "can_display_images": True,
    "permission_doc_url": None,  # No separate doc, mentioned in main 2026 doc
}


def get_database_url():
    """Get database URL from environment or use default."""
    import os
    host = os.environ.get("PSQL_HOST", "localhost")
    port = os.environ.get("PSQL_PORT", "5432")
    database = os.environ.get("PSQL_DATABASE", "literature")
    username = os.environ.get("PSQL_USERNAME", "postgres")
    password = os.environ.get("PSQL_PASSWORD", "postgres")
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"


def update_existing_permissions(session: Session):
    """Update existing image_permission records."""
    for update in PERMISSION_UPDATES:
        image_permission_id = update["image_permission_id"]

        # Check if record exists
        result = session.execute(
            text("SELECT image_permission_id FROM image_permission WHERE image_permission_id = :id"),
            {"id": image_permission_id}
        ).fetchone()

        if not result:
            logger.warning(f"Permission ID {image_permission_id} not found, skipping")
            continue

        # Update the record
        session.execute(
            text("""
                UPDATE image_permission
                SET name = :name,
                    can_display_images = :can_display_images,
                    permission_doc_url = :permission_doc_url,
                    date_updated = :date_updated
                WHERE image_permission_id = :id
            """),
            {
                "id": image_permission_id,
                "name": update["name"],
                "can_display_images": update["can_display_images"],
                "permission_doc_url": update["permission_doc_url"],
                "date_updated": datetime.utcnow(),
            }
        )
        logger.info(f"Updated permission ID {image_permission_id}: {update['name']}")


def add_new_permission(session: Session):
    """Add new WormBook permission if it doesn't exist."""
    # Check if WormBook permission already exists
    result = session.execute(
        text("SELECT image_permission_id FROM image_permission WHERE name LIKE '%WormBook%'")
    ).fetchone()

    if result:
        logger.info(f"WormBook permission already exists (ID: {result[0]}), updating...")
        session.execute(
            text("""
                UPDATE image_permission
                SET can_display_images = :can_display_images,
                    permission_text = :permission_text,
                    date_updated = :date_updated
                WHERE image_permission_id = :id
            """),
            {
                "id": result[0],
                "can_display_images": NEW_PERMISSION["can_display_images"],
                "permission_text": NEW_PERMISSION["permission_text"],
                "date_updated": datetime.utcnow(),
            }
        )
    else:
        session.execute(
            text("""
                INSERT INTO image_permission (name, permission_text, can_display_images, permission_doc_url, date_created)
                VALUES (:name, :permission_text, :can_display_images, :permission_doc_url, :date_created)
            """),
            {
                "name": NEW_PERMISSION["name"],
                "permission_text": NEW_PERMISSION["permission_text"],
                "can_display_images": NEW_PERMISSION["can_display_images"],
                "permission_doc_url": NEW_PERMISSION["permission_doc_url"],
                "date_created": datetime.utcnow(),
            }
        )
        logger.info(f"Added new permission: {NEW_PERMISSION['name']}")


def verify_updates(session: Session):
    """Verify the updates were applied correctly."""
    result = session.execute(
        text("""
            SELECT image_permission_id, name, can_display_images, permission_doc_url
            FROM image_permission
            WHERE name LIKE '%Cold Spring Harbor%'
               OR name LIKE '%Folia%'
               OR name LIKE '%Charles Univ%'
               OR name LIKE '%Portland%'
               OR name LIKE '%WormBook%'
               OR name LIKE '%GSA%'
               OR name LIKE '%Genetic Society%'
               OR name LIKE '%Rockefeller%'
               OR name LIKE '%Neuroscience%'
               OR name LIKE '%Company of Biologists%'
            ORDER BY name
        """)
    ).fetchall()

    logger.info("\n=== Updated Permissions ===")
    for row in result:
        logger.info(f"ID: {row[0]}, Name: {row[1]}, Can Display: {row[2]}, Doc URL: {'Yes' if row[3] else 'No'}")


def main():
    """Main function to run the updates."""
    database_url = get_database_url()
    engine = create_engine(database_url)

    with Session(engine) as session:
        try:
            logger.info("Starting image permission updates...")

            update_existing_permissions(session)
            add_new_permission(session)

            session.commit()
            logger.info("All updates committed successfully!")

            verify_updates(session)

        except Exception as e:
            session.rollback()
            logger.error(f"Error updating permissions: {e}")
            raise


if __name__ == "__main__":
    main()
