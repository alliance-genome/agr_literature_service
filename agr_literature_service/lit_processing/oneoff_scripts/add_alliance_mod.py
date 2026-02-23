from sqlalchemy import select
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModModel


def add_alliance_mod():

    db = create_postgres_session(False)

    try:
        # check if already exists
        existing = db.query(ModModel).filter_by(abbreviation="alliance").one_or_none()

        if existing:
            print("Alliance mod already exists. Skipping.")
            return

        # get a valid user from existing rows (avoids FK / NOT NULL issues)
        user = db.execute(
            select(ModModel.created_by).where(ModModel.created_by.isnot(None)).limit(1)
        ).scalar_one_or_none()

        new_mod = ModModel(
            abbreviation="alliance",
            short_name="Alliance",
            full_name="Alliance of Genome Resources",
            taxon_ids=None,
            created_by=user,
            updated_by=user,
        )

        db.add(new_mod)
        db.commit()

        print("Alliance mod added successfully.")

    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    add_alliance_mod()
