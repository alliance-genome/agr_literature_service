"""
fix_author_order.py

Scan all references in the authors table and, for each one whose
author.order values aren’t exactly 1…N in ascending order, renumber
them sequentially to remove gaps or duplicates.
"""
import logging

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import AuthorModel

logging.basicConfig(format='%(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def fix_author_orders():

    db = create_postgres_session(False)

    # grab every distinct reference_id
    reference_ids = db.query(AuthorModel.reference_id).distinct().all()
    for (ref_id,) in reference_ids:
        # load all authors for this paper, ordered by current order, then by PK to break ties
        authors = (
            db.query(AuthorModel)
            .filter_by(reference_id=ref_id)
            .order_by(AuthorModel.order.asc(), AuthorModel.author_id.asc())
            .all()
        )

        # build the “should be” list
        expected = list(range(1, len(authors) + 1))
        actual = [a.order for a in authors]

        # only touch it if there’s a mismatch
        if actual != expected:
            logging.info(f"Reference {ref_id}: orders {actual} → renumbering to {expected}")
            for idx, author in enumerate(authors, start=1):
                author.order = idx
                db.add(author)
            db.commit()


if __name__ == "__main__":

    fix_author_orders()
