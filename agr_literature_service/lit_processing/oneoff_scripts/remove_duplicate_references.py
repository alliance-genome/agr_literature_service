import argparse
import logging
import sys
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session


logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout,
                    format='%(asctime)s - %(levelname)s - {%(module)s %(funcName)s:%(lineno)d} - %(message)s',
                    # noqa E251
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

def remove_duplicates(db_session, mod='FB', saveit=False):
    query = """SELECT r.reference_id, r.curie, cr.curie, cr.is_obsolete, r.title
FROM reference r, cross_reference cr
WHERE r.reference_id = cr.reference_id
AND cr.curie in (
    SELECT cr.curie FROM cross_reference cr
    WHERE cr.curie_prefix = :mod
    GROUP BY cr.curie
    HAVING COUNT(*) > 1)
ORDER BY cr.curie, cr.is_obsolete"""

    results = db_session.execute(text(query), {'mod': mod}).fetchall()
    prev_row = None
    REF_ID = 0
    REF_CURIE = 1
    CR_CURIE = 2
    CR_OBS = 3
    TITLE = 4
    delete_list = []
    for row in results:
        print(f"ref_id={row[REF_ID]} ref_curie={row[REF_CURIE]} cr_curie={row[CR_CURIE]} obsolete={row[CR_OBS]} title={row[TITLE]}")
        if prev_row and row[CR_CURIE] == prev_row[CR_CURIE]:
            if row[TITLE] == prev_row[TITLE]:
                if row[CR_OBS]:  # obsolete
                    print(f"REMOVE {row[REF_ID]} {row[REF_CURIE]} {row[CR_CURIE]}")
                    delete_list.append(row[REF_ID])
                else:
                    print(f"KEEPING {row[REF_ID]} {row[REF_CURIE]} {row[CR_CURIE]}")
            else:
                print(f"Mismatch reference titles: {row[TITLE]} != {prev_row[TITLE]}")
        else:
            if row[CR_OBS]:
                print(f"obsolete first! {row}")
            print(f"KEEPING FIRST ONE {row[REF_ID]} {row[REF_CURIE]} {row[CR_CURIE]}")
        prev_row= row

    if not saveit:
        print(f"Deleting {delete_list}")
    else:
        query = "DELETE FROM reference where reference_id in :delete_list"
        # db_session.execute(text(query), {'delete_list': delete_list})
        # db.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Remove duplicate references')
    parser.add_argument('--mod', type=str, default='FB',
                        help='MOD abbreviation (default: FB)')
    parser.add_argument('--saveit', type=bool, default=False,
                        help='saveit mode (default: False)')
    args = parser.parse_args()

    db_session = create_postgres_session(False)
    # populate_test_mods()
    print(args.saveit)
    remove_duplicates(db_session, mod=args.mod, saveit=args.saveit)
    db_session.close()
