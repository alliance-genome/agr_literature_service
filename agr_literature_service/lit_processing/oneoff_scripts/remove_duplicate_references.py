import argparse
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session


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
                    print(f"keeping {row[REF_ID]} {row[REF_CURIE]} {row[CR_CURIE]}")
            else:
                print(f"WARNING!!! Mismatch reference titles: {row[TITLE]} != {prev_row[TITLE]}")
        else:
            if row[CR_OBS]:
                print(f"WARNING obsolete first! {row}")
            print(f"keeping {row[REF_ID]} {row[REF_CURIE]} {row[CR_CURIE]}")
        prev_row = row

    if not saveit:
        print(f"Would be Deleting {delete_list}")
    else:
        print(f"Deleting {delete_list}")
        for table_name in ('reference_mod_referencetype', 'reference'):
            query = f"DELETE FROM {table_name} WHERE reference_id = ANY(:delete_list)"
            db_session.execute(text(query), {'delete_list': delete_list})
        db_session.commit()


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
