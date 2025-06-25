"""
Add workflow_tags for a list of references, mod abbreviaion and topic.

i.e.
python add_workflow_tag_for_references_and_tag.py -m ZFIN -t 'ATP:123' -r 985307,985857
"""
import argparse
from sqlalchemy import text
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import ModModel



def parse_arguments():
    parser = argparse.ArgumentParser(description='ADD ATP to references and mod.')
    parser.add_argument("-r", "--reference_curies", type=str, help="Only run for this reference.", required=True)
    parser.add_argument("-m", "--mod_abbreviation", type=str, help="Only run for this mod.", required=True)
    parser.add_argument("-t", "--topic", type=str, help="Only run for this topic.", required=True)

    return parser.parse_args()


def add_atps(db, args):
    mod_id = db.query(ModModel).filter(ModModel.abbreviation == args.mod_abbreviation).one().mod_id
    print(f"mod_id = {mod_id}")
    print(f"topic = {args.topic}")
    print(f"reference_curies = {args.reference_curies}")
    for reference_id in args.reference_curies.split(','):
        sql = text(f"""INSERT INTO workflow_tag (workflow_tag_id, reference_id, mod_id, date_created) VALUES ('{args.topic}',{reference_id}, {mod_id}, NOW())""")
        print(sql)
        db.execute(sql)
    db.commit()


if __name__ == "__main__":
    args = parse_arguments()
    db = create_postgres_session(False)
    print(f"db = {db}")
    add_atps(db, args)
