import logging.config
import json
import requests
import sys
from os import environ, path
from sqlalchemy import text
from dotenv import load_dotenv
from fastapi_okta.okta_utils import get_authentication_token, generate_headers

# put this back
from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.api.user import set_global_user_id

load_dotenv()

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


def old_way_that_may_create_dupicates(db_session):
    rows = db_session.execute(text("""
        SELECT DISTINCT ON (cr.reference_id)
               cr.curie, cr.reference_id, cr.is_obsolete,
               r.curie AS reference_curie
        FROM cross_reference cr
        JOIN reference r ON cr.reference_id = r.reference_id
        WHERE cr.curie_prefix = 'WB'
          AND cr.is_obsolete = FALSE
          AND EXISTS (
              SELECT 1
              FROM workflow_tag wt1
              JOIN mod m1 ON wt1.mod_id = m1.mod_id
              WHERE wt1.reference_id = r.reference_id
                AND m1.abbreviation = 'WB'
                AND wt1.workflow_tag_id = 'ATP:0000163'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM workflow_tag wt2
              JOIN mod m2 ON wt2.mod_id = m2.mod_id
              WHERE wt2.reference_id = r.reference_id
                AND m2.abbreviation = 'WB'
                AND wt2.workflow_tag_id IN (
                    'ATP:0000173', 'ATP:0000174', 'ATP:0000190', 'ATP:0000187'
                )
          )
        ORDER BY cr.reference_id
    """)).fetchall()
    atp_tags = ['ATP:0000221', 'ATP:0000175', 'ATP:0000173', 'ATP:0000220', 'ATP:0000206', 'ATP:0000272', 'ATP:0000269']
    batch_counter = 0
    batch_size = 250
    for x in rows:
        # logger.info(f"reference_id {x[0]}\t{x[1]}\t{x[2]}\t{x[3]}")
        wb_wbpaper_id = x[0]
        agr_reference_id = x[1]
        for wb_atp in atp_tags:
            batch_counter += 1
            if batch_counter % batch_size == 0:
                batch_counter = 0
                # UNCOMMENT TO POPULATE
                # db_session.commit()
            logger.info(f"INSERT {agr_reference_id} {wb_wbpaper_id} is NOT in entity extraction needed, needs new value {wb_atp}")
            try:
                x = WorkflowTagModel(reference_id=agr_reference_id,
                                     mod_id=2,
                                     workflow_tag_id=wb_atp)
                db_session.add(x)
            except Exception as e:
                logger.info("An error occurred when adding workflog_tag row for reference_id = " + str(agr_reference_id) + " and atp value = " + wb_atp + " " + str(e))
    # UNCOMMENT TO POPULATE
    # db_session.commit()


def define_mappings():
    # starting ontology in indented text format
    # ATP:0000172     entity extraction
    #   ATP:0000174   entity extraction complete
    #     ATP:0000215 allele extraction complete
    #     ATP:0000196 antibody extraction complete
    #     ATP:0000214 gene extraction complete
    #     ATP:0000203 species extraction complete
    #     ATP:0000250 strain extraction complete
    #     ATP:0000251 transgenic allele extraction complete
    #   ATP:0000187   entity extraction failed
    #     ATP:0000217 allele extraction failed
    #     ATP:0000188 antibody extraction failed
    #     ATP:0000216 gene extraction failed
    #     ATP:0000204 species extraction failed
    #     ATP:0000270 strain extraction failed
    #     ATP:0000267 transgenic allele extraction failed
    #   ATP:0000190   entity extraction in progress
    #     ATP:0000219 allele extraction in progress
    #     ATP:0000195 antibody extraction in progress
    #     ATP:0000218 gene extraction in progress
    #     ATP:0000205 species extraction in progress
    #     ATP:0000271 strain extraction in progress
    #     ATP:0000268 transgenic allele extraction in progress
    #   ATP:0000173   entity extraction needed
    #     ATP:0000221 allele extraction needed
    #     ATP:0000175 antibody extraction needed
    #     ATP:0000220 gene extraction needed
    #     ATP:0000206 species extraction needed
    #     ATP:0000272 strain extraction needed
    #     ATP:0000269 transgenic allele extraction needed
    children_by_parent = {
        'ATP:0000172': [
            'ATP:0000174',
            'ATP:0000187',
            'ATP:0000190',
            'ATP:0000173'
        ],
        'ATP:0000174': [
            'ATP:0000215',
            'ATP:0000196',
            'ATP:0000214',
            'ATP:0000203',
            'ATP:0000250',
            'ATP:0000251'
        ],
        'ATP:0000187': [
            'ATP:0000217',
            'ATP:0000188',
            'ATP:0000216',
            'ATP:0000204',
            'ATP:0000270',
            'ATP:0000267'
        ],
        'ATP:0000190': [
            'ATP:0000219',
            'ATP:0000195',
            'ATP:0000218',
            'ATP:0000205',
            'ATP:0000271',
            'ATP:0000268'
        ],
        'ATP:0000173': [
            'ATP:0000221',
            'ATP:0000175',
            'ATP:0000220',
            'ATP:0000206',
            'ATP:0000272',
            'ATP:0000269'
        ]
    }
    atp_term_to_name = {
        'ATP:0000172': 'entity extraction',
        'ATP:0000174': 'entity extraction complete',
        'ATP:0000215': 'allele extraction complete',
        'ATP:0000196': 'antibody extraction complete',
        'ATP:0000214': 'gene extraction complete',
        'ATP:0000203': 'species extraction complete',
        'ATP:0000250': 'strain extraction complete',
        'ATP:0000251': 'transgenic allele extraction complete',
        'ATP:0000187': 'entity extraction failed',
        'ATP:0000217': 'allele extraction failed',
        'ATP:0000188': 'antibody extraction failed',
        'ATP:0000216': 'gene extraction failed',
        'ATP:0000204': 'species extraction failed',
        'ATP:0000270': 'strain extraction failed',
        'ATP:0000267': 'transgenic allele extraction failed',
        'ATP:0000190': 'entity extraction in progress',
        'ATP:0000219': 'allele extraction in progress',
        'ATP:0000195': 'antibody extraction in progress',
        'ATP:0000218': 'gene extraction in progress',
        'ATP:0000205': 'species extraction in progress',
        'ATP:0000271': 'strain extraction in progress',
        'ATP:0000268': 'transgenic allele extraction in progress',
        'ATP:0000173': 'entity extraction needed',
        'ATP:0000221': 'allele extraction needed',
        'ATP:0000175': 'antibody extraction needed',
        'ATP:0000220': 'gene extraction needed',
        'ATP:0000206': 'species extraction needed',
        'ATP:0000272': 'strain extraction needed',
        'ATP:0000269': 'transgenic allele extraction needed'
    }
    print(atp_term_to_name['ATP:0000172'])  # Output: entity extraction
    return children_by_parent, atp_term_to_name


def validate_mappings(children_by_parent, atp_term_to_name):  # noqa: C901
    token = get_authentication_token()
    logger.info(f"token {token}")
    headers = generate_headers(token)
    logger.info(f"headers {headers}")
    base_url = environ.get('ATEAM_API_URL', "")

    GRANDPARENT_TERM = 'ATP:0000172'
    ateam_children_by_parent = {}
    ateam_atp_term_to_name = {}

    url = base_url + '/atpterm/' + GRANDPARENT_TERM + '/'
    logger.info(f"url {url}")
    post_return = requests.get(url, headers=headers)
    logger.info(post_return.text)
    result_dict = post_return.json()
    ateam_atp_term_to_name[result_dict['entity']['curie']] = result_dict['entity']['name']
    # logger.info(json.dumps(result_dict, indent=4))

    visited = set()
    queue = [GRANDPARENT_TERM]
    # for parent_term in children_by_parent:
    while queue:
        parent_term = queue.pop(0)
        if parent_term in visited:
            continue
        visited.add(parent_term)
        url = f"{base_url}/atpterm/{parent_term}/children"
        logger.info(f"Fetching children for {parent_term} from {url}")
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.warning(f"Failed to fetch data for {parent_term}: {response.status_code}")
            continue
        try:
            result = response.json()
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response for {parent_term}")
            continue
        children = result.get("entities", [])
        if not children:
            logger.info(f"No children found for {parent_term}")
            continue
        curies = [child["curie"] for child in children]
        names = {child["curie"]: child["name"] for child in children}
        ateam_children_by_parent[parent_term] = curies
        ateam_atp_term_to_name.update(names)
        queue.extend(curies)

    logger.info("ATEAM Children by Parent:\n" + json.dumps(ateam_children_by_parent, indent=4))
    logger.info("ATEAM ATP Term to Name:\n" + json.dumps(ateam_atp_term_to_name, indent=4))

    mismatch_found = False
    logger.info("🔍 Comparing parent keys:")
    local_parents = set(children_by_parent.keys())
    ateam_parents = set(ateam_children_by_parent.keys())
    only_in_local_parents = local_parents - ateam_parents
    only_in_ateam_parents = ateam_parents - local_parents
    if only_in_local_parents or only_in_ateam_parents:
        mismatch_found = True
        logger.info("🔍 Mismatch in parent terms:")
        if only_in_local_parents:
            logger.info(f"  ➖ Parents only in local: {sorted(only_in_local_parents)}")
        if only_in_ateam_parents:
            logger.info(f"  ➕ Parents only in ATEAM: {sorted(only_in_ateam_parents)}")

    logger.info(f"🔍 Comparing Grandparent Children {GRANDPARENT_TERM}:")
    local_grand_children = set(children_by_parent.get(GRANDPARENT_TERM, []))
    ateam_grand_children = set(ateam_children_by_parent.get(GRANDPARENT_TERM, []))
    only_in_local = local_grand_children - ateam_grand_children
    only_in_ateam = ateam_grand_children - local_grand_children
    if only_in_local or only_in_ateam:
        mismatch_found = True
        logger.info(f"Grandparent: {GRANDPARENT_TERM}")
        if only_in_local:
            logger.info(f"  ➖ In local only: {sorted(only_in_local)}")
        if only_in_ateam:
            logger.info(f"  ➕ In ATEAM only: {sorted(only_in_ateam)}")
    else:
        logger.info("✅ Grandparent children match.")

    logger.info("🔍 Comparing Children by Parent:")
    all_parents = set(children_by_parent.keys()).union(ateam_children_by_parent.keys())
    for parent in sorted(all_parents):
        local_children = set(children_by_parent.get(parent, []))
        ateam_children = set(ateam_children_by_parent.get(parent, []))
        only_in_local = local_children - ateam_children
        only_in_ateam = ateam_children - local_children
        if only_in_local or only_in_ateam:
            mismatch_found = True
            logger.info(f"Parent: {parent}")
            if only_in_local:
                logger.info(f"  ➖ In local only: {sorted(only_in_local)}")
            if only_in_ateam:
                logger.info(f"  ➕ In ATEAM only: {sorted(only_in_ateam)}")
        else:
            logger.info(f"✅ Parent {parent} matches.")

    logger.info("🔍 Comparing ATP Term to Name Mappings:")
    all_terms = set(atp_term_to_name.keys()).union(ateam_atp_term_to_name.keys())
    for term in sorted(all_terms):
        local_name = atp_term_to_name.get(term)
        ateam_name = ateam_atp_term_to_name.get(term)
        if local_name != ateam_name:
            mismatch_found = True
            logger.info(f"❗ Mismatch for {term}:")
            logger.info(f"  🔸 Local : {local_name}")
            logger.info(f"  🔹 ATEAM : {ateam_name}")

    if mismatch_found:
        logger.info("🚨 Mismatch error detected.  Aborting program.\nLook at hardcoded mappings for children_by_parent, atp_term_to_name and compare to ateam values, there may be unaccounted for terms, or change in definitions.  Consult curators.")
        sys.exit("Mismatch error")
    else:
        logger.info("✅ All mappings match. Validation passed.")


if __name__ == "__main__":
    # put this back
    db_session = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)
    # old_way_that_may_create_dupicates(db_session)
    children_by_parent, atp_term_to_name = define_mappings()
    validate_mappings(children_by_parent, atp_term_to_name)
