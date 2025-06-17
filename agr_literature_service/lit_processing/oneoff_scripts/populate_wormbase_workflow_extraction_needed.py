import logging.config
import json
import requests
import sys
from os import environ, path
from sqlalchemy import text
from dotenv import load_dotenv
from fastapi_okta.okta_utils import get_authentication_token, generate_headers

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.api.models import WorkflowTagModel
from agr_literature_service.api.user import set_global_user_id

load_dotenv()

log_file_path = path.join(path.dirname(path.abspath(__file__)), '../../../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


# https://agr-jira.atlassian.net/browse/SCRUM-4974
# Take all references inside corpus for WB.  Take all the workflow_tag entries for WB.  The references must have ATP:0000163 file converted to text.
# Set parent to entity extraction needed if no extraction tags exist.  The references must not have ATP:0000173 ATP:0000174 ATP:0000190 ATP:0000187 or any of their children.  For the references that satisfy all conditions, add ATP:0000173 entity extraction needed.
# Set datatypes to needed based on siblings.  For each datatype sibling, if there’s only one sibling leave it be, if there’s multiple report that the data is bad, if zero exist add the datatype needed one.
# Set parent based on heirarchy:
# if at least one subclass is 'failed', set parent to 'failed' ATP:0000187 and delete the other parents.
#   else if at least one subclass is 'in progress', then set parent to 'in progress' ATP:0000190 and delete the other parents.
#   else if at least one subclass is 'needed', set it parent 'needed' ATP:0000173 and delete the other parents.
#   else set parent to 'complete' ATP:0000174, and delete the other parents,but not possible because the script is going to create needed entries.
#   The only time it will set things to complete is in the future if no new datatype has been added, and all the existing datatypes are complete
# For example, if a reference has
#   ATP:0000174   entity extraction complete
#   ATP:0000217   allele extraction failed
#   then it needs to delete the 174, keep the 217, and add ATP:0000187   entity extraction failed

# If this becomes a cronjob, this will need functional tests.
# Test default case of no tags sets parent of ATP:0000173 and grandchildren to extraction needed
# Add TEI file
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000163', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script should do
#   INSERT 7 is NOT in exclusion list, add ATP:0000173
#   INSERT 7 does not have siblings of ATP:0000221, add ATP:0000221
#   INSERT 7 does not have siblings of ATP:0000175, add ATP:0000175
#   INSERT 7 does not have siblings of ATP:0000220, add ATP:0000220
#   INSERT 7 does not have siblings of ATP:0000206, add ATP:0000206
#   INSERT 7 does not have siblings of ATP:0000272, add ATP:0000272
#   INSERT 7 does not have siblings of ATP:0000269, add ATP:0000269
#
# Test setting child with parent of priority ATP:0000187   entity extraction failed
# Restart with TEI file
#   DELETE FROM workflow_tag WHERE reference_id = 7;
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000163', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add ATP:0000204 species extraction failed
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000204', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script should do
#   7 has exclusion tags in DB: ['ATP:0000204']
#   INSERT 7 does not have siblings of ATP:0000221, add ATP:0000221
#   INSERT 7 does not have siblings of ATP:0000175, add ATP:0000175
#   INSERT 7 does not have siblings of ATP:0000220, add ATP:0000220
#   7 has existing sibling tags for datatype group ATP:0000206: ['ATP:0000204']
#   INSERT 7 does not have siblings of ATP:0000272, add ATP:0000272
#   INSERT 7 does not have siblings of ATP:0000269, add ATP:0000269
#   7 has children of ATP:0000187 {'ATP:0000204'}, removing {'ATP:0000190', 'ATP:0000174', 'ATP:0000173'} to add ATP:0000187
#   INSERT 7 has children {'ATP:0000204'}, add parent ATP:0000187
#
# Test that aftewards, adding a child with parent of priority ATP:0000190 does not take precedence over ATP:0000187
# Add ATP:0000195 antibody extraction in progress
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000195', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script makes no changes
#   7 has exclusion tags in DB: ['ATP:0000175', 'ATP:0000187', 'ATP:0000195', 'ATP:0000204', 'ATP:0000220', 'ATP:0000221', 'ATP:0000269', 'ATP:0000272']
#   7 has existing sibling tags for datatype group ATP:0000221: ['ATP:0000221']
#   7 has existing sibling tags for datatype group ATP:0000175: ['ATP:0000175', 'ATP:0000195']
#   7 has existing sibling tags for datatype group ATP:0000220: ['ATP:0000220']
#   7 has existing sibling tags for datatype group ATP:0000206: ['ATP:0000204']
#   7 has existing sibling tags for datatype group ATP:0000272: ['ATP:0000272']
#   7 has existing sibling tags for datatype group ATP:0000269: ['ATP:0000269']
#   7 has children of ATP:0000187 {'ATP:0000204'}, removing {'ATP:0000173', 'ATP:0000190', 'ATP:0000174'} to add ATP:0000187
#
# Test setting child with parent of priority ATP:0000190   entity extraction in progress
# Restart with TEI file
#   DELETE FROM workflow_tag WHERE reference_id = 7
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000163', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add ATP:0000195 antibody extraction in progress
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000195', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script does
#   7 has exclusion tags in DB: ['ATP:0000195']
#   INSERT 7 does not have siblings of ATP:0000221, add ATP:0000221
#   7 has existing sibling tags for datatype group ATP:0000175: ['ATP:0000195']
#   INSERT 7 does not have siblings of ATP:0000220, add ATP:0000220
#   INSERT 7 does not have siblings of ATP:0000206, add ATP:0000206
#   INSERT 7 does not have siblings of ATP:0000272, add ATP:0000272
#   INSERT 7 does not have siblings of ATP:0000269, add ATP:0000269
#   7 has children of ATP:0000190 {'ATP:0000195'}, removing {'ATP:0000173', 'ATP:0000187', 'ATP:0000174'} to add ATP:0000190
#   INSERT 7 has children {'ATP:0000195'}, add parent ATP:0000190
#
# Test setting child with parent of priority ATP:0000173   entity extraction needed
# Restart with TEI file
#   DELETE FROM workflow_tag WHERE reference_id = 7
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000163', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add ATP:0000221 allele extraction needed
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000221', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script does
#   7 has exclusion tags in DB: ['ATP:0000221']
#   7 has existing sibling tags for datatype group ATP:0000221: ['ATP:0000221']
#   INSERT 7 does not have siblings of ATP:0000175, add ATP:0000175
#   INSERT 7 does not have siblings of ATP:0000220, add ATP:0000220
#   INSERT 7 does not have siblings of ATP:0000206, add ATP:0000206
#   INSERT 7 does not have siblings of ATP:0000272, add ATP:0000272
#   INSERT 7 does not have siblings of ATP:0000269, add ATP:0000269
#   7 has children of ATP:0000173 {'ATP:0000221'}, removing {'ATP:0000174', 'ATP:0000190', 'ATP:0000187'} to add ATP:0000173
#   INSERT 7 has children {'ATP:0000221'}, add parent ATP:0000173
#
# Test setting child with parent of priority ATP:0000174   entity extraction complete
# Restart with TEI file
#   DELETE FROM workflow_tag WHERE reference_id = 7
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000163', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add ATP:0000215 allele extraction complete
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000215', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script does
#   7 has exclusion tags in DB: ['ATP:0000215']
#   7 has existing sibling tags for datatype group ATP:0000221: ['ATP:0000215']
#   INSERT 7 does not have siblings of ATP:0000175, add ATP:0000175
#   INSERT 7 does not have siblings of ATP:0000220, add ATP:0000220
#   INSERT 7 does not have siblings of ATP:0000206, add ATP:0000206
#   INSERT 7 does not have siblings of ATP:0000272, add ATP:0000272
#   INSERT 7 does not have siblings of ATP:0000269, add ATP:0000269
#   INSERT 7 default case, add parent ATP:0000174
#
# Test all parents + child of complete, does nothing because only ensuring parent complete exists.  Should it remove other parent tags ?  TODO - YES
# Restart with TEI file
#   DELETE FROM workflow_tag WHERE reference_id = 7;
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000163', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add all parents
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES
#     ( 7, 2, 'ATP:0000187', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000190', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000173', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000174', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add ATP:0000215 allele extraction complete
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000215', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script does
#   7 has exclusion tags in DB: ['ATP:0000173', 'ATP:0000174', 'ATP:0000187', 'ATP:0000190', 'ATP:0000215']
#   7 entity extraction complete, not setting datatypes to needed based on siblings
# because it already has the correct parent set
#
# Test all parents + child of needed.  Does not add all other children of needed, should it ?  -- TODO, it should add based on siblings.
# -- TODO - look at siblings, if any single sibling, leave it be.  if multiple siblings, add to report of bad data.
# Restart with TEI file
#   DELETE FROM workflow_tag WHERE reference_id = 7;
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000163', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add all parents
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES
#     ( 7, 2, 'ATP:0000187', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000190', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000173', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000174', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add ATP:0000221 allele extraction needed
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000221', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script does
#   7 has exclusion tags in DB: ['ATP:0000173', 'ATP:0000174', 'ATP:0000187', 'ATP:0000190', 'ATP:0000221']
#   7 entity extraction complete, not setting datatypes to needed based on siblings
#   7 has children of ATP:0000173 {'ATP:0000221'}, removing {'ATP:0000190', 'ATP:0000174', 'ATP:0000187'} to add ATP:0000173
#   DELETE 7 remove ATP:0000190 to add ATP:0000173
#   DELETE 7 remove ATP:0000174 to add ATP:0000173
#   DELETE 7 remove ATP:0000187 to add ATP:0000173
#
# Test all parents + child of in progress.  Correctly removed all other parents
# Restart with TEI file
#   DELETE FROM workflow_tag WHERE reference_id = 7;
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000163', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add all parents
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES
#     ( 7, 2, 'ATP:0000187', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000190', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000173', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000174', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add ATP:0000219 allele extraction in progress
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000219', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script does
#   7 has exclusion tags in DB: ['ATP:0000173', 'ATP:0000174', 'ATP:0000187', 'ATP:0000190', 'ATP:0000219']
#   7 entity extraction complete, not setting datatypes to needed based on siblings
#   7 has children of ATP:0000190 {'ATP:0000219'}, removing {'ATP:0000173', 'ATP:0000187', 'ATP:0000174'} to add ATP:0000190
#   DELETE 7 remove ATP:0000173 to add ATP:0000190
#   DELETE 7 remove ATP:0000187 to add ATP:0000190
#   DELETE 7 remove ATP:0000174 to add ATP:0000190
#
# Test all parents + child of failed.  Correctly removed all other parents
# Restart with TEI file
#   DELETE FROM workflow_tag WHERE reference_id = 7;
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000163', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add all parents
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES
#     ( 7, 2, 'ATP:0000187', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000190', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000173', NOW(), '00u1ctzvjgMpk87Qm5d7' ),
#     ( 7, 2, 'ATP:0000174', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# Add ATP:0000217 allele extraction failed
#   INSERT INTO workflow_tag ( reference_id, mod_id, workflow_tag_id, date_created, created_by )
#   VALUES ( 7, 2, 'ATP:0000217', NOW(), '00u1ctzvjgMpk87Qm5d7' );
# script does
#   7 has exclusion tags in DB: ['ATP:0000173', 'ATP:0000174', 'ATP:0000187', 'ATP:0000190', 'ATP:0000217']
#   7 entity extraction complete, not setting datatypes to needed based on siblings
#   7 has children of ATP:0000187 {'ATP:0000217'}, removing {'ATP:0000190', 'ATP:0000173', 'ATP:0000174'} to add ATP:0000187
#   DELETE 7 remove ATP:0000190 to add ATP:0000187
#   DELETE 7 remove ATP:0000173 to add ATP:0000187
#   DELETE 7 remove ATP:0000174 to add ATP:0000187


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
            batch_counter = db_commit_if_batch_size(db_session, batch_counter, batch_size)
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
    # print(atp_term_to_name['ATP:0000172'])  # Output: entity extraction
    siblings = {
        'ATP:0000221': ['ATP:0000215', 'ATP:0000217', 'ATP:0000219', 'ATP:0000221'],
        'ATP:0000175': ['ATP:0000196', 'ATP:0000188', 'ATP:0000195', 'ATP:0000175'],
        'ATP:0000220': ['ATP:0000214', 'ATP:0000216', 'ATP:0000218', 'ATP:0000220'],
        'ATP:0000206': ['ATP:0000203', 'ATP:0000204', 'ATP:0000205', 'ATP:0000206'],
        'ATP:0000272': ['ATP:0000250', 'ATP:0000270', 'ATP:0000271', 'ATP:0000272'],
        'ATP:0000269': ['ATP:0000251', 'ATP:0000267', 'ATP:0000268', 'ATP:0000269']
    }
    return children_by_parent, atp_term_to_name, siblings


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
            logger.info(f"  🔹 Local : {local_name}")
            logger.info(f"  🔸 ATEAM : {ateam_name}")

    if mismatch_found:
        logger.info("🚨 Mismatch error detected.  Aborting program.\nLook at hardcoded mappings for children_by_parent, atp_term_to_name and compare to ateam values, there may be unaccounted for terms, or change in definitions.  Consult curators.")
        sys.exit("Mismatch error")
    else:
        logger.info("✅ All mappings match. Validation passed.")


def process(db_session, children_by_parent, atp_term_to_name, siblings):
    mod_id_row = db_session.execute(text("""
        SELECT mod_id
        FROM mod
        WHERE abbreviation = 'WB'
    """)).fetchone()
    if mod_id_row is None:
        raise ValueError("No mod_id found for abbreviation 'WB'")
        return
    mod_id = mod_id_row.mod_id

    wanted_reference_ids = set()
    rows = db_session.execute(text("""
        SELECT cr.curie, cr.reference_id, cr.is_obsolete,
               r.curie AS reference_curie
        FROM cross_reference cr
        JOIN reference r ON cr.reference_id = r.reference_id
        WHERE cr.curie_prefix = 'WB'
          AND cr.is_obsolete = FALSE
    """)).fetchall()
    for x in rows:
        wanted_reference_ids.add(x.reference_id)

    wf_tags_db = {}
    rows = db_session.execute(text("""
        SELECT reference_id, workflow_tag_id
        FROM workflow_tag wt1
        WHERE mod_id = :mod_id
    """), {'mod_id': mod_id}).fetchall()
    for x in rows:
        if x[0] not in wf_tags_db:
            wf_tags_db[x[0]] = set()
        wf_tags_db[x[0]].add(x[1])
        # logger.info(f"{x[0]}\t{x[1]}")

#     atp_tags = ['ATP:0000221', 'ATP:0000175', 'ATP:0000173', 'ATP:0000220', 'ATP:0000206', 'ATP:0000272', 'ATP:0000269']
    batch_counter = 0
    batch_size = 250
    exclusion_tags = set(atp_term_to_name.keys()) - {'ATP:0000172'}
    for reference_id in sorted(wanted_reference_ids):
        if reference_id != 7:
            continue
        if batch_counter > 10:
            # UNCOMMENT TO POPULATE
            # db_session.commit()
            sys.exit(f"{batch_counter} counter reached")
        logger.info(f"Processing reference_id {reference_id}")
        parent_term_already_set_flag = False
        tags = wf_tags_db.get(reference_id, set())

        if 'ATP:0000163' not in tags:  # must have 'file converted to text'
            logger.info(f"Skipping {reference_id} because no TEI file")
            continue

        # set parent term to entity extraction needed if there are no children or grandchildren tags
        matching_exclusions = tags.intersection(exclusion_tags)  # must not have children or grandchildren of ATP:0000172
        if matching_exclusions:
            logger.info(f"{reference_id} has exclusion tags in DB: {sorted(matching_exclusions)}")
        else:
            wf_atp = 'ATP:0000173'
            error_message = "An error occurred when adding workflog_tag row for reference_id = " + str(reference_id) + " and atp value = " + wf_atp
            logger.info(f"INSERT {reference_id} is NOT in exclusion list, add {wf_atp}")
            batch_counter = create_workflow(db_session, batch_counter, batch_size, reference_id, mod_id, wf_atp, error_message)
            parent_term_already_set_flag = True

        # set grandchildren terms to datatype extraction needed
        if 'ATP:0000174' in tags:
            logger.info(f"{reference_id} entity extraction complete, not setting datatypes to needed based on siblings")
        else:  # must not be 'entity extraction complete' to add datatype siblings needed
            for group_tag, group_members in siblings.items():
                matching_member_tags = [member for member in group_members if member in tags]
                if matching_member_tags:
                    logger.info(f"{reference_id} has existing sibling tags for datatype group {group_tag}: {sorted(matching_member_tags)}")
                else:
                    error_message = "An error occurred when adding workflog_tag row for reference_id = " + str(reference_id) + " and atp value = " + group_tag
                    logger.info(f"INSERT {reference_id} does not have siblings of {group_tag}, add {group_tag}")
                    batch_counter = create_workflow(db_session, batch_counter, batch_size, reference_id, mod_id, group_tag, error_message)

        if parent_term_already_set_flag:
            continue

        # this might not be right.  maybe only one parent term can exist at the same time, in which case it should delete the three others.
        # set parent tag based on hierarchy
        # if at least one subclass is 'failed', set parent to 'failed' ATP:0000187
        #   else if at least one subclass is 'in progress', then set parent to 'in progress' ATP:0000190
        #   else if at least one subclass is 'needed', then set parent to 'needed' ATP:0000173
        #   else set parent to 'complete' ATP:0000174, but not possible because the script is going to create needed entries.
        # pseudocode
        # if      any children_by_parent of ATP:0000187, set ATP:0000187, remove ATP:0000190 ATP:0000173 ATP:0000174
        # else if any children_by_parent of ATP:0000190, set ATP:0000190, remove ATP:0000173 ATP:0000174
        # else if any children_by_parent of ATP:0000173 or a sibling was added above, set ATP:0000173, remove ATP:0000174
        # else set ATP:0000174
        parents = {'ATP:0000187', 'ATP:0000190', 'ATP:0000173', 'ATP:0000174'}
        matching_children_187 = tags.intersection(set(children_by_parent['ATP:0000187']))
        matching_children_190 = tags.intersection(set(children_by_parent['ATP:0000190']))
        matching_children_173 = tags.intersection(set(children_by_parent['ATP:0000173']))
        if matching_children_187:
            tag_to_add = 'ATP:0000187'
            tags_to_remove = parents - {tag_to_add}
            batch_counter = ensure_parent_tag(db_session, batch_counter, batch_size, tags, reference_id, mod_id, matching_children_187, tags_to_remove, tag_to_add)
        elif matching_children_190:
            tag_to_add = 'ATP:0000190'
            tags_to_remove = parents - {tag_to_add}
            batch_counter = ensure_parent_tag(db_session, batch_counter, batch_size, tags, reference_id, mod_id, matching_children_190, tags_to_remove, tag_to_add)
        elif matching_children_173:
            tag_to_add = 'ATP:0000173'
            tags_to_remove = parents - {tag_to_add}
            batch_counter = ensure_parent_tag(db_session, batch_counter, batch_size, tags, reference_id, mod_id, matching_children_173, tags_to_remove, tag_to_add)
        elif 'ATP:0000174' not in tags:
            error_message = "An error occurred when adding workflog_tag row for reference_id = " + str(reference_id) + " and atp value = ATP:0000174"
            logger.info(f"INSERT {reference_id} default case, add parent ATP:0000174")
            batch_counter = create_workflow(db_session, batch_counter, batch_size, reference_id, mod_id, 'ATP:0000174', error_message)

    # UNCOMMENT TO POPULATE
    db_session.commit()


def ensure_parent_tag(db_session, batch_counter, batch_size, tags, reference_id, mod_id, matching_children, tags_to_remove, tag_to_add):
    logger.info(f"{reference_id} has children of {tag_to_add} {matching_children}, removing {tags_to_remove} to add {tag_to_add}")
    for wf_atp in tags_to_remove:
        if wf_atp in tags:
            logger.info(f"DELETE {reference_id} remove {wf_atp} to add {tag_to_add}")
            error_message = "An error occurred when removing workflog_tag row for reference_id = " + str(reference_id) + " and atp value = " + wf_atp
            batch_counter = delete_workflow(db_session, batch_counter, batch_size, reference_id, mod_id, wf_atp, error_message)
    if tag_to_add not in tags:
        error_message = "An error occurred when adding workflog_tag row for reference_id = " + str(reference_id) + " and atp value = " + tag_to_add
        logger.info(f"INSERT {reference_id} has children {matching_children}, add parent {tag_to_add}")
        batch_counter = create_workflow(db_session, batch_counter, batch_size, reference_id, mod_id, tag_to_add, error_message)
    return batch_counter


def db_commit_if_batch_size(db_session, batch_counter, batch_size):
    batch_counter += 1
    if batch_counter % batch_size == 0:
        batch_counter = 0
        # UNCOMMENT TO POPULATE
        db_session.commit()
    return batch_counter


def delete_workflow(db_session, batch_counter, batch_size, reference_id, mod_id, wf_atp, error_message):
    batch_counter = db_commit_if_batch_size(db_session, batch_counter, batch_size)
    x = db_session.query(WorkflowTagModel).filter_by(reference_id=reference_id, mod_id=mod_id, workflow_tag_id=wf_atp).one_or_none()
    if x:
        try:
            db_session.delete(x)
        except Exception as e:
            logger.info(error_message + " " + str(e))
    return batch_counter


def create_workflow(db_session, batch_counter, batch_size, reference_id, mod_id, wf_atp, error_message):
    batch_counter = db_commit_if_batch_size(db_session, batch_counter, batch_size)
    try:
        x = WorkflowTagModel(reference_id=reference_id,
                             mod_id=mod_id,
                             workflow_tag_id=wf_atp)
        db_session.add(x)
    except Exception as e:
        logger.info(error_message + " " + str(e))
    return batch_counter


if __name__ == "__main__":
    db_session = create_postgres_session(False)
    scriptNm = path.basename(__file__).replace(".py", "")
    set_global_user_id(db_session, scriptNm)
    # old_way_that_may_create_dupicates(db_session)
    children_by_parent, atp_term_to_name, siblings = define_mappings()
# put this back
#     validate_mappings(children_by_parent, atp_term_to_name)
    process(db_session, children_by_parent, atp_term_to_name, siblings)
