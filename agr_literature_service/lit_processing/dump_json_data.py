import argparse
import logging
from os import environ, makedirs, path, rename, remove
from dotenv import load_dotenv
from datetime import datetime, date
import json

from agr_literature_service.api.models import CrossReferenceModel, \
    ReferenceModel, ModModel, ModCorpusAssociationModel, \
    ResourceModel, ModReferenceTypeModel
from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_session, \
    create_postgres_engine
from agr_literature_service.lit_processing.helper_s3 import upload_file_to_s3
from agr_literature_service.lit_processing.helper_email import send_email

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

load_dotenv()

s3_bucket = 'agr-literature'
sub_bucket = 'develop/reference/dumps/'
latest_bucket = sub_bucket + 'latest/'
recent_bucket = sub_bucket + 'recent/'
monthly_bucket = sub_bucket + 'monthly/'
ondemand_bucket = sub_bucket + 'ondemand/'

max_per_db_connection = 20000
limit = 500
loop_count = 700


def dump_data(mod, email, ondemand, ui_root_url=None):  # noqa: C901

    db_session = create_postgres_session(False)

    json_file = "reference" + "_" + mod + ".json"

    datestamp = str(date.today()).replace("-", "")
    if ondemand:
        # 2022-06-23 18:27:28.150889 => 20220623T18:27:28
        datestamp = str(datetime.now()).replace('-', '').replace(' ', 'T').split('.')[0]
        json_file = "reference" + "_" + mod + "_" + datestamp + ".json"

    base_path = environ.get('XML_PATH', "")
    json_path = base_path + "json_data/"
    if not path.exists(json_path):
        makedirs(json_path)

    m = db_session.query(ModModel).filter_by(abbreviation=mod).one_or_none()
    if m is None:
        log.info("Unknown mod name: " + mod)
        return
    mod_id = m.mod_id

    log.info("Getting cross_reference data from the database...")

    reference_id_to_xrefs = get_cross_reference_data(db_session, mod_id)

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    log.info("Getting author data from the database...")

    reference_id_to_authors = get_author_data(db_connection, mod_id)

    log.info("Getting comment/correction data from the database...")

    reference_id_to_comment_correction_data = get_comment_correction_data(db_connection)

    mod_id_to_mod = dict([(x.mod_id, x.abbreviation) for x in db_session.query(ModModel).all()])

    log.info("Getting mod reference type data from the database...")

    reference_id_to_mod_reference_types = get_mod_reference_type_data(db_session)

    log.info("Getting mod_corpus_association data from the database...")
    reference_id_to_mod_corpus_data = get_mod_corpus_association_data(db_session,
                                                                      mod_id_to_mod)

    log.info("Getting journal data from the database...")

    resource_id_to_journal = get_journal_data(db_session)

    log.info("Getting mesh term data from the database...")

    reference_id_to_mesh_terms = get_mesh_term_data(db_connection, mod_id)

    db_session.close()
    db_connection.close()
    engine.dispose()

    log.info("Getting data from Reference table and generating json file...")
    try:
        get_reference_data_and_generate_json(mod_id, mod, reference_id_to_xrefs,
                                             reference_id_to_authors,
                                             reference_id_to_comment_correction_data,
                                             reference_id_to_mod_reference_types,
                                             reference_id_to_mesh_terms,
                                             reference_id_to_mod_corpus_data,
                                             resource_id_to_journal,
                                             json_path + json_file, datestamp)

    except Exception as e:
        error_msg = "Error occurred when retrieving data from Reference data and generating json file: " + str(e)
        log.info(error_msg)
        if ondemand:
            send_email_report("ERROR", email, mod, error_msg)
        return

    log.info("Uploading json file to s3...")
    filename = None
    try:
        filename = upload_json_file_to_s3(json_path, json_file, datestamp, ondemand)
    except Exception as e:
        error_msg = "Error occurred when uploading json file to s3: " + str(e)
        log.info(error_msg)
        if ondemand:
            send_email_report("ERROR", email, mod, error_msg)
        return

    if ondemand:
        log.info("Sending email...")
        ui_url = path.join(ui_root_url, filename)
        email_message = "The file " + filename + " is ready for <a href=" + ui_url + ">download</a>"
        send_email_report("SUCCESS", email, mod, email_message)

    log.info("DONE!")


def generate_json_file(metaData, data, filename_with_path):

    try:
        jsonData = {"data": data,
                    "metaData": metaData}
        fw = open(filename_with_path, 'w')
        fw.write(json.dumps(jsonData, indent=4, sort_keys=True))
        fw.close()
    except Exception as e:
        log.info("Error when generating " + filename_with_path + ": " + str(e))
        exit()


def upload_json_file_to_s3(json_path, json_file, datestamp, ondemand):

    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    if env_state == 'test':
        return None

    if ondemand:
        s3_filename = ondemand_bucket.replace('develop', env_state) + json_file
        upload_file_to_s3(json_path + json_file, s3_bucket, s3_filename)
        return json_file

    json_file_with_datestamp = json_file.replace('.json', '_' + datestamp + '.json')

    ## upload file to recent bucket
    s3_filename = recent_bucket.replace('develop', env_state) + json_file_with_datestamp
    upload_file_to_s3(json_path + json_file, s3_bucket, s3_filename)

    ## upload file to latest bucket
    s3_filename = latest_bucket.replace('develop', env_state) + json_file
    upload_file_to_s3(json_path + json_file, s3_bucket, s3_filename)

    ## upload file to monthly bucket if it is first day of the month
    todayDate = date.today()
    if todayDate.day == 1:
        s3_filename = monthly_bucket.replace('develop', env_state) + json_file_with_datestamp
        upload_file_to_s3(json_path + json_file, s3_bucket, s3_filename, 'GLACIER_IR')

    return None


def concatenate_json_files(json_file, index):

    if index == 1:
        rename(json_file + '_0', json_file)
        return

    fw = open(json_file, "w")

    for i in range(index):
        this_json_file = json_file + '_' + str(i)
        f = open(this_json_file)
        if i == 0:
            # first chuck of data so keep beginning part, remove ending part
            for line in f:
                if line.strip().endswith("}") and len(line) == 10:
                    fw.write("        },\n")
                    break
                fw.write(line)
        elif i + 1 == index:
            # last chunk of data so keep ending part, remove beginning part
            for line in f:
                if line.startswith('{') or line.startswith('    "data": ['):
                    continue
                fw.write(line)
        else:
            # mid section(s) so remove beginning part & ending part
            for line in f:
                if line.strip().endswith("}") and len(line) == 10:
                    fw.write("        },\n")
                    break
                if line.startswith('{') or line.startswith('    "data": ['):
                    continue
                fw.write(line)
        f.close()

        remove(this_json_file)

    fw.close()


def get_meta_data(mod, datestamp):

    ## return more info here?
    return {
        "dateProduced": datestamp,
        "dataProvider": {
            "type": "curated",
            "mod": mod
        }
    }


def get_reference_col_names():

    return ['reference_id',
            'curie',
            'resource_id',
            'title',
            'language',
            'date_published',
            'date_arrived_in_pubmed',
            'date_last_modified_in_pubmed',
            'volume',
            'plain_language_abstract',
            'pubmed_abstract_languages',
            'page_range',
            'abstract',
            'keywords',
            'pubmed_types',
            'publisher',
            'category',
            'pubmed_publication_status',
            'issue_name',
            'date_updated',
            'date_created',
            'open_access']


def get_reference_data_and_generate_json(mod_id, mod, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_comment_correction_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, json_file_with_path, datestamp):

    metaData = get_meta_data(mod, datestamp)

    data = []

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    i = 0
    j = 0
    for index in range(loop_count):

        if i >= max_per_db_connection:
            i = 0
            db_connection.close()
            json_file = json_file_with_path + "_" + str(j)
            log.info("generating " + json_file + ": data size=" + str(len(data)))
            generate_json_file(metaData, data, json_file)
            data = []
            j += 1
            db_connection = engine.connect()

        offset = index * limit

        log.info("offs=" + str(offset) + ", data=" + str(len(data)))

        # all = db_session.query(
        #    ReferenceModel
        # ).join(
        #    ReferenceModel.mod_corpus_association
        # ).filter(
        #    ModCorpusAssociationModel.mod_id == mod_id
        # ).order_by(
        #    ReferenceModel.reference_id
        # ).offset(
        #    offset
        # ).limit(
        #    limit
        # ).all()
        # 924511
        rs = None
        if mod in ['WB', 'XB', 'ZFIN', 'SGD', 'RGD', 'FB']:
            refColNmList = ", ".join(get_reference_col_names())
            rs = db_connection.execute('select ' + refColNmList + ' from reference where reference_id in (select reference_id from mod_corpus_association where mod_id = ' + str(mod_id) + ' and corpus is True) order by reference_id limit ' + str(limit) + ' offset ' + str(offset))
        else:
            refColNmList = "r." + ", r.".join(get_reference_col_names())
            rs = db_connection.execute('select ' + refColNmList + ' from reference r, mod_corpus_association m where r.reference_id = m.reference_id and m.mod_id = ' + str(mod_id) + ' and corpus is True order by reference_id limit ' + str(limit) + ' offset ' + str(offset))

        rows = rs.fetchall()
        if len(rows) == 0:
            ## finished retrieving all data from database
            if len(data) > 0:
                json_file = json_file_with_path + "_" + str(j)
                log.info("generating " + json_file + ": data size=" + str(len(data)))
                generate_json_file(metaData, data, json_file)
            log.info("concatenating " + str(j + 1) + " small json files to a single json file: " + json_file_with_path)
            concatenate_json_files(json_file_with_path, j + 1)
            return

        count_index = generate_json_data(rows, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_comment_correction_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, data)
        i += count_index

    db_connection.close()
    engine.dispose()


def generate_json_data(ref_data, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_comment_correction_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, data):

    i = 0
    for x in ref_data:

        i += 1

        reference_id = x[0]
        resource_id = x[2]

        if i % 100 == 0:
            log.info(str(i) + " " + x[1])

        row = {'reference_id': x[0],
               'curie': x[1],
               'resource_id': x[2],
               'title': x[3],
               'language': x[4],
               'date_published': str(x[5]),
               'date_arrived_in_pubmed': str(x[6]),
               'date_last_modified_in_pubmed': str(x[7]),
               'volume': x[8],
               'plain_language_abstract': x[9],
               'pubmed_abstract_languages': x[10],
               'page_range': x[11],
               'abstract': x[12],
               'keywords': x[13],
               'pubmed_types': x[14],
               'publisher': x[15],
               'category': x[16],
               'pubmed_publication_status': x[17],
               'issue_name': x[18],
               'date_updated': str(x[19]),
               'date_created': str(x[20]),
               'open_access': x[21]}

        row['authors'] = reference_id_to_authors.get(reference_id, [])

        row['cross_references'] = reference_id_to_xrefs.get(reference_id, [])

        row['mod_reference_types'] = reference_id_to_mod_reference_types.get(reference_id, [])

        row['mesh_terms'] = reference_id_to_mesh_terms.get(reference_id, [])

        row['comment_and_corrections'] = reference_id_to_comment_correction_data.get(reference_id, {})

        row['mod_corpus_associations'] = reference_id_to_mod_corpus_data.get(reference_id, [])

        if x.resource_id in resource_id_to_journal:
            (resource_curie, resource_title) = resource_id_to_journal[resource_id]
            row['resource_curie'] = resource_curie
            row['resource_title'] = resource_title
        else:
            row['resource_curie'] = None
            row['resource_title'] = None

        data.append(row)

    return i


def send_email_report(status, email, mod, email_message):

    email_recipients = email
    if email_recipients is None:
        if environ.get('CRONTAB_EMAIL'):
            email_recipients = environ['CRONTAB_EMAIL']
        else:
            return

    sender_email = None
    if environ.get('SENDER_EMAIL'):
        sender_email = environ['SENDER_EMAIL']
    sender_password = None
    if environ.get('SENDER_PASSWORD'):
        sender_password = environ['SENDER_PASSWORD']
    reply_to = sender_email
    if environ.get('REPLY_TO'):
        reply_to = environ['REPLY_TO']

    email_subject = None
    email_message = None
    if status == 'SUCCESS':
        email_subject = "The " + mod + " Reference json file is ready for download"
    else:
        email_subject = "Error Report for " + mod + " Reference download"

    (status, message) = send_email(email_subject, email_recipients, email_message,
                                   sender_email, sender_password, reply_to)
    if status == 'error':
        log.info("Failed sending email to slack: " + message + "\n")


def get_mod_corpus_association_data(db_session, mod_id_to_mod):

    reference_id_to_mod_corpus_data = {}
    for x in db_session.query(ModCorpusAssociationModel).all():
        data = []
        if x.reference_id in reference_id_to_mod_corpus_data:
            data = reference_id_to_mod_corpus_data[x.reference_id]
        data.append({"mod_corpus_association_id": x.mod_corpus_association_id,
                     "mod_abbreviation": mod_id_to_mod[x.mod_id],
                     "corpus": x.corpus,
                     "mod_corpus_sort_source": x.mod_corpus_sort_source,
                     "date_created": str(x.date_created),
                     "date_updated": str(x.date_updated)})
        reference_id_to_mod_corpus_data[x.reference_id] = data

    return reference_id_to_mod_corpus_data


def get_cross_reference_data(db_session, mod_id):

    reference_id_to_xrefs = {}
    for x in db_session.query(CrossReferenceModel).join(ReferenceModel.cross_reference).outerjoin(ReferenceModel.mod_corpus_association).filter(ModCorpusAssociationModel.mod_id == mod_id).all():
        data = []
        if x.reference_id in reference_id_to_xrefs:
            data = reference_id_to_xrefs[x.reference_id]
        row = {"curie": x.curie,
               "is_obsolete": x.is_obsolete}
        data.append(row)
        reference_id_to_xrefs[x.reference_id] = data

    return reference_id_to_xrefs


def get_author_data(db_connection, mod_id):

    reference_id_to_authors = {}

    author_limit = 500000
    for index in range(100):
        offset = index * author_limit
        # rs = db_connection.execute('select a.* from author a, mod_corpus_association mca where a.reference_id = mca.reference_id and mca.mod_id = ' + str(mod_id) + ' order by a.reference_id, a.order limit ' + str(author_limit) + ' offset ' + str(offset))
        # to avoid column order change etc
        rs = db_connection.execute('select a.author_id, a.reference_id, a.orcid, a.first_author, a.order, a.corresponding_author, a.name, a.affiliations, a.first_name, a.last_name, a.date_updated, a.date_created from author a, mod_corpus_association mca where a.reference_id = mca.reference_id and mca.mod_id = ' + str(mod_id) + ' order by a.reference_id, a.order limit ' + str(author_limit) + ' offset ' + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break
        for x in rows:
            data = []
            reference_id = x[1]
            if reference_id in reference_id_to_authors:
                data = reference_id_to_authors[reference_id]
            data.append({"author_id": x[0],
                         "orcid": x[2],
                         "first_author": x[3],
                         "order": x[4],
                         "corresponding_author": x[5],
                         "name": x[6],
                         "affilliations": x[7] if x[7] else [],
                         "first_name": x[8],
                         "last_name": x[9],
                         "date_updated": str(x[10]),
                         "date_created": str(x[11])})
            reference_id_to_authors[reference_id] = data

    return reference_id_to_authors


def get_comment_correction_data(db_connection):

    reference_id_to_comment_correction_data = {}

    type_mapping = {'ErratumFor': 'ErratumIn',
                    'RepublishedFrom': 'RepublishedIn',
                    'RetractionOf': 'RetractionIn',
                    'ExpressionOfConcernFor': 'ExpressionOfConcernIn',
                    'ReprintOf': 'ReprintIn',
                    'UpdateOf': 'UpdateIn'}

    reference_id_to_curies = {}
    rs = db_connection.execute("select cc.reference_id, cc.curie, r.curie from cross_reference cc, reference r where cc.reference_id = r.reference_id and (cc.reference_id in (select reference_id_from from reference_comments_and_corrections) or cc.reference_id in (select reference_id_to from reference_comments_and_corrections))")
    rows = rs.fetchall()
    for x in rows:
        if x[1].startswith('PMID:'):
            reference_id_to_curies[x[0]] = (x[1], x[2])

    rs = db_connection.execute("select reference_id_from, reference_id_to, reference_comment_and_correction_type from reference_comments_and_corrections")

    for x in rs:

        type_db = x[2]
        type_db = type_db.replace("ReferenceCommentAndCorrectionType.", "")
        reference_id_from = x[0]
        reference_id_to = x[1]

        ## for reference_id_from
        data = {}
        if reference_id_from in reference_id_to_comment_correction_data:
            data = reference_id_to_comment_correction_data[reference_id_from]
        (pmid, ref_curie) = reference_id_to_curies[reference_id_from]
        data[type_db] = {"PMID": pmid,
                         "reference_curie": ref_curie}
        reference_id_to_comment_correction_data[reference_id_from] = data

        ## for reference_id_to
        data = {}
        if reference_id_to in reference_id_to_comment_correction_data:
            data = reference_id_to_comment_correction_data[reference_id_to]

        (pmid, ref_curie) = reference_id_to_curies[reference_id_to]
        type = type_mapping.get(type_db)
        if type is None:
            log.info(type_db + " is not in type_mapping.")
        else:
            data[type] = {"PMID": pmid,
                          "reference_curie": ref_curie}
            reference_id_to_comment_correction_data[reference_id_to] = data

    return reference_id_to_comment_correction_data


def get_mesh_term_data(db_connection, mod_id):

    reference_id_to_mesh_terms = {}

    mesh_limit = 1000000
    for index in range(50):
        offset = index * mesh_limit
        rs = db_connection.execute('select md.mesh_detail_id, md.reference_id, md.heading_term, md.qualifier_term from mesh_detail md, mod_corpus_association mca where md.reference_id = mca.reference_id and mca.mod_id = ' + str(mod_id) + ' order by md.mesh_detail_id limit ' + str(mesh_limit) + ' offset ' + str(offset))
        rows = rs.fetchall()
        if len(rows) == 0:
            break
        for x in rows:
            reference_id = x[1]
            data = []
            if reference_id in reference_id_to_mesh_terms:
                data = reference_id_to_mesh_terms[reference_id]
            data.append({"heading_term": x[2],
                         "qualifier_term": x[3],
                         "mesh_detail_id": x[0]})
            reference_id_to_mesh_terms[reference_id] = data

    return reference_id_to_mesh_terms


def get_mod_reference_type_data(db_session):

    reference_id_to_mod_reference_types = {}

    for x in db_session.query(ModReferenceTypeModel).all():
        data = []
        if x.reference_id in reference_id_to_mod_reference_types:
            data = reference_id_to_mod_reference_types[x.reference_id]
        data.append({"reference_type": x.reference_type,
                     "source": x.source,
                     "mod_reference_type_id": x.mod_reference_type_id})
        reference_id_to_mod_reference_types[x.reference_id] = data

    return reference_id_to_mod_reference_types


def get_journal_data(db_session):

    resource_id_to_journal = {}

    for x in db_session.query(ResourceModel).all():
        resource_id_to_journal[x.resource_id] = (x.curie, x.title)

    return resource_id_to_journal


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mod', action='store', type=str, help='MOD to dump',
                        choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XB'], required=True)
    parser.add_argument('-e', '--email', action='store', type=str, help="Email address to send file")
    parser.add_argument('-o', '--ondemand', action='store_true', help="by curator's request")

    args = vars(parser.parse_args())
    dump_data(args['mod'], args['email'], args['ondemand'])
