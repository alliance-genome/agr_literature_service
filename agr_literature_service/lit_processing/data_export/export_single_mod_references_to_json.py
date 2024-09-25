import argparse
import logging
from os import environ, makedirs, path, rename, remove
from dotenv import load_dotenv
from datetime import datetime, date
import json
import gzip
import shutil

from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
from agr_literature_service.lit_processing.utils.s3_utils import upload_file_to_s3
from agr_literature_service.lit_processing.utils.db_read_utils import get_journal_by_resource_id,\
    get_all_reference_relation_data, get_mod_corpus_association_data_for_ref_ids, \
    get_cross_reference_data_for_ref_ids, get_author_data_for_ref_ids, \
    get_mesh_term_data_for_ref_ids, get_mod_reference_type_data_for_ref_ids
from agr_literature_service.lit_processing.data_ingest.utils.file_processing_utils \
    import escape_special_characters, remove_surrogates
from agr_literature_service.lit_processing.utils.report_utils import send_data_export_report
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

init_tmp_dir()

logging.basicConfig(format='%(message)s')
log = logging.getLogger(__name__)
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

    json_file = "reference" + "_" + mod
    datestamp = str(date.today()).replace("-", "")
    if ondemand:
        # 2022-06-23 18:27:28.150889 => 20220623T182728
        datestamp = str(datetime.now()).replace('-', '').replace(':', '').replace(' ', 'T').split('.')[0]
        json_file = json_file + "_" + datestamp
    json_file = json_file + ".json"

    base_path = environ.get('XML_PATH', "")
    json_path = base_path + "json_data/"
    if not path.exists(json_path):
        makedirs(json_path)

    db_session = create_postgres_session(False)

    log.info("Getting reference_relation data from the database...")

    reference_id_to_reference_relation_data = get_all_reference_relation_data(db_session)

    log.info("Getting journal data from the database...")

    resource_id_to_journal = get_journal_by_resource_id(db_session)

    db_session.close()

    log.info("Getting data from Reference table and generating json file...")
    try:
        get_reference_data_and_generate_json(mod, reference_id_to_reference_relation_data,
                                             resource_id_to_journal, json_path + json_file,
                                             datestamp)

    except Exception as e:
        error_msg = "Error occurred when retrieving data from Reference data and generating json file: " + str(e)
        log.info(error_msg)
        if ondemand:
            send_data_export_report("ERROR", email, mod, error_msg)
        return

    log.info("Uploading json file to s3...")
    filename = None
    try:
        filename = upload_json_file_to_s3(json_path, json_file, datestamp, ondemand)
    except Exception as e:
        error_msg = "Error occurred when uploading json file to s3: " + str(e)
        log.info(error_msg)
        if ondemand:
            send_data_export_report("ERROR", email, mod, error_msg)
        return

    if ondemand:
        log.info("Sending email...")
        ui_url = str(ui_root_url) + filename
        email_message = "The file " + filename + " is ready for <a href=" + ui_url + ">download</a>"
        send_data_export_report("SUCCESS", email, mod, email_message)

    log.info("DONE!")


"""
def generate_json_file(metaData, data, filename_with_path):

    dataDict = {"data": data,
                "metaData": metaData}
    fw = open(filename_with_path, 'w')
    try:
        jsonStr = json.dumps(dataDict, indent=4, sort_keys=True)
        byteStr = jsonStr.encode('utf-8')
        decodedJsonStr = byteStr.decode('unicode-escape')
        fw.write(decodedJsonStr)
    except Exception as e:
        log.info("Error when generating " + filename_with_path + ": " + str(e))
        fw.write(json.dumps(dataDict, indent=4, sort_keys=True))
    fw.close
"""


def generate_json_file(metaData, data, filename_with_path):

    dataDict = {"data": data, "metaData": metaData}

    problematic_json_file = filename_with_path + "_problematic_items"

    try:
        # attempt to serialize the entire dataDict to JSON
        with open(filename_with_path, 'w', encoding='utf-8') as fw:
            json.dump(dataDict, fw, indent=4, sort_keys=True, ensure_ascii=False)
    except UnicodeEncodeError as e:
        log.info(f"UnicodeEncodeError when generating {filename_with_path}: {e}")
        # try to identify the problematic data
        for index, item in enumerate(data):
            try:
                json.dumps(item, ensure_ascii=False)
            except UnicodeEncodeError as item_e:
                log.info(f"UnicodeEncodeError in data at index {index}: {item_e}")
                log.info(f"Problematic data: {item}")
                with open(problematic_json_file, 'w', encoding='utf-8') as error_file:
                    json.dump(item, error_file, ensure_ascii=False)
                break  # Stop after finding the first problematic item
    except Exception as e:
        log.info(f"Error when generating {filename_with_path}: {e}")


def upload_json_file_to_s3(json_path, json_file, datestamp, ondemand):  # pragma: no cover

    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    if env_state == 'test':
        return None

    gzip_json_file = json_file + ".gz"

    with open(json_path + json_file, 'rb') as f_in, gzip.open(json_path + gzip_json_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    if ondemand:
        s3_filename = ondemand_bucket.replace('develop', env_state) + gzip_json_file
        upload_file_to_s3(json_path + gzip_json_file, s3_bucket, s3_filename)
        return gzip_json_file

    gzip_json_file_with_datestamp = gzip_json_file.replace('.json.gz', '_' + datestamp + '.json.gz')

    ## upload file to recent bucket
    s3_filename = recent_bucket.replace('develop', env_state) + gzip_json_file_with_datestamp
    upload_file_to_s3(json_path + gzip_json_file, s3_bucket, s3_filename)

    ## upload file to latest bucket
    s3_filename = latest_bucket.replace('develop', env_state) + gzip_json_file
    upload_file_to_s3(json_path + gzip_json_file, s3_bucket, s3_filename)

    ## upload file to monthly bucket if it is first day of the month
    todayDate = date.today()
    if todayDate.day == 1:
        s3_filename = monthly_bucket.replace('develop', env_state) + gzip_json_file_with_datestamp
        upload_file_to_s3(json_path + gzip_json_file, s3_bucket, s3_filename, 'GLACIER_IR')

    remove(json_path + json_file)
    remove(json_path + gzip_json_file)

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
            'date_created']


def get_reference_data_and_generate_json(mod, reference_id_to_reference_relation_data, resource_id_to_journal, json_file_with_path, datestamp):

    metaData = get_meta_data(mod, datestamp)

    data = []

    db_session = create_postgres_session(False)

    rs = db_session.execute("SELECT mod_id FROM mod where abbreviation = '" + mod + "'")
    rows = rs.fetchall()
    mod_id = rows[0][0]

    i = 0
    j = 0
    for index in range(loop_count):

        if i >= max_per_db_connection:
            i = 0
            db_session.close()
            json_file = json_file_with_path + "_" + str(j)
            log.info("generating " + json_file + ": data size=" + str(len(data)))
            generate_json_file(metaData, data, json_file)
            data = []
            j += 1
            db_session = create_postgres_session(False)

        offset = index * limit

        log.info("offs=" + str(offset) + ", data=" + str(len(data)))

        rs = None
        if mod in ['WB', 'XB', 'ZFIN', 'SGD', 'RGD', 'FB']:
            refColNmList = ", ".join(get_reference_col_names())
            rs = db_session.execute(f"SELECT {refColNmList} "
                                    f"FROM reference "
                                    f"WHERE reference_id IN "
                                    f"(select reference_id from mod_corpus_association "
                                    f"where mod_id = {mod_id} and corpus is True) "
                                    f"order by reference_id "
                                    f"limit {limit} "
                                    f"offset {offset}")
        else:
            refColNmList = "r." + ", r.".join(get_reference_col_names())
            rs = db_session.execute(f"SELECT {refColNmList} "
                                    f"FROM reference r, mod_corpus_association m "
                                    f"WHERE r.reference_id = m.reference_id "
                                    f"AND m.mod_id = {mod_id} and m.corpus is True "
                                    f"order by r.reference_id "
                                    f"limit {limit} "
                                    f"offset {offset}")

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

        reference_id_list = []
        for x in rows:
            reference_id_list.append(x[0])

        ref_ids = ", ".join([str(x) for x in reference_id_list])

        reference_id_to_xrefs = get_cross_reference_data_for_ref_ids(db_session, ref_ids)
        reference_id_to_authors = get_author_data_for_ref_ids(db_session, ref_ids)
        reference_id_to_mesh_terms = get_mesh_term_data_for_ref_ids(db_session, ref_ids)
        reference_id_to_mod_corpus_data = get_mod_corpus_association_data_for_ref_ids(db_session, ref_ids)
        reference_id_to_mod_reference_types = get_mod_reference_type_data_for_ref_ids(db_session, ref_ids)

        count_index = generate_json_data(rows, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_reference_relation_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, data)
        i += count_index

    db_session.close()


def generate_json_data(ref_data, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_reference_relation_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, data):  # pragma: no cover

    i = 0
    for x in ref_data:

        i += 1

        reference_id = x[0]
        resource_id = x[2]

        if i % 100 == 0:
            log.info(str(i) + " " + x[1])

        abstract = escape_special_characters(x[12])
        title = escape_special_characters(x[3])

        row = {'reference_id': x[0],
               'curie': x[1],
               'resource_id': x[2],
               'title': title,
               'language': remove_surrogates(x[4]),
               'date_published': remove_surrogates(str(x[5])),
               'date_arrived_in_pubmed': remove_surrogates(str(x[6])),
               'date_last_modified_in_pubmed': remove_surrogates(str(x[7])),
               'volume': remove_surrogates(x[8]),
               'plain_language_abstract': remove_surrogates(x[9]),
               'pubmed_abstract_languages': [remove_surrogates(lang) for lang in x[10]] if x[10] else x[10],
               'page_range': remove_surrogates(x[11]),
               'abstract': abstract,
               'keywords': [remove_surrogates(k.replace('"', '\\"')) for k in x[13]] if x[13] else x[13],
               'pubmed_types': [remove_surrogates(type) for type in x[14]] if x[14] else x[14],
               'publisher': remove_surrogates(x[15]),
               'category': remove_surrogates(x[16]),
               'pubmed_publication_status': remove_surrogates(x[17]),
               'issue_name': remove_surrogates(x[18]),
               'date_updated': remove_surrogates(str(x[19])),
               'date_created': remove_surrogates(str(x[20]))}

        # row['authors'] = reference_id_to_authors.get(reference_id, [])
        row['authors'] = reference_id_to_authors.get(reference_id, [])

        row['cross_references'] = reference_id_to_xrefs.get(reference_id, [])

        row['mod_reference_types'] = reference_id_to_mod_reference_types.get(reference_id, [])

        row['mesh_terms'] = reference_id_to_mesh_terms.get(reference_id, [])
        if 'ChapterIn' in reference_id_to_reference_relation_data.get(reference_id, {}):
            row['reference_relations'] = reference_id_to_reference_relation_data.get(reference_id, {})
        else:
            row['comment_and_corrections'] = reference_id_to_reference_relation_data.get(reference_id, {})
        # row['reference_relations'] = reference_id_to_reference_relation_data.get(reference_id, {})

        row['mod_corpus_associations'] = reference_id_to_mod_corpus_data.get(reference_id, [])

        if x.resource_id in resource_id_to_journal:
            (resource_curie, resource_title, resource_medline_abbreviation) = resource_id_to_journal[resource_id]
            row['resource_curie'] = resource_curie
            row['resource_title'] = escape_special_characters(resource_title)
            row['resource_medline_abbreviation'] = escape_special_characters(resource_medline_abbreviation)
        else:
            row['resource_curie'] = None
            row['resource_title'] = None
            row['resource_medline_abbreviation'] = None

        data.append(row)

    return i


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mod', action='store', type=str, help='MOD to dump',
                        choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XB'], required=True)
    parser.add_argument('-e', '--email', action='store', type=str, help="Email address to send file")
    parser.add_argument('-o', '--ondemand', action='store_true', help="by curator's request")

    args = vars(parser.parse_args())
    dump_data(args['mod'], args['email'], args['ondemand'])
