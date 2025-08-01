import argparse
from sqlalchemy import text
import logging
from os import environ, makedirs, path, remove
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
    get_mesh_term_data_for_ref_ids, get_mod_reference_type_data_for_ref_ids, \
    get_citation_data, get_license_data
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


def generate_json_data(ref_data, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_reference_relation_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, reference_id_to_citation_data, reference_id_to_license_data, data):  # pragma: no cover

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

        row['comment_and_corrections'] = reference_id_to_reference_relation_data.get(reference_id, {})
        reference_relations = []
        for type, entries in row['comment_and_corrections'].items():
            for entry in entries:
                reference_relations.append({
                    "reference_curie": entry['reference_curie'],
                    "reference_relation_type": type,
                    "PMID": entry['PMID']
                })
        row['reference_relations'] = reference_relations

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
        if reference_id in reference_id_to_citation_data:
            row['citation'] = reference_id_to_citation_data[reference_id]['citation']
            row['short_citation'] = reference_id_to_citation_data[reference_id]['short_citation']
        else:
            row['citation'] = None
            row['short_citation'] = None
        if reference_id in reference_id_to_license_data:
            row['copyright_license_name'] = reference_id_to_license_data[reference_id]['name']
            row['copyright_license_url'] = reference_id_to_license_data[reference_id]['url']
            row['copyright_license_description'] = reference_id_to_license_data[reference_id]['description']
            row['copyright_license_open_access'] = reference_id_to_license_data[reference_id]['open_access']
        else:
            row['copyright_license_name'] = None
            row['copyright_license_url'] = None
            row['copyright_license_description'] = None
            row['copyright_license_open_access'] = None
        data.append(row)

    return i


def dump_data(mod=None, email=None, ondemand=False, ui_root_url=None):  # noqa: C901
    """
    If mod is None, dump one big JSON of every paper that belongs to at least one of:
      ['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XB']
    Then gzip & upload to S3 (and email if ondemand).

    latest:   reference_all_mods.json.gz
    recent:   reference_all_mods_YYYYMMDD.json.gz

    If mod is provided, dump a json of every paper that belongs to the given mod
    latest:   reference_[mod].json.gz
    recent:   reference_[mod]_YYYYMMDD.json.gz
    """
    if mod:
        mods = [mod]
        base_name = f"reference_{mod}"
    else:
        mods = ['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XB']
        base_name = "reference_all_mods"

    # ─── 1. prepare filenames & paths ──────────────────────────────────────────
    if ondemand:
        # include full timestamp for on-demand dumps
        datestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
        json_fname = f"{base_name}_{datestamp}.json"
    else:
        # daily scheduled dumps: keep JSON filename un‐stamped
        datestamp = date.today().strftime('%Y%m%d')
        json_fname = f"{base_name}.json"

    base = environ.get('XML_PATH', '') + 'json_data/'
    if not path.exists(base):
        makedirs(base)
    out_path = base + json_fname

    # ─── 2. preload shared lookups ────────────────────────────────────────────
    db = create_postgres_session(False)
    rels = get_all_reference_relation_data(db)
    journals = get_journal_by_resource_id(db)
    cites = get_citation_data(db)
    licenses = get_license_data(db)
    db.close()

    # ─── 3. gather all reference_ids in these MOD corpora ────────────────────
    db = create_postgres_session(False)
    mod_rows = db.execute(
        text("SELECT mod_id FROM mod WHERE abbreviation = ANY(:mods)"),
        {'mods': mods}
    ).fetchall()
    mod_ids = [r[0] for r in mod_rows]

    ref_rows = db.execute(
        text("""
            SELECT DISTINCT reference_id
              FROM mod_corpus_association
             WHERE mod_id = ANY(:mod_ids)
               AND corpus is True
        """),
        {'mod_ids': mod_ids}
    ).fetchall()
    reference_ids = [r[0] for r in ref_rows]
    db.close()

    # ─── 4. build JSON data in manageable chunks ─────────────────────────────
    all_data = []

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    for chunk in chunks(reference_ids, limit):
        db = create_postgres_session(False)
        cols = ", ".join(get_reference_col_names())
        rows = db.execute(
            text(f"SELECT {cols} FROM reference WHERE reference_id = ANY(:rids) ORDER BY reference_id"),
            {'rids': chunk}
        ).fetchall()

        # per-chunk lookups
        rids_str = ",".join(map(str, chunk))
        xrefs = get_cross_reference_data_for_ref_ids(db, rids_str)
        authors = get_author_data_for_ref_ids(db, rids_str)
        meshes = get_mesh_term_data_for_ref_ids(db, rids_str)
        mod_types = get_mod_reference_type_data_for_ref_ids(db, rids_str)
        mod_corpus = get_mod_corpus_association_data_for_ref_ids(db, rids_str)
        db.close()

        generate_json_data(
            rows,
            xrefs,
            authors,
            rels,
            mod_types,
            meshes,
            mod_corpus,
            journals,
            cites,
            licenses,
            all_data
        )

    # ─── 5. write out the big JSON ────────────────────────────────────────────
    modLabel = mod if mod else 'ALL MODS'
    meta = get_meta_data(modLabel, datestamp)
    generate_json_file(meta, all_data, out_path)

    # ─── 6. gzip & upload, using the same helper as MOD dumps ────────────────
    try:
        uploaded_name = upload_json_file_to_s3(base, json_fname, datestamp, ondemand)
    except Exception as e:
        log.info(f"Error uploading {modLabel} JSON to S3: {e}")
        if ondemand:
            send_data_export_report("ERROR", email, modLabel, str(e))
        return

    # ─── 7. send on-demand email notification ────────────────────────────────
    if ondemand and uploaded_name:
        ui_url = f"{ui_root_url}{uploaded_name}"
        send_data_export_report(
            "SUCCESS",
            email,
            modLabel,
            f"The file {uploaded_name} is ready for <a href={ui_url}>download</a>"
        )

    return uploaded_name or out_path


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mod', action='store', type=str, help='MOD to dump',
                        choices=['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'XB'])
    parser.add_argument('-e', '--email', action='store', type=str, help="Email address to send file")
    parser.add_argument('-o', '--ondemand', action='store_true', help="by curator's request")

    args = vars(parser.parse_args())
    dump_data(args['mod'], args['email'], args['ondemand'])
