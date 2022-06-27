import argparse
import logging
from os import environ, makedirs, path, rename, remove
from dotenv import load_dotenv
from datetime import datetime, date
import json

from agr_literature_service.api.models import CrossReferenceModel, \
    ReferenceModel, ModModel, ModCorpusAssociationModel, \
    ReferenceCommentAndCorrectionModel, AuthorModel, MeshDetailModel, \
    ResourceModel, ModReferenceTypeModel
from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_session
from agr_literature_service.lit_processing.helper_s3 import upload_file_to_s3

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

max_per_db_connection = 50000
limit = 500
loop_count = 700


def dump_data(mod, ondemand):  # noqa: C901

    db_session = create_postgres_session(False)

    json_file = "reference" + "_" + mod + ".json"

    datestamp = str(date.today()).replace("-", "")
    if ondemand:
        # 2022-06-23 18:27:28.150889 => 20220623T18:27:28
        datestamp = str(datetime.now()).replace('-', '').replace(' ', 'T').split('.')[0]

    base_path = environ.get('XML_PATH', "")
    json_path = base_path + "json_data/"
    if not path.exists(json_path):
        makedirs(json_path)

    m = db_session.query(ModModel).filter_by(abbreviation=mod).one_or_none()
    if m is None:
        log.info("Unknown mod name: " + mod)
        return
    mod_id = m.mod_id

    log.info("Getting mod-in-corpus reference_ids from the database...")

    reference_id_to_source = dict([(x.reference_id, x.mod_corpus_sort_source.replace("ModCorpusSortSourceType.", "")) for x in db_session.query(ModCorpusAssociationModel).filter_by(mod_id=mod_id, corpus=True).all()])

    log.info("Getting cross_reference data from the database...")

    (reference_id_to_xrefs, reference_id_to_pmid) = get_cross_reference_data(db_session, mod_id)

    log.info("Getting author data from the database...")

    reference_id_to_authors = get_author_data(db_session, mod_id)

    log.info("Getting comment/correction data from the database...")

    reference_id_to_comment_correction_data = get_comment_correction_data(db_session,
                                                                          reference_id_to_pmid)

    mod_id_to_mod = dict([(x.mod_id, x.abbreviation) for x in db_session.query(ModModel).all()])

    log.info("Getting mod reference type data from the database...")

    reference_id_to_mod_reference_types = get_mod_reference_type_data(db_session)

    log.info("Getting mod_corpus_association data from the database...")
    reference_id_to_mod_corpus_data = get_mod_corpus_association_data(db_session,
                                                                      mod_id_to_mod)

    log.info("Getting journal data from the database...")

    resource_id_to_journal = get_journal_data(db_session)

    log.info("Getting mesh term data from the database...")

    reference_id_to_mesh_terms = get_mesh_term_data(db_session, mod_id)

    db_session.close()

    log.info("Getting data from Reference table and generating json file...")
    try:
        get_reference_data_and_generate_json(mod_id, mod, reference_id_to_source,
                                             reference_id_to_xrefs,
                                             reference_id_to_authors,
                                             reference_id_to_comment_correction_data,
                                             reference_id_to_mod_reference_types,
                                             reference_id_to_mesh_terms,
                                             reference_id_to_mod_corpus_data,
                                             resource_id_to_journal,
                                             json_path + json_file, datestamp)
    except Exception as e:
        print("Error occurred when retrieving data from Reference data and generating json file: " + str(e))
        return

    log.info("Uploading json file to s3...")
    try:
        upload_json_file_to_s3(json_path, json_file, datestamp, ondemand)
    except Exception as e:
        print("Error occurred when uploading json file to s3: " + str(e))
        return

    log.info("DONE!")


def generate_json_file(metaData, data, filename_with_path):

    try:
        jsonData = {"data": data,
                    "metaData": metaData}
        fw = open(filename_with_path, 'w')
        fw.write(json.dumps(jsonData, indent=4, sort_keys=True))
        fw.close()
    except Exception as e:
        print("Error when generating", filename_with_path, ": " + str(e))
        exit()


def upload_json_file_to_s3(json_path, json_file, datestamp, ondemand):

    env_state = environ.get('ENV_STATE', 'develop')
    if env_state == 'build':
        env_state = 'develop'
    if env_state == 'test':
        return

    json_file_with_datestamp = json_file.replace('.json', '_' + datestamp + '.json')

    if ondemand:
        s3_filename = ondemand_bucket.replace('develop', env_state) + json_file_with_datestamp
        upload_file_to_s3(json_path + json_file, s3_bucket, s3_filename)
        return

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


def get_reference_data_and_generate_json(mod_id, mod, reference_id_to_source, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_comment_correction_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, json_file_with_path, datestamp):

    metaData = get_meta_data(mod, datestamp)

    data = []

    db_session = create_postgres_session(False)

    i = 0
    j = 0
    for index in range(loop_count):

        if i >= max_per_db_connection:
            i = 0
            db_session.close()
            json_file = json_file_with_path + "_" + str(j)
            print("generating", json_file + ": data size=", len(data))
            generate_json_file(metaData, data, json_file)
            data = []
            j += 1
            db_session = create_postgres_session(False)

        offset = index * limit

        print("offset=", offset, "data=", len(data))

        all = db_session.query(
            ReferenceModel
        ).join(
            ReferenceModel.mod_corpus_association
        ).filter(
            ModCorpusAssociationModel.mod_id == mod_id
        ).order_by(
            ReferenceModel.reference_id
        ).offset(
            offset
        ).limit(
            limit
        ).all()

        if len(all) == 0:
            ## finished retrieving all data from database
            if len(data) > 0:
                json_file = json_file_with_path + "_" + str(j)
                print("generating", json_file + ": data size=", len(data))
                generate_json_file(metaData, data, json_file)
            print("concatenating", j + 1, "small json files to a single json file:", json_file_with_path)
            concatenate_json_files(json_file_with_path, j + 1)
            return

        ref_data = []
        for x in all:
            if x.reference_id not in reference_id_to_source:
                continue
            ref_data.append(x)
            i += 1
            if i % 50 == 0:
                print(i, x.curie)

        generate_json_data(ref_data, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_comment_correction_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, data)

    db_session.close()


def generate_json_data(ref_data, reference_id_to_xrefs, reference_id_to_authors, reference_id_to_comment_correction_data, reference_id_to_mod_reference_types, reference_id_to_mesh_terms, reference_id_to_mod_corpus_data, resource_id_to_journal, data):

    for x in ref_data:

        row = {'reference_id': x.reference_id,
               'curie': x.curie,
               'category': x.category,
               'title': x.title,
               'volume': x.volume,
               'page_range': x.page_range,
               'issue_name': x.issue_name,
               'language': x.language,
               'pubmed_types': x.pubmed_types,
               'pubmed_publication_status': x.pubmed_publication_status,
               'abstract': x.abstract,
               'keywords': x.keywords,
               'plain_language_abstract': x.plain_language_abstract,
               'pubmed_abstract_languages': x.pubmed_abstract_languages,
               'date_published': str(x.date_published),
               'date_arrived_in_pubmed': str(x.date_arrived_in_pubmed),
               'date_last_modified_in_pubmed': str(x.date_last_modified_in_pubmed),
               'resource_id': x.resource_id,
               'publisher': x.publisher,
               'open_access': x.open_access,
               'date_created': str(x.date_created),
               'date_updated': str(x.date_updated)}

        row['authors'] = reference_id_to_authors.get(x.reference_id, [])

        row['cross_references'] = reference_id_to_xrefs.get(x.reference_id, [])

        row['mod_reference_types'] = reference_id_to_mod_reference_types.get(x.reference_id, [])

        row['mesh_terms'] = reference_id_to_mesh_terms.get(x.reference_id, [])

        row['comment_and_corrections'] = reference_id_to_comment_correction_data.get(x.reference_id, {})

        row['mod_corpus_associations'] = reference_id_to_mod_corpus_data.get(x.reference_id, [])

        if x.resource_id in resource_id_to_journal:
            (resource_curie, resource_title) = resource_id_to_journal[x.resource_id]
            row['resource_curie'] = resource_curie
            row['resource_title'] = resource_title
        else:
            row['resource_curie'] = None
            row['resource_title'] = None

        data.append(row)


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
    reference_id_to_pmid = {}
    for x in db_session.query(CrossReferenceModel).join(ReferenceModel.cross_reference).outerjoin(ReferenceModel.mod_corpus_association).filter(ModCorpusAssociationModel.mod_id == mod_id).all():
        data = []
        if x.reference_id in reference_id_to_xrefs:
            data = reference_id_to_xrefs[x.reference_id]
        row = {"curie": x.curie,
               "is_obsolete": x.is_obsolete}
        if x.curie.startswith('PMID:'):
            row['pages'] = x.pages
            row['url'] = "https://www.ncbi.nlm.nih.gov/pubmed/" + x.curie.replace("PMID:", "")
        elif x.curie.startswith('PMCID:'):
            row['pages'] = x.pages
            row['url'] = "https://www.ncbi.nlm.nih.gov/pmc/" + x.curie.replace("PMCID:", "")
        elif x.curie.startswith('DOI:'):
            row['pages'] = x.pages
            row['url'] = "https://doi.org/doi:" + x.curie.replace("DOI:", "")
        elif x.curie.startswith('SGD:'):
            row['pages'] = [{"name": "reference",
                             "url": "https://www.yeastgenome.org/reference/" + x.curie.replace("SGD:", "")}]
            row['url'] = "https://www.yeastgenome.org/reference/" + x.curie.replace("SGD:", "")
        elif x.curie.startswith('WB:'):
            row['pages'] = [{"name": "reference",
                             "url": "https://www.wormbase.org/db/get?name=" + x.curie.replace("WB:", "") + ";class=Paper"}]
            row['url'] = "https://www.wormbase.org/db/get?name=" + x.curie.replace("WB:", "")
        elif x.curie.startswith('MGI:'):
            row['pages'] = [{"name": "reference",
                             "url": "http://www.informatics.jax.org/reference/" + x.curie}]
            row['url'] = "http://www.informatics.jax.org/"
        elif x.curie.startswith('FB:'):
            row['pages'] = [{"name": "reference",
                             "url": "https://flybase.org/reports/" + x.curie.replace("FB:", "") + ".html"}]
            row['url'] = "https://flybase.org/reports/" + x.curie.replace("FB:", "") + ".html"
        elif x.curie.startswith('ZFIN:'):
            row['pages'] = [{"name": "reference",
                             "url": "https://zfin.org/" + x.curie.replace("ZFIN:", "")}]
            row['url'] = "https://zfin.org/" + x.curie.replace("ZFIN:", "")
        elif x.curie.startswith('RGD:'):
            row['pages'] = [{"name": "reference",
                             "url": "https://rgd.mcw.edu/rgdweb/report/reference/main.html?id=" + x.curie}]
            row['url'] = "https://rgd.mcw.edu/rgdweb/elasticResults.html?term=" + x.curie
        data.append(row)
        reference_id_to_xrefs[x.reference_id] = data
        if x.curie.startswith('PMID:'):
            reference_id_to_pmid[x.reference_id] = x.curie.replace("PMID:", "")

    return (reference_id_to_xrefs, reference_id_to_pmid)


def get_author_data(db_session, mod_id):

    orcid_to_cross_ref = {}
    for x in db_session.query(CrossReferenceModel).filter(CrossReferenceModel.curie.like('ORCID:%')).all():
        orcid_to_cross_ref[x.curie] = (x.pages, x.is_obsolete)

    reference_id_to_authors = {}
    for x in db_session.query(AuthorModel).join(ReferenceModel.author).outerjoin(ReferenceModel.mod_corpus_association).filter(ModCorpusAssociationModel.mod_id == mod_id).order_by(AuthorModel.reference_id, AuthorModel.order).all():
        data = []
        if x.reference_id in reference_id_to_authors:
            data = reference_id_to_authors[x.reference_id]
        orcid = x.orcid
        if x.orcid and x.orcid in orcid_to_cross_ref:
            (pages, is_obsolete) = orcid_to_cross_ref[x.orcid]
            orcid = {"curie": x.orcid,
                     "url": "https://orcid.org/" + x.orcid.replace("ORCID:", ""),
                     "pages": pages,
                     "is_obsolete": is_obsolete}
        elif x.orcid:
            orcid = {"curie": x.orcid,
                     "url": "https://orcid.org/" + x.orcid.replace("ORCID:", ""),
                     "pages": None,
                     "is_obsolete": False}
        data.append({"author_id": x.author_id,
                     "name": x.name,
                     "first_name": x.first_name,
                     "last_name": x.last_name,
                     "order": x.order,
                     "affilliation": x.affiliations if x.affiliations else [],
                     "orcid": orcid,
                     "first_author": x.first_author,
                     "corresponding_author": x.corresponding_author})
        reference_id_to_authors[x.reference_id] = data

    return reference_id_to_authors


def get_comment_correction_data(db_session, reference_id_to_pmid):

    reference_id_to_comment_correction_data = {}

    type_mapping = {'ErratumFor': 'ErratumIn',
                    'RepublishedFrom': 'RepublishedIn',
                    'RetractionOf': 'RetractionIn',
                    'ExpressionOfConcernFor': 'ExpressionOfConcernIn',
                    'ReprintOf': 'ReprintIn',
                    'UpdateOf': 'UpdateIn'}

    for x in db_session.query(ReferenceCommentAndCorrectionModel).all():

        type_db = x.reference_comment_and_correction_type
        type_db = type_db.replace("ReferenceCommentAndCorrectionType.", "")

        ## for reference_id_from
        data = {}
        if x.reference_id_from in reference_id_to_comment_correction_data:
            data = reference_id_to_comment_correction_data[x.reference_id_from]
        pmid = None
        if x.reference_id_from in reference_id_to_pmid:
            pmid = reference_id_to_pmid[x.reference_id_from]
        else:
            pmid = get_pmid_for_reference_id(db_session, x.reference_id_from)
        if pmid:
            data[type_db] = "PMID:" + pmid
            reference_id_to_comment_correction_data[x.reference_id_from] = data

        ## for reference_id_to
        data = {}
        if x.reference_id_to in reference_id_to_comment_correction_data:
            data = reference_id_to_comment_correction_data[x.reference_id_to]
        pmid = None
        if x.reference_id_to in reference_id_to_pmid:
            pmid = reference_id_to_pmid[x.reference_id_to]
        else:
            pmid = get_pmid_for_reference_id(db_session, x.reference_id_to)
        if pmid:
            type = type_mapping.get(type_db)
            if type is None:
                log.info(type_db + " is not in type_mapping.")
            if type:
                data[type] = "PMID:" + pmid
                reference_id_to_comment_correction_data[x.reference_id_to] = data

    return reference_id_to_comment_correction_data


def get_pmid_for_reference_id(db_session, reference_id):

    x = db_session.query(CrossReferenceModel).filter_by(reference_id=reference_id).filter(CrossReferenceModel.curie.like('PMID:%')).one_or_none()
    if x:
        return x.curie
    else:
        return None


def get_mesh_term_data(db_session, mod_id):

    reference_id_to_mesh_terms = {}

    mesh_limit = 1000000
    for index in range(10):
        offset = index * mesh_limit
        all = db_session.query(MeshDetailModel).join(
            ReferenceModel.mesh_term
        ).outerjoin(
            ReferenceModel.mod_corpus_association
        ).filter(
            ModCorpusAssociationModel.mod_id == mod_id
        ).order_by(
            MeshDetailModel.mesh_detail_id
        ).limit(
            mesh_limit
        ).offset(
            offset
        ).all()

        if len(all) == 0:
            break

        for x in all:
            data = []
            if x.reference_id in reference_id_to_mesh_terms:
                data = reference_id_to_mesh_terms[x.reference_id]
            data.append({"heading_term": x.heading_term,
                         "qualifier_term": x.qualifier_term,
                         "mesh_detail_id": x.mesh_detail_id})
            reference_id_to_mesh_terms[x.reference_id] = data

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


def print_usage():

    print("Usage: python dump_json_data.py -m [SGD|WB|FB|ZFIN|MGI|RGD]")
    print("Usage: python dump_json_data.py -m [SGD|WB|FB|ZFIN|MGI|RGD] -o")
    print("Usage example: python dump_json_data.py -m SGD")
    print("Usage example: python dump_json_data.py -m SGD -o")


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mod', action='store', help='which mod, [SGD|WB|FB|ZFIN|MGI|RGD][NONE]')
    parser.add_argument('-o', '--ondemand', action='store_true', help="by curator's request")

    args = vars(parser.parse_args())
    mod = args['mod'] if args.get('mod') else None

    ## set mod to NONE to only update the papers that are not associated with a MOD
    if mod:
        if mod in ['SGD', 'WB', 'FB', 'ZFIN', 'MGI', 'RGD']:
            dump_data(mod, args['ondemand'])
        else:
            print_usage()
    else:
        print_usage()
