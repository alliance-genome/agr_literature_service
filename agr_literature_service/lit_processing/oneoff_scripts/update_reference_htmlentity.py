from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
import html
from agr_literature_service.api.models import (
    AuthorModel,
    ReferenceModel,
    MeshDetailModel
)
# from agr_literature_service.api.crud.reference_crud import update_citation
import time
import logging

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def update_author_entity():
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print("start at:" + current_time)
    try:
        db_session = create_postgres_session(False)
        author_results = db_session.execute(
            "select  name, first_name, last_name, affiliations, author_id, reference_id from  author ")
        ids = author_results.fetchall()
        for id in ids:
            author_name = id["name"]
            author_first_name = id["first_name"]
            author_last_name = id["last_name"]
            author_affiliations_list = id["affiliations"]
            author_id = id["author_id"]
            reference_id = id["reference_id"]
            flag_affiliation = False
            if author_affiliations_list:
                for x in range(len(author_affiliations_list)):
                    affiliation = author_affiliations_list[x]
                    if affiliation:
                        affiliation_unescaped = html.unescape(affiliation)
                    if affiliation and affiliation_unescaped and affiliation != affiliation_unescaped:
                        author_affiliations_list[x] = affiliation_unescaped
                        flag_affiliation = True
            if author_name:
                author_name_unescaped = html.unescape(author_name)
            if author_first_name:
                author_first_name_unescaped = html.unescape(author_first_name)
            if author_last_name:
                author_last_name_unescaped = html.unescape(author_last_name)
            dataDict = {}
            if author_name and author_name_unescaped and author_name_unescaped != author_name:
                dataDict["name"] = author_name_unescaped
            if author_first_name and author_first_name_unescaped and author_first_name_unescaped != author_first_name:
                dataDict["first_name"] = author_first_name_unescaped
            if author_last_name and author_last_name_unescaped and author_last_name_unescaped != author_last_name:
                dataDict["last_name"] = author_last_name_unescaped
            if flag_affiliation:
                dataDict["affiliations"] = author_affiliations_list
            if dataDict:
                author_db_obj = db_session.query(AuthorModel).filter(AuthorModel.author_id == author_id).first()
                for key, value in dataDict.items():
                    setattr(author_db_obj, key, value)
                    log.info("Will update key: " + key)
                db_session.commit()
                db_session.query(ReferenceModel).filter(ReferenceModel.reference_id == reference_id).first()
                # if ref_db_obj:
                #     curie = ref_db_obj.curie
                #     update_citation(db_session, curie)
        db_session.close()
    except Exception as e:
        log.info("Error " + str(e))
    current_time = time.strftime("%H:%M:%S", t)
    log.info("End at: " + current_time)


def update_mesh_detail_entity():
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print("start at:" + current_time)
    try:
        db_session = create_postgres_session(False)
        mesh_detail_results = db_session.execute(
            " select  qualifier_term, heading_term,  mesh_detail_id from mesh_detail m  where   qualifier_term is not null")
        ids = mesh_detail_results.fetchall()
        for id in ids:
            mesh_detail_id = id["mesh_detail_id"]
            qualifier_term = id["qualifier_term"]
            heading_term = id["heading_term"]
            if qualifier_term:
                qualifier_term_unescaped = html.unescape(qualifier_term)
            if heading_term:
                heading_term_unescaped = html.unescape(heading_term)
            dataDict = {}
            if qualifier_term and qualifier_term_unescaped and qualifier_term_unescaped != qualifier_term:
                dataDict["qualifier_term"] = qualifier_term_unescaped
            if heading_term and heading_term_unescaped and heading_term_unescaped != heading_term:
                dataDict["heading_term"] = heading_term_unescaped
            if dataDict:
                author_db_obj = db_session.query(MeshDetailModel).filter(MeshDetailModel.mesh_detail_id == mesh_detail_id).first()
                for key, value in dataDict.items():
                    setattr(author_db_obj, key, value)
                    log.info("Will update key: " + key)
                db_session.commit()
        db_session.close()
    except Exception as e:
        log.info("Error " + str(e))
    current_time = time.strftime("%H:%M:%S", t)
    log.info("End at: " + current_time)


def update_reference_entity():
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print("start at:" + current_time)
    try:
        db_session = create_postgres_session(False)
        reference_results = db_session.execute(
            "select  abstract, title, plain_language_abstract, publisher, keywords, reference_id from  reference ")
        ids = reference_results.fetchall()
        for id in ids:
            publisher = id["publisher"]
            plain_language_abstract = id["plain_language_abstract"]
            abstract = id["abstract"]
            title = id["title"]
            reference_id = id["reference_id"]
            keywords_list = id["keywords"]
            flag_keywords = False
            if keywords_list:
                for x in range(len(keywords_list)):
                    keyword = keywords_list[x]
                    if keyword:
                        keyword_unescaped = html.unescape(keyword)
                    if keyword and keyword_unescaped and keyword != keyword_unescaped:
                        keywords_list[x] = keyword_unescaped
                        flag_keywords = True
            if publisher:
                publisher_unescaped = html.unescape(publisher)
            if plain_language_abstract:
                plain_language_abstract_unescaped = html.unescape(plain_language_abstract)
            if abstract:
                abstract_unescaped = html.unescape(abstract)
            if title:
                title_unescaped = html.unescape(title)
            referenceDict = {}
            if publisher and publisher_unescaped and publisher_unescaped != publisher:
                referenceDict["publisher"] = publisher_unescaped
            if plain_language_abstract and plain_language_abstract_unescaped and plain_language_abstract_unescaped != plain_language_abstract:
                referenceDict["plain_language_abstract"] = plain_language_abstract_unescaped
            if abstract and abstract_unescaped and abstract_unescaped != abstract:
                referenceDict["abstract"] = abstract_unescaped
            if title and title_unescaped and title_unescaped != title:
                referenceDict["title"] = title_unescaped
            if flag_keywords:
                referenceDict["keywords"] = keywords_list
            if referenceDict:
                reference_db_obj = db_session.query(ReferenceModel).filter(
                    ReferenceModel.reference_id == reference_id).first()
                for key, value in referenceDict.items():
                    setattr(reference_db_obj, key, value)
                    log.info("Will update key: " + key)
                db_session.commit()
        db_session.close()
    except Exception as e:
        log.info("Error " + str(e))
    current_time = time.strftime("%H:%M:%S", t)
    log.info("End at: " + current_time)


if __name__ == "__main__":
    update_author_entity()
    update_reference_entity()
    update_mesh_detail_entity()
