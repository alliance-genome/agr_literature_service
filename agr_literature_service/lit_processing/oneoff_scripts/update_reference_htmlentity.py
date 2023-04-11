from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session
import html
from agr_literature_service.api.models import (
    AuthorModel,
    ReferenceModel
)
# from agr_literature_service.api.crud.reference_crud import update_citation
import time


# update author first_name, last_name, name, affiliation html entity
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
                    # print("will update key:" + key)
                db_session.commit()
                db_session.query(ReferenceModel).filter(ReferenceModel.reference_id == reference_id).first()
                # if ref_db_obj:
                #     curie = ref_db_obj.curie
                #     update_citation(db_session, curie)
        db_session.close()
    except Exception as e:
        print('Error: ' + str(type(e)))
    current_time = time.strftime("%H:%M:%S", t)
    print("end at:" + current_time)


# update reference title and abstract html entity
def update_reference_entity():
    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    print("start at:" + current_time)
    try:
        db_session = create_postgres_session(False)
        reference_results = db_session.execute(
            "select  abstract, title, reference_id from  reference")
        ids = reference_results.fetchall()
        for id in ids:
            abstract = id["abstract"]
            title = id["title"]
            reference_id = id["reference_id"]
            if abstract:
                abstract_unescaped = html.unescape(abstract)
            if title:
                title_unescaped = html.unescape(title)
            referenceDict = {}
            if abstract_unescaped and abstract and abstract_unescaped != abstract:
                referenceDict["abstract"] = abstract_unescaped
            if title_unescaped and title and title_unescaped != title:
                referenceDict["title"] = title_unescaped
            if referenceDict:
                reference_db_obj = db_session.query(ReferenceModel).filter(
                    ReferenceModel.reference_id == reference_id).first()
                for key, value in referenceDict.items():
                    setattr(reference_db_obj, key, value)
                    # print("will update key:" + key)
                db_session.commit()
        db_session.close()
    except Exception as e:
        print('Error: ' + str(type(e)))
    current_time = time.strftime("%H:%M:%S", t)
    print("end at:" + current_time)


if __name__ == "__main__":
    update_author_entity()
    # update_reference_entity()
