"""
xml_to_json
===========

module that converts XMLs to JSON files


pipenv run python xml_to_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/inputs/sample_set

22 minutes on dev.wormbase for 646727 documents from filesystem. 12G of xml to 6.0G of json
1 hour 55 minutes on agr-literature-dev for 649074 documents from filesystem.  15G of xml to 8.0G of json

pipenv run python xml_to_json.py -u "http://tazendra.caltech.edu/~azurebrd/cgi-bin/forms/generic.cgi?action=ListPmids"


not using author firstinit, nlm, issn

update sample
cp pubmed_json/32542232.json pubmed_sample
cp pubmed_json/32644453.json pubmed_sample
cp pubmed_json/33408224.json pubmed_sample
cp pubmed_json/33002525.json pubmed_sample
cp pubmed_json/33440160.json pubmed_sample
cp pubmed_json/33410237.json pubmed_sample
git add pubmed_sample/32542232.json
git add pubmed_sample/32644453.json
git add pubmed_sample/33408224.json
git add pubmed_sample/33002525.json
git add pubmed_sample/33440160.json
git add pubmed_sample/33410237.json

https://ftp.ncbi.nih.gov/pubmed/J_Medline.txt

Processing CommentIn/CommentOn from commentsCorrections results in 12-deep recursive chain of PMID comments, e.g. 32919857, but also many others fairly deep.
Removing CommentIn/CommentOn from allowed RefType values the deepest set is 4 deep, Recursive example 26757732 -> 26868856 -> 26582243 -> 26865040 -> 27032729

Need to set up a queue that queries postgres to get a list of pubmed id that don't have a pubmed final flag
Need to set up flags to take in pmids from postgres queue, file in filesystem, file in URL, list from command line

to get set of pmids with search term 'elegans'
https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term=elegans&retmax=100000000
"""

from collections import defaultdict
import hashlib
import json
import logging
import os
import re
import sys
import urllib.request

import click
import coloredlogs

import calendar

# from dotenv import load_dotenv
#
# load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")


known_article_id_types = {
    "pubmed": {"prefix": "PMID:"},
    "doi": {"prefix": "DOI:"},
    "pmc": {"prefix": "PMCID:"},
}
#     'pubmed': {'pages': 'PubMed', 'prefix': 'PMID:'},
#     'doi': {'pages': 'DOI', 'prefix': 'DOI:'},
#     'pmc': {'pages': 'PMC', 'prefix': 'PMCID:'}}
ignore_article_id_types = {"bookaccession", "mid", "pii", "pmcid"}
unknown_article_id_types = set([])


def get_year_month_day_from_xml_date(pub_date):
    """

    :param pub_date:
    :return:
    """

    date_list = []
    month = "01"
    day = "01"

    try:
        year = re.search("<Year>(.+?)</Year>", pub_date).group(1)
    except AttributeError:
        year = ""

    try:
        month = re.search("<Month>(.+?)</Month>", pub_date).group(1)
        if not month.isdigit():
            month = f"{list(calendar.month_abbr).index(month):02d}"
    except Exception as e:
        month = "01"

    try:
        day = re.search("<Day>(.+?)</Day>", pub_date).group(1)
    except Exception as e:
        day = "01"

    return year, month, day


def get_medline_date_from_xml_date(xml_data):
    """

    :param pub_date:
    :return:
    """

    medline_re_output = re.search("<MedlineDate>(.+?)</MedlineDate>", xml_data)
    if medline_re_output is not None:
        return medline_re_output.group(1)


def get_title(xml_data, is_book, is_vernacular):
    """

    :param xml_data: 
    :param is_book: 
    :param is_vernacular: 
    :return: 
    """

    if is_book:
        title = re.search("<BookTitle[^>]*?>(.+?)</BookTitle>", xml_data, re.DOTALL).group(1).rstrip()
    elif is_vernacular:
        title = re.search("<VernacularTitle[^>]*?>(.+?)</VernacularTitle>", xml_data, re.DOTALL).group(1).rstrip()
    else:
        title = re.search("<ArticleTitle[^>]*?>(.+?)</ArticleTitle>", xml_data, re.DOTALL).group(1).rstrip()

    return title


def get_section(xml_data, section):
    """

    :param xml_data:
    :return:
    """

    try:
        section = re.search(f"<{section}>(.+?)</{section}>", xml_data).group(1)
        return section
    except AttributeError:
        return ""


def get_comments_corrections(xml_data, pmids, previous_pmids = []):
    """

    :param xml_data:
    :return:
    """


    # <CommentsCorrectionsList><CommentsCorrections RefType="CommentIn"><RefSource>Mult Scler.
    # 1999 Dec;5(6):378</RefSource><PMID Version="1">10644162</PMID></CommentsCorrections><CommentsCorrections
    # RefType="CommentIn"><RefSource>Mult Scler. 2000 Aug;6(4):291-2</RefSource><PMID Version="1">10962551</PMID>
    # </CommentsCorrections></CommentsCorrectionsList>

    ref_types_set = set([])
    new_pmids_set = set([])

    comments_corrections_group = re.findall("<CommentsCorrections (.+?)</CommentsCorrections>", xml_data, re.DOTALL)
    comments_dict = {}
    if len(comments_corrections_group) > 0:
        for comcor_xml in comments_corrections_group:
            ref_type = ""
            other_pmid = ""
            ref_type_re_output = re.search('RefType="(.*?)"', comcor_xml)
            if ref_type_re_output is not None:
                ref_type = ref_type_re_output.group(1)
            other_pmid_re_output = re.search("<PMID[^>]*?>(.+?)</PMID>", comcor_xml)
            if other_pmid_re_output is not None:
                other_pmid = other_pmid_re_output.group(1)
            if (other_pmid != "") and (ref_type != "") and (ref_type != "CommentIn") \
                    and (ref_type != "CommentOn"):
                if ref_type in comments_dict:
                    if other_pmid not in comments_dict[ref_type]:
                        comments_dict[ref_type].append(other_pmid)
                else:
                    comments_dict[ref_type] = [other_pmid]
                ref_types_set.add(ref_type)
                if other_pmid not in pmids and other_pmid not in previous_pmids:
                    new_pmids_set.add(other_pmid)

    return comments_dict, ref_types_set, new_pmids_set


def get_publication_type(xml_data):
    """

    :param xml_data:
    :return:
    """

    try:
        types_group = re.findall('<PublicationType UI=".*?">(.+?)</PublicationType>', xml_data)
    except:
        types_group = re.findall("<PublicationType>(.+?)</PublicationType>", xml)

    return types_group


def get_authors(xml_data):
    """

    :param xml_data:
    :return:
    """

    # this will need to be restructured to match schema
    authors_group = re.findall("<Author.*?>(.+?)</Author>", xml_data, re.DOTALL)
    authors_list = []
    author_cross_references = []
    author_rank = 0
    if len(authors_group) > 0:
        for author_xml in authors_group:
            author = {}
            author_rank += 1

            try:
                 author["lastname"] = re.search("<LastName>(.+?)</LastName>", author_xml).group(1)
            except AttributeError:
                author["lastname"] = ""

            try:
                author["firstname"] = re.search("<ForeName>(.+?)</ForeName>", author_xml).group(1)
            except AttributeError:
                author["firstname"] = ""

            try:
                author["firstinit"] = re.search("<Initials>(.+?)</Initials>", author_xml).group(1)
            except AttributeError:
                author["firstinit"] = ""

            if len(author["firstname"]) == 0:
                author["firstname"] = author["firstinit"]

            # e.g. 27899353 30979869
            try:
                collective_name = re.search("<CollectiveName>(.+?)</CollectiveName>", author_xml).group(1).replace("\n", " ").replace("\r", "")
                author["collective_name"] = re.sub(r"\s+", " ", collective_name)
            except AttributeError:
                author["collective_name"] = ""

            # e.g. 30003105   <Identifier Source="ORCID">0000-0002-9948-4783</Identifier>
            # e.g. 30002370   <Identifier Source="ORCID">http://orcid.org/0000-0003-0416-374X</Identifier>
            try:
                orcid_one = re.search("<Identifier Source=\"ORCID\">(.+?)</Identifier>", author_xml).group(1)
                orcid_two = re.search('<Identifier Source="ORCID">.*?([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]).*?</Identifier>',author_xml).group(1)
                orcid_dict = {"id": "ORCID:" + orcid_two, "pages": ["person/orcid"]}
                author_cross_references.append(orcid_dict)
                author["orcid"] = orcid_dict
            except AttributeError:
                pass
                # logger.info("No ORCID found for author")

            # e.g. 30003105 30002370
            # <AffiliationInfo>
            # <Affiliation>Department of Animal Medical Sciences, Faculty of Life Sciences, Kyoto Sangyo University , Kyoto , Japan.</Affiliation>
            # </AffiliationInfo>
            affiliation_list = []
            try:
                affiliation_info_group = re.findall("<AffiliationInfo>(.*?)</AffiliationInfo>", author_xml, re.DOTALL)
                for affiliation_info in affiliation_info_group:
                    affiliation_group = re.findall("<Affiliation>(.+?)</Affiliation>", affiliation_info, re.DOTALL)
                    for affiliation in affiliation_group:
                        if affiliation not in affiliation_list:
                            affiliation_list.append(affiliation)
            except AttributeError:
                logger.info("No affiliation found for author")

            fullname = ""
            if author["firstname"] != "" and author["lastname"] != "":
                fullname = author["firstname"] + " " + author["lastname"]
            elif author["collective_name"] != "":
                fullname = collective_name
            elif author["lastname"] != "":
                fullname = author["lastname"]
            else:
                logger.warning(f"No name match {author_xml}")

            author["name"] = fullname
            author["authos_rank"] = author_rank
            author["affiliation"] = affiliation_list
            if len(author_cross_references) > 0:
                author["crossReferences"] = author_cross_references
            authors_list.append(author)

    return authors_list


def get_dates(xml_data, date_type):
    """

    1524678 2993907 have MedlineDate instead of Year Month Day
    datePublished is a string, not a date-time

    :param xml_data:
    :param date_type:
    :return:
    """

    date_string = ""
    date_dict = {}
    print(date_type)
    try:
        if date_type == "PubMedPubDate":
            pub_date = re.search("<PubMedPubDate PubStatus=\"received\">(.+?)</PubMedPubDate>", xml, re.DOTALL).group(1)
        else:
            pub_date = re.search(f"<{date_type}>(.+?)</{date_type}>", xml_data, re.DOTALL).group(1)
        year, month, day = get_year_month_day_from_xml_date(pub_date)
        date_string = f"{year}-{month}-{day}"
        date_dict = {"date_string": date_string,
                     "year": year, "month": month, "day": day}

        return date_dict
    except Exception as e:
        logger.error(str(e))
        medline_date = get_medline_date_from_xml_date(xml_data)
        print(medline_date)
        if medline_date:
            date_string = medline_date
            date_dict = medline_date
        return date_dict

    return date_dict

def generate_json(pmids, base_path, previous_pmids=[]):  # noqa: C901
    """

    :param pmids:
    :param previous_pmids:
    :return:
    """

    # open input xml file and read data in form of python dictionary using xmltodict module
    md5data = ""

    xml_storage_path = os.path.join(base_path, "pubmed_xml")
    json_storage_path = os.path.join(base_path, "pubmed_json")

    if not os.path.exists(json_storage_path):
        logger.info(f"Creating directory {json_storage_path}")
        os.makedirs(json_storage_path)
    else:
        logger.info(f"Directory {json_storage_path} already exists")

    for pmid in pmids:

        is_book = False
        is_vernacular = False

        filename = os.path.join(xml_storage_path, f"{pmid}.xml")
        logger.info(f"Pocessing {filename}")

        with open(filename) as xml_file:
            xml = xml_file.read()

            data_dict = {}

            # e.g. 21290765 has BookDocument and ArticleTitle
            if re.search("<BookDocument>", xml):
                # e.g. 33054145 21413221
                is_book = True
                data_dict["is_book"] = "book"
            elif re.search("<VernacularTitle>", xml):
                # e.g. 28304499 28308877
                is_vernacular = True
                data_dict["is_vernacular"] = "vernacular"
            else:
                data_dict["is_article"] = "article"
                data_dict["is_journal"] = "journal"

            data_dict["title"] = get_title(xml, is_book, is_vernacular)

            if len(data_dict["title"]) == 0:
                logger.warning(f"{pmid} has no title")

            data_dict["journal"] = get_section(xml, "MedlineTA")
            data_dict["pages"] = get_section(xml, "MedlinePgn")
            data_dict["volume"] = get_section(xml, "Volume")
            data_dict["issueName"] = get_section(xml, "Issue")
            data_dict["publicationStatus"] = get_section(xml, "PublicationStatus")
            data_dict["pubMedType"] = get_publication_type(xml)
            comments, new_ref_types, new_pmids = get_comments_corrections(xml, pmids)
            data_dict["commentsCorrections"] = comments
            data_dict["authors"] = get_authors(xml)
            data_dict["datePublished"] = get_dates(xml, "PubDate")
            data_dict["issueDate"] = data_dict['datePublished']['date_string']
            data_dict['date_string'] = data_dict['datePublished']['date_string']
            data_dict["dateLastModified"]  = get_dates(xml, "DateRevised")
            # temp, data_dict["dateArrivedInPubmed"] = get_section(xml, "PubMedPubDate")
            print(data_dict)

            get_authors(xml)

    #
    #         cross_references = []
    #         has_self_pmid = False  # e.g. 20301347, 21413225 do not have the PMID itself in the ArticleIdList, so must be appended to the cross_references
    #         article_id_list_re_output = re.search("<ArticleIdList>(.*?)</ArticleIdList>", xml, re.DOTALL)
    #         if article_id_list_re_output is not None:
    #             article_id_list = article_id_list_re_output.group(1)
    #             article_id_group = re.findall('<ArticleId IdType="(.*?)">(.+?)</ArticleId>', article_id_list)
    #             if len(article_id_group) > 0:
    #                 type_has_value = set()
    #                 for type_value in article_id_group:
    #                     type = type_value[0]
    #                     value = type_value[1]
    #                     # convert the only html entities found in DOIs  &lt; &gt; &amp;#60; &amp;#62;
    #                     # e.g. PMID:8824556 PMID:10092111
    #                     value = value.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;#60;", "<").replace("&amp;#62;", ">")
    #                     # print pmid + " type " + type + " value " + value
    #                     if type in known_article_id_types:
    #                         if value == pmid:
    #                             has_self_pmid = True
    #                         if type in type_has_value:
    #                             logger.info("%s has multiple for type %s", pmid, type)
    #                         type_has_value.add(type)
    #                         # cross_references.append({'id': known_article_id_types[type]['prefix'] + value, 'pages': [known_article_id_types[type]['pages']]})
    #                         cross_references.append({"id": known_article_id_types[type]["prefix"] + value})
    #                         data_dict[type] = value  # for cleaning up crossReferences when reading dqm data
    #                     else:
    #                         if type not in ignore_article_id_types:
    #                             logger.info("%s has unexpected type %s", pmid, type)
    #                             unknown_article_id_types.add(type)
    #         if not has_self_pmid:
    #             cross_references.append({"id": "PMID:" + pmid})
    #
    #         medline_journal_info_re_output = re.search("<MedlineJournalInfo>(.*?)</MedlineJournalInfo>", xml, re.DOTALL)
    #         if medline_journal_info_re_output is not None:
    #             medline_journal_info = medline_journal_info_re_output.group(1)
    #             # print(pmid + " medline_journal_info " + medline_journal_info)
    #             nlm = ""
    #             issn = ""
    #             journal_abbrev = ""
    #             nlm_re_output = re.search("<NlmUniqueID>(.+?)</NlmUniqueID>", medline_journal_info)
    #             if nlm_re_output is not None:
    #                 nlm = nlm_re_output.group(1)
    #                 cross_references.append({"id": "NLM:" + nlm})
    #                 # cross_references.append({'id': 'NLM:' + nlm, 'pages': ['NLM']})
    #             issn_re_output = re.search("<ISSNLinking>(.+?)</ISSNLinking>", medline_journal_info)
    #             if issn_re_output is not None:
    #                 issn = issn_re_output.group(1)
    #                 cross_references.append({"id": "ISSN:" + issn})
    #                 # cross_references.append({'id': 'ISSN:' + issn, 'pages': ['ISSN']})
    #             journal_abbrev_re_output = re.search("<MedlineTA>(.+?)</MedlineTA>", medline_journal_info)
    #             if journal_abbrev_re_output is not None:
    #                 journal_abbrev = journal_abbrev_re_output.group(1)
    #             data_dict["nlm"] = nlm  # for mapping to resource
    #             data_dict["issn"] = issn  # for mapping to MOD data to resource
    #             data_dict["resourceAbbreviation"] = journal_abbrev
    #             # check whether all xml has an nlm or issn, for WB set, they all do
    #             # if (nlm and issn):
    #             #     print "GOOD\t" + pmid
    #             # elif nlm:
    #             #     print "NLM\t" + pmid + "\t" + nlm
    #             # elif issn:
    #             #     print "ISSN\t" + pmid + "\t" + issn
    #             # else:
    #             #     print "NO\t" + pmid
    #
    #         if len(cross_references) > 0:
    #             data_dict["crossReferences"] = cross_references
    #
    #         publisher_re_output = re.search("<PublisherName>(.+?)</PublisherName>", xml)
    #         if publisher_re_output is not None:
    #             publisher = publisher_re_output.group(1)
    #             # print publisher
    #             data_dict["publisher"] = publisher
    #
    #         # previously was only getting all abstract text together, but this was causing different types of abstracts to be concatenated
    #         # regex_abstract_output = re.findall("<AbstractText.*?>(.+?)</AbstractText>", xml, re.DOTALL)
    #         # if len(regex_abstract_output) > 0:
    #         #     abstract = " ".join(regex_abstract_output)
    #         #     data_dict['abstract'] = re.sub(r'\s+', ' ', abstract)
    #
    #         main_abstract_list = []
    #         regex_abstract_output = re.findall(
    #             "<Abstract>(.+?)</Abstract>", xml, re.DOTALL
    #         )
    #         if len(regex_abstract_output) > 0:
    #             for abs in regex_abstract_output:
    #                 regex_abstract_text_output = re.findall("<AbstractText.*?>(.+?)</AbstractText>", abs, re.DOTALL)
    #                 if len(regex_abstract_text_output) > 0:
    #                     for abstext in regex_abstract_text_output:
    #                         main_abstract_list.append(abstext)
    #         main_abstract = " ".join(main_abstract_list)
    #         if main_abstract != "":
    #             main_abstract = re.sub(r"\s+", " ", main_abstract)
    #
    #         pip_abstract_list = []
    #         plain_abstract_list = []
    #         lang_abstract_list = []
    #         regex_other_abstract_output = re.findall("<OtherAbstract (.+?)</OtherAbstract>", xml, re.DOTALL)
    #         if len(regex_other_abstract_output) > 0:
    #             for other_abstract in regex_other_abstract_output:
    #                 abs_type = ""
    #                 abs_lang = ""
    #                 abs_type_re_output = re.search('Type="(.*?)"', other_abstract)
    #                 if abs_type_re_output is not None:
    #                     abs_type = abs_type_re_output.group(1)
    #                 abs_lang_re_output = re.search('Language="(.*?)"', other_abstract)
    #                 if abs_lang_re_output is not None:
    #                     abs_lang = abs_lang_re_output.group(1)
    #                 if abs_type == "Publisher":
    #                     lang_abstract_list.append(abs_lang)
    #                 else:
    #                     regex_abstract_text_output = re.findall('<AbstractText.*?>(.+?)</AbstractText>', other_abstract, re.DOTALL)
    #                     if len(regex_abstract_text_output) > 0:
    #                         for abstext in regex_abstract_text_output:
    #                             if abs_type == "plain-language-summary":
    #                                 plain_abstract_list.append(abstext)
    #                             elif abs_type == "PIP":
    #                                 pip_abstract_list.append(abstext)
    #         pip_abstract = " ".join(pip_abstract_list)  # e.g. 9643811 has pip but not main
    #         if pip_abstract != "":
    #             pip_abstract = re.sub(r"\s+", " ", pip_abstract)
    #         plain_abstract = " ".join(plain_abstract_list)
    #         if plain_abstract != "":  # e.g. 32338603 has plain abstract
    #             data_dict["plainLanguageAbstract"] = re.sub(r"\s+", " ", plain_abstract)
    #         if len(lang_abstract_list) > 0:  # e.g. 30160698 has fre and spa
    #             data_dict["pubmedAbstractLanguages"] = lang_abstract_list
    #         if main_abstract != "":
    #             data_dict["abstract"] = main_abstract
    #         elif pip_abstract != "":  # e.g. 9643811 has pip but not main abstract
    #             data_dict["abstract"] = pip_abstract
    #
    #         # some xml has keywords spanning multiple lines e.g. 30110134
    #         # others get captured inside other keywords e.g. 31188077
    #         regex_keyword_output = re.findall("<Keyword .*?>(.+?)</Keyword>", xml, re.DOTALL)
    #         if len(regex_keyword_output) > 0:
    #             keywords = []
    #             for keyword in regex_keyword_output:
    #                 keyword = re.sub("<[^>]+?>", "", keyword)
    #                 keyword = keyword.replace("\n", " ").replace("\r", "")
    #                 keyword = re.sub(r"\s+", " ", keyword)
    #                 keyword = keyword.lstrip()
    #                 keywords.append(keyword)
    #             data_dict["keywords"] = keywords
    #
    #         meshs_group = re.findall("<MeshHeading>(.+?)</MeshHeading>", xml, re.DOTALL)
    #         if len(meshs_group) > 0:
    #             meshs_list = []
    #             for mesh_xml in meshs_group:
    #                 descriptor_re_output = re.search("<DescriptorName.*?>(.+?)</DescriptorName>", mesh_xml, re.DOTALL)
    #                 if descriptor_re_output is not None:
    #                     mesh_heading_term = descriptor_re_output.group(1)
    #                     qualifier_group = re.findall("<QualifierName.*?>(.+?)</QualifierName>", mesh_xml, re.DOTALL)
    #                     if len(qualifier_group) > 0:
    #                         for mesh_qualifier_term in qualifier_group:
    #                             mesh_dict = {"referenceId": "PMID:" + pmid, "meshHeadingTerm": mesh_heading_term,
    #                                          "meshQualifierTerm": mesh_qualifier_term}
    #                             meshs_list.append(mesh_dict)
    #                     else:
    #                         mesh_dict = {"referenceId": "PMID:" + pmid, "meshHeadingTerm": mesh_heading_term}
    #                         meshs_list.append(mesh_dict)
    #             # for mesh_xml in meshs_group:
    #             #     descriptor_group = re.findall("<DescriptorName.*?UI=\"(.+?)\".*?>(.+?)</DescriptorName>",
    #             #                                  mesh_xml, re.DOTALL)
    #             #     if len(descriptor_group) > 0:
    #             #         for id_name in descriptor_group:
    #             #             mesh_dict = {}
    #             #             mesh_dict["referenceId"] = id_name[0]
    #             #             mesh_dict["meshHeadingTerm"] = id_name[1]
    #             #             meshs_list.append(mesh_dict)
    #             #     qualifier_group = re.findall("<QualifierName.*?UI=\"(.+?)\".*?>(.+?)</QualifierName>",
    #             #                                 mesh_xml, re.DOTALL)
    #             #     if len(qualifier_group) > 0:
    #             #         for id_name in qualifier_group:
    #             #             mesh_dict = {}
    #             #             mesh_dict["referenceId"] = id_name[0]
    #             #             mesh_dict["meshQualifierTerm"] = id_name[1]
    #             #             meshs_list.append(mesh_dict)
    #             data_dict["meshTerms"] = meshs_list
    #
    #         # generate the object using json.dumps()
    #         # corresponding to json data
    #
    #         # minified
    #         # json_data = json.dumps(data_dict)
    #
    #         # pretty-print
    #         json_data = json.dumps(data_dict, indent=4, sort_keys=True)
    #
    #         # Write the json data to output json file
    #         # UNCOMMENT TO write to json directory
    #         json_filename = json_storage_path + pmid + ".json"
    #         # if getting pmids from directories split into multiple sub-subdirectories
    #         # json_filename = get_path_from_pmid(pmid, 'json')
    #         with open(json_filename, "w") as json_file:
    #             json_file.write(json_data)
    #             json_file.close()
    #         md5sum = hashlib.md5(json_data.encode("utf-8")).hexdigest()
    #         md5data += pmid + "\t" + md5sum + "\n"
    #
    # md5file = json_storage_path + "md5sum"
    # logger.info("Writing md5sum mappings to %s", md5file)
    # with open(md5file, "a") as md5file_fh:
    #     md5file_fh.write(md5data)
    #
    # for unknown_article_id_type in unknown_article_id_types:
    #     logger.warning("unknown_article_id_type %s", unknown_article_id_type)
    #
    # for ref_type in ref_types_set:
    #     logger.info("ref_type %s", ref_type)
    #
    # new_pmids = sorted(new_pmids_set)
    # for pmid in new_pmids:
    #     logger.info("new_pmid %s", pmid)

    # return new_pmids


# def process_tasks(cli, db, ffile, api, sample, url):
#     """
#
#     :param cli:
#     :param db:
#     :param ffile:
#     :param api:
#     :param sample:
#     :param url:
#     :return:
#     """
#
#     # set storage location
#     # todo: see if environment variable check works
#     # base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'
#     if len(os.environ.get("XML_PATH")) == 0:
#         sys.exit()
#     else:
#         base_path = os.environ.get("XML_PATH")
#
#     storage_path = base_path + "pubmed_xml/"
#     logger.info("Base path is at " + base_path)
#     logger.info("XMLs will be saved on " + storage_path)
#
#     pmids = []  # list that will contain the PMIDs to be converted
#
#     # checking parameters
#     if db:
#         # python xml_to_json.py -d
#         logger.info("Processing database entries")
#     elif api:
#         # python xml_to_json.py -r
#         logger.info("Processing rest api entries")
#     elif ffile:
#         # python xml_to_json.py -f /home/azurebrd/git/agr_literature_
#         # service_demo/src/xml_processing/inputs/pmid_file.txt
#         logger.info("Processing file input from " + ffile)
#         # this requires a well structured input
#         pmids = open(ffile).read().splitlines()
#     elif url:
#         # python xml_to_json.py -u http://tazendra.caltech.edu/~azurebrd/var/work/pmid_sample
#         logger.info("Processing url input from %s", url)
#         req = urllib.request.urlopen(url)
#         data = req.read()
#         lines = data.splitlines()
#         for pmid in lines:
#             pmids.append(str(int(pmid)))
#     elif cli:
#         # python xml_to_json.py -c 1234 4576 1828
#         logger.info("Processing commandline input")
#         for pmid in cli:
#             pmids.append(pmid)
#     elif sample:
#         # python xml_to_json.py -s
#         logger.info("Processing hardcoded sample input")
#         pmids = ["12345678", "12345679", "12345680"]
#     # else:
#     #     logger.info("Processing database entries")
#
#     # when iterating manually through list of PMIDs from PubMed XML CommentsCorrections,
#     # and wanting to exclude PMIDs that have already been looked at from original alliance DQM input, or previous iterations.
#     previous_pmids = []
#     previous_pmids_files = []
#     # previous_pmids_files = ['inputs/alliance_pmids', 'inputs/comcor_add1', 'inputs/comcor_add2', 'inputs/comcor_add3']
#     # previous_pmids_files = ['inputs/alliance_pmids', 'inputs/comcor_add1', 'inputs/comcor_add2',
#     #                        'inputs/comcor_add3', 'inputs/comcor_add4', 'inputs/comcor_add5', 'inputs/comcor_add6',
#     #                        'inputs/comcor_add7', 'inputs/comcor_add8', 'inputs/comcor_add9', 'inputs/comcor_add10',
#     #                        'inputs/comcor_add11']
#     for previous_pmids_file in previous_pmids_files:
#         with open(previous_pmids_file, "r") as fp:
#             pmid = fp.readline()
#             while pmid:
#                 previous_pmids.append(pmid.rstrip())
#                 pmid = fp.readline()
#
#     generate_json(pmids, previous_pmids, base_path)
#
#     logger.info("Done converting XML to JSON")


if __name__ == "__main__":
    """
    call main start function
    """

    generate_json(pmids, previous_pmids, base_path)
