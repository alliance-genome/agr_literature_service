import json
from os import environ, makedirs, path

from helper_file_processing import write_json


def sanitize_pubmed_json_list(pmids, inject_list):
    """

    :param pmids:
    :param inject_list: list of object to inject into each pmid json, each object's field replaces the entry field, so if multiple objects would have the same field they should get aggregated before coming here
    :return:
    """

    base_path = environ.get('XML_PATH')
    sanitized_reference_json_path = base_path + 'sanitized_reference_json/'
    if not path.exists(sanitized_reference_json_path):
        makedirs(sanitized_reference_json_path)

    pmid_fields = ['authors', 'volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished',
                   'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher',
                   'meshTerms', 'plainLanguageAbstract', 'pubmedAbstractLanguages', 'crossReferences']
    single_value_fields = ['volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished',
                           'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'publisher',
                           'plainLanguageAbstract', 'pubmedAbstractLanguages']
    replace_value_fields = ['authors', 'pubMedType', 'meshTerms', 'crossReferences']
    date_fields = ['issueDate', 'dateArrivedInPubmed', 'dateLastModified']

    sanitized_data = []
    for pmid in pmids:
        pubmed_json_filepath = base_path + 'pubmed_json/' + pmid + '.json'
        try:
            pubmed_data = dict()
            with open(pubmed_json_filepath, 'r') as f:
                pubmed_data = json.load(f)
                f.close()
            entry = dict()
            entry['primaryId'] = 'PMID:' + pmid
            if 'nlm' in pubmed_data:
                entry['resource'] = 'NLM:' + pubmed_data['nlm']
            entry['category'] = 'unknown'
            for pmid_field in pmid_fields:
                if pmid_field in single_value_fields:
                    pmid_data = ''
                    if pmid_field in pubmed_data:
                        if pmid_field in date_fields:
                            pmid_data = pubmed_data[pmid_field]['date_string']
                        else:
                            pmid_data = pubmed_data[pmid_field]
                    if pmid_data != '':
                        entry[pmid_field] = pmid_data
                elif pmid_field in replace_value_fields:
                    if pmid_field in pubmed_data:
                        entry[pmid_field] = pubmed_data[pmid_field]
            for inject_object in inject_list:
                for inject_field in inject_object:
                    entry[inject_field] = inject_object[inject_field]
            sanitized_data.append(entry)
        except IOError:
            print(pubmed_json_filepath + ' not found in filesystem')
    # json_filename = sanitized_reference_json_path + 'REFERENCE_PUBMED_' + pmid + '.json'
    json_filename = sanitized_reference_json_path + 'REFERENCE_PUBMED_PMID.json'

    write_json(json_filename, sanitized_data)
