
import json
import urllib.request

import argparse
import re

from os import path
import logging
import logging.config

# pipenv run python parse_dqm_json.py -p  takes about 90 seconds to run
# pipenv run python parse_dqm_json.py -f dqm_data/ -m all   takes 3.5 minutes without looking at pubmed json
# pipenv run python parse_dqm_json.py -f dqm_data/ -m all   takes 13.5 minutes with comparing to pubmed json into output chunks without comparing fields for differences
# pipenv run python parse_dqm_json.py -f dqm_data/ -m all   takes 19 minutes with comparing to pubmed json into output chunks and comparing fields for differences

#  pipenv run python parse_dqm_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/dqm_data/ -m MGI > log_mgi
# Loading .env environment variables...
# Killed
# in 4.5 minutes, logs show it read the last pmid
# rewrote to split into chunks of 100000 entries by pubmed vs pubmod, MGI now runs in 3.5 minutes (without doing data comparison)




log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--generate-pmid-data', action='store_true', help='generate pmid outputs')
parser.add_argument('-f', '--file', action='store', help='take input from REFERENCE files in full path')
parser.add_argument('-m', '--mod', action='store', help='which mod, use all or leave blank for all')
# parser.add_argument('-d', '--database', action='store_true', help='take input from database query')
# parser.add_argument('-r', '--restapi', action='store', help='take input from rest api')
# parser.add_argument('-s', '--sample', action='store_true', help='test sample input from hardcoded entries')
# parser.add_argument('-u', '--url', action='store', help='take input from entries in file at url')

args = vars(parser.parse_args())

base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'


def split_identifier(identifier, ignore_error=False):
    """Split Identifier.

    Does not throw exception anymore. Check return, if None returned, there was an error
    """
    prefix = None
    identifier_processed = None
    separator = None

    if ':' in identifier:
        prefix, identifier_processed = identifier.split(':', 1)  # Split on the first occurrence
        separator = ':'
    elif '-' in identifier:
        prefix, identifier_processed = identifier.split('-', 1)  # Split on the first occurrence
        separator = '-'
    else:
        if not ignore_error:
            self.logger.critical('Identifier does not contain \':\' or \'-\' characters.')
            self.logger.critical('Splitting identifier is not possible.')
            self.logger.critical('Identifier: %s', identifier)
            self.missing_keys[key] = 1
        prefix = identifier_processed = separator = None

    return prefix, identifier_processed, separator


def generate_pmid_data():
        # output set of PMID identifiers that will need XML downloaded
        # output pmids and the mods that have them
    mods = ['SGD', 'RGD', 'FB', 'WB', 'MGI', 'ZFIN']
#     mods = ['SGD']

    pmid_stats = dict()
    unknown_prefix = set()
    pmid_references = dict()
    for mod in mods:
        pmid_references[mod] = []
    non_pmid_references = dict()
    for mod in mods:
        non_pmid_references[mod] = []

    check_primary_id_is_unique = True
    check_pmid_is_unique = True

    for mod in mods:
#         filename = 'dqm_data/1.0.1.4_REFERENCE_WB_0.json'
       filename = base_path + 'dqm_data/REFERENCE_' + mod + '.json'
       f = open(filename)
       dqm_data = json.load(f)

       primary_id_unique = dict()
       pmid_unique = dict()

#        wb_papers = dict()
       mod_papers = dict()
       pmid_papers = dict()
       for entry in dqm_data['data']:

           if check_primary_id_is_unique:
               try:
                   primary_id_unique[entry['primaryId']] = primary_id_unique[entry['primaryId']] + 1
               except KeyError:
                   primary_id_unique[entry['primaryId']] = 1

           pmid = '0'
           prefix, identifier, separator = split_identifier(entry['primaryId'])
#            if prefix == 'WB':
#                wb_papers[identifier] = entry
           if prefix == 'PMID':
               pmid = identifier
#                pmid_papers[identifier] = entry
#                try:
#                    pmid_stats[identifier].append(mod)
#                except KeyError:
#                    pmid_stats[identifier] = [mod]
           elif prefix in mods:
#                mod_papers[identifier] = entry
               if 'crossReferences' in entry:
                   for cross_reference in entry['crossReferences']:
                       prefix_xref, identifier_xref, separator_xref = split_identifier(cross_reference['id'])
                       if prefix_xref == 'PMID':
                           pmid = identifier_xref
           else:
               unknown_prefix.add(prefix)

           if pmid is not '0':
               try:
                   pmid_stats[pmid].append(mod)
               except KeyError:
                   pmid_stats[pmid] = [mod]
               if check_pmid_is_unique:
                   try:
                       pmid_unique[pmid] = pmid_unique[pmid] + 1
                   except KeyError:
                       pmid_unique[pmid] = 1
               pmid_references[mod].append(pmid)
           else:
               non_pmid_references[mod].append(entry['primaryId'])

# output check of a mod's non-unique primaryIds
       if check_primary_id_is_unique:
           for primary_id in primary_id_unique:
               if primary_id_unique[primary_id] > 1:
                    print("%s primary_id %s has %s mentions" % (mod, primary_id, primary_id_unique[primary_id]))
# output check of a mod's non-unique pmids (different from above because could be crossReferences
       if check_pmid_is_unique:
           for pmid in pmid_unique:
               if pmid_unique[pmid] > 1:
                    print("%s pmid %s has %s mentions" % (mod, pmid, pmid_unique[pmid]))


#         for identifier in pmid_papers:
#             entry = pmid_papers[identifier]
#             print(identifier)
#         #     print(identifier + ' ' + entry['allianceCategory'])

# TODO create a sample of 100 entries per MOD to play with

# output each mod's count of pmid references
    for mod in pmid_references:
        count = len(pmid_references[mod])
        print("%s has %s pmid references" % (mod, count))
#         logger.info("%s has %s pmid references", mod, count)

# output each mod's count of non-pmid references
    for mod in non_pmid_references:
        count = len(non_pmid_references[mod])
        print("%s has %s non-pmid references" % (mod, count))
#         logger.info("%s has %s non-pmid references", mod, count)

# output actual reference identifiers that are not pmid
#     for mod in non_pmid_references:
#         for primary_id in non_pmid_references[mod]:
#             print("%s non-pmid %s" % (mod, primary_id))
# #             logger.info("%s non-pmid %s", mod, primary_id)

# if a reference has an unexpected prefix, give a warning
    for prefix in unknown_prefix:
        logger.info("WARNING: unknown prefix %s", prefix)

# output set of identifiers that will need XML downloaded
    output_pmid_file = base_path + 'inputs/alliance_pmids'
    with open(output_pmid_file, "w") as pmid_file:
#         for pmid in sorted(pmid_stats.iterkeys(), key=int):	# python 2
        for pmid in sorted(pmid_stats, key=int):
            pmid_file.write("%s\n" % (pmid))
        pmid_file.close()

# output pmids and the mods that have them
    output_pmid_mods_file = base_path + 'pmids_by_mods'
    with open(output_pmid_mods_file, "w") as pmid_mods_file:
        for identifier in pmid_stats:
            ref_mods_list = pmid_stats[identifier]
            count = len(ref_mods_list)
            ref_mods_str = ", ".join(ref_mods_list)
            pmid_mods_file.write("%s\t%s\t%s\n" % (identifier, count, ref_mods_str))
#             logger.info("pmid %s\t%s\t%s", identifier, count, ref_mods_str)
        pmid_mods_file.close()

    # for primary_id in primary_ids:
    #     logger.info("primary_id %s", primary_id)




def simplify_text(text):
    no_html = re.sub('<[^<]+?>', '', text)
    stripped = re.sub("[^a-zA-Z]+", "", no_html)
    clean = stripped.lower()
    return clean

def compare_dqm_pubmed(fh, pmid, field, dqm_data, pubmed_data):
#     to_return = ''
#     logger.info("%s\t%s\t%s\t%s", field, pmid, dqm_data, pubmed_data)
    dqm_clean = simplify_text(dqm_data)
    pubmed_clean = simplify_text(pubmed_data)
    if dqm_clean != pubmed_clean:
        fh.write("dqm and pubmed differ\t%s\t%s\t%s\t%s\n" % (field, pmid, dqm_data, pubmed_data))
#         logger.info("%s\t%s\t%s\t%s", field, pmid, dqm_clean, pubmed_clean)
#         logger.info("%s\t%s\t%s\t%s", field, pmid, dqm_data, pubmed_data)
#         return "%s\t%s\t%s\t%s" % (field, pmid, dqm_data, pubmed_data)
#     else:
#         logger.info("%s\t%s\t%s", field, pmid, 'GOOD')

def chunks(list, size):
    for i in range(0, len(list), size):
        yield list[i:i+size]

def write_json(json_filename, dict_to_output):
    with open(json_filename, "w") as json_file:
        logger.info("Generating JSON for %s", json_filename)
        json_data = json.dumps(dict_to_output, indent=4, sort_keys=True)
#         logger.info("Writing JSON")
        json_file.write(json_data)
#         logger.info("Closing JSON file")
        json_file.close()
#         logger.info("Done with JSON")


def load_pubmed_resource():
   filename = base_path + 'pubmed_resource_json/resource_pubmed_all.json'
   f = open(filename)
   resource_data = json.load(f)
   resource_to_nlm = dict()
   resource_fields = ['primaryId', 'nlm', 'title', 'isoAbbreviation', 'medlineAbbreviation', 'printISSN', 'onlineISSN']
   for entry in resource_data:
       primary_id = entry['primaryId']
       for field in resource_fields:
           if field in entry:
               value = entry[field].lower()
#                if value == '2985088r':
#                    print("2985088r loaded\n")
               if value in resource_to_nlm:
#                    if value == '2985088r':
#                        print("already in 2985088r to %s loaded\n" % (value))
                   if primary_id not in resource_to_nlm[value]:
                       resource_to_nlm[value].append(primary_id)
#                        if value == '2985088r':
#                            print("append in 2985088r to %s loaded\n" % (value))
               else:
                   resource_to_nlm[value] = [ primary_id ]
#                    if value == '2985088r':
#                        print("orig 2985088r to %s loaded\n" % (value))
   return resource_to_nlm
       

def aggregate_dqm_with_pubmed(input_path, input_mod):
        # reads agr_schemas's reference.json to check for dqm data that's not accounted for there.
        # outputs sanitized json to sanitized_reference_json/
        # does checks on dqm crossReferences.  if primaryId is not PMID, and a crossReference is PubMed, assigns PMID to primaryId and to authors's referenceId.
        # if any reference's author doesn't have author Rank, assign authorRank based on array order.
    cross_ref_no_pages_ok_fields = ['DOI', 'PMID', 'PMC', 'PMCID']
    pmid_fields = ['authors', 'volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher', 'meshTerms']
#     single_value_fields = ['volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher']
    single_value_fields = ['volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'publisher']
    replace_value_fields = ['pubMedType', 'meshTerms']
    date_fields = ['issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified']

    resource_to_nlm = load_pubmed_resource()

    compare_if_dqm_empty = False		# do dqm vs pmid comparison even if dqm has no data, by default skip

    mods = ['SGD', 'RGD', 'FB', 'WB', 'MGI', 'ZFIN']
    if input_mod in mods:
        mods = [ input_mod ]

    json_storage_path = base_path + 'sanitized_reference_json/'

    fh_mod_report = dict()
    for mod in mods:
        filename = base_path + 'report_files/' + mod
        fh_mod_report.setdefault(mod, open(filename,'w')) 


    logger.info("Aggregating DQM and PubMed data from %s using mods %s", input_path, mods)
    agr_schemas_reference_json_url = 'https://raw.githubusercontent.com/alliance-genome/agr_schemas/master/ingest/resourcesAndReferences/reference.json'
    schema_data = dict()
    with urllib.request.urlopen(agr_schemas_reference_json_url) as url:
        schema_data = json.loads(url.read().decode())
#         print(schema_data)

# not using this, instead checking if the .xml file exists, which needs to happen anyway
#     pmids_not_found = set()
#     filename = base_path + 'pmids_not_found'
#     with open(filename, 'r') as f:
#         for pmid in f:
#             pmids_not_found.add(pmid)
#         f.close()

# TODO get rid of sanitized_data, read mixed-mod-pmids and bin into mixed data instead of sanitized_pubm*d_data

    for mod in mods:
        filename = args['file'] + '/REFERENCE_' + mod + '.json'
        logger.info("Processing %s", filename)
        unexpected_mod_properties = set()
        dqm_data = dict()
        with open(filename, 'r') as f:
            dqm_data = json.load(f)
            f.close()
#         json_storage_path = base_path + 'sanitized_reference_json/'
#         json_filename = json_storage_path + 'REFERENCE_' + mod + '.json'
#         with open(json_filename, "w") as json_file:
        entries = dqm_data['data']
        sanitized_data = []
        sanitized_pubmod_data = []
        sanitized_pubmed_data = []
        for entry in entries:
            is_pubmed = False
            is_pubmod = True
            update_primary_id = False
            primary_id = entry['primaryId']
            orig_primary_id = entry['primaryId']
#             print("primaryId %s" % (entry['primaryId']))
            for entry_property in entry:
                if entry_property not in schema_data['properties']:
                    unexpected_mod_properties.add(entry_property)
            if 'crossReferences' in entry:
                for cross_reference in entry['crossReferences']:
                    if 'pages' in cross_reference:
                        if len(cross_reference["pages"]) > 1:
                            fh_mod_report[mod].write("mod %s primaryId %s has cross reference %s with pages %s\n" % (mod, primary_id, cross_reference["id"], cross_reference["pages"]))
#                             logger.info("mod %s primaryId %s has cross reference %s with pages %s", mod, primary_id, cross_reference["id"], cross_reference["pages"])
                        else:
                            if not re.match(r"^PMID:[0-9]+", orig_primary_id):
                                if cross_reference["pages"][0] == 'PubMed':
                                    xref_id = cross_reference["id"]
                                    if re.match(r"^PMID:[0-9]+", xref_id):
                                        update_primary_id = True
                                        primary_id = xref_id
                                        entry['primaryId'] = xref_id
                    else:
                        prefix, identifier, separator = split_identifier(cross_reference["id"])
                        if prefix not in cross_ref_no_pages_ok_fields:
                            fh_mod_report[mod].write("mod %s primaryId %s has cross reference %s without pages\n" % (mod, primary_id, cross_reference["id"]))
    #                         logger.debug("mod %s primaryId %s has cross reference %s without pages", mod, primary_id, cross_reference["id"])
            else:
                fh_mod_report[mod].write("mod %s primaryId %s has no cross references\n" % (mod, primary_id))
#                 logger.info("mod %s primaryId %s has no cross references", mod, primary_id)
            pmid_group = re.search(r"^PMID:([0-9]+)", primary_id)
            if pmid_group is None:
#                 print("primaryKey %s is None" % (primary_id))
                if 'authors' in entry:
                    all_authors_have_rank = True
                    for author in entry['authors']:
                        if 'authorRank' not in entry:
                            all_authors_have_rank = False
                    if all_authors_have_rank == False:
                        authors_with_rank = []
                        for i in range(len(entry['authors'])):
                            author = entry['authors'][i]
                            author['authorRank'] = i + 1
                            authors_with_rank.append(author)
                        entry['authors'] = authors_with_rank
                    if update_primary_id:
                        authors_updated = []
                        for author in entry['authors']:
                            author['referenceId'] = primary_id
                            authors_updated.append(author)
                        entry['authors'] = authors_updated
                if 'resourceAbbreviation' in entry:
                    journal = entry['resourceAbbreviation'].lower()
                    if journal not in resource_to_nlm:
                        fh_mod_report[mod].write("primaryId %s has resourceAbbreviation %s not in NLM source file.\n" % (primary_id, entry['resourceAbbreviation']))
                    else:
                        entry['nlm'] = resource_to_nlm[journal]
                else:
                    fh_mod_report[mod].write("primaryId %s does not have a resourceAbbreviation.\n" % (primary_id))
            else:
                pmid = pmid_group[1]
                is_pubmed = True
                is_pubmod = False
#                 print(pmid)
                filename = base_path + 'pubmed_json/' + pmid + '.json'
#                 print("primary_id %s is None reading %s" % (primary_id, filename))
                pubmed_data = dict()
                try:
                    with open(filename, 'r') as f:
                        pubmed_data = json.load(f)
                        f.close()
#                     print("primary_id %s is None data %s" % (primary_id, pubmed_data['authors']))

#     pmid_fields = ['authors', 'volume', 'title', 'pages', 'issueName', 'issueDate', 'datePublished', 'dateArrivedInPubmed', 'dateLastModified', 'abstract', 'pubMedType', 'publisher', 'meshTerms']
                    for pmid_field in pmid_fields:
                        if pmid_field in single_value_fields:
                            pmid_data = ''
                            dqm_data = ''
                            if pmid_field in pubmed_data:
                                if pmid_field in date_fields:
                                    pmid_data = pubmed_data[pmid_field]['date_string']
                                else:
                                    pmid_data = pubmed_data[pmid_field]
                            if pmid_field in entry:
                                dqm_data = entry[pmid_field]
# UNCOMMENT to output log of data comparison between dqm and pubmed
#                             if (dqm_data != '') or (compare_if_dqm_empty):
#                                compare_dqm_pubmed(fh_mod_report[mod], pmid, pmid_field, dqm_data, pmid_data)
                            entry[pmid_field] = pmid_data
                        elif pmid_field in replace_value_fields:
                            if pmid_field in pubmed_data:
#                                 logger.info("PMID %s pmid_field %s data %s", pmid, pmid_field, pubmed_data[pmid_field])
                                entry[pmid_field] = pubmed_data[pmid_field]

                    if 'nlm' in pubmed_data:
                        nlm = pubmed_data['nlm'].lower()
                        if nlm not in resource_to_nlm:
                            fh_mod_report[mod].write("NLM value %s from PMID %s XML does not map to a proper resource.\n" % (pubmed_data['nlm'], pmid))
                    else:
                        if 'is_journal' in pubmed_data:
                            fh_mod_report[mod].write("PMID %s does not have an NLM resource.\n" % (pmid))

#                     fh_mod_report[mod].write("Warning: PMID %s does not have PubMed xml, from Mod %s primary_id %s\n" % (pmid, mod, orig_primary_id))

#                     if 'title' in pubmed_data:
# #                         compare_dqm_pubmed(pmid, 'title', entry['title'], pubmed_data['title'])
#                         entry['title'] = pubmed_data['title']
# #                     else:
# #                         compare_dqm_pubmed(pmid, 'title', entry['title'], '')
# 
#                     if 'authors' in pubmed_data:
#                         entry['authors'] = pubmed_data['authors']
#                     if 'volume' in pubmed_data:
#                         entry['volume'] = pubmed_data['volume']
# 
#                     if 'pages' in pubmed_data:
#                         entry['pages'] = pubmed_data['pages']
#                     if 'issueName' in pubmed_data:
#                         entry['issueName'] = pubmed_data['issueName']
#                     if 'issueDate' in pubmed_data:
#                         entry['issueDate'] = pubmed_data['issueDate']['date_string']
#                     if 'datePublished' in pubmed_data:
#                         entry['datePublished'] = pubmed_data['datePublished']['date_string']
#                     if 'dateArrivedInPubmed' in pubmed_data:
#                         entry['dateArrivedInPubmed'] = pubmed_data['dateArrivedInPubmed']['date_string']
#                     if 'dateLastModified' in pubmed_data:
#                         entry['dateLastModified'] = pubmed_data['dateLastModified']['date_string']
#                     if 'abstract' in pubmed_data:
#                         entry['abstract'] = pubmed_data['abstract']
#                     if 'pubMedType' in pubmed_data:
#                         entry['pubMedType'] = pubmed_data['pubMedType']
#                     if 'publisher' in pubmed_data:
#                         entry['publisher'] = pubmed_data['publisher']
#                     if 'meshTerms' in pubmed_data:
#                         entry['meshTerms'] = pubmed_data['meshTerms']

# TODO datePublished, keywords, and crossReferences
# if datePublished empty in pubmed but has dqm, use dqm.
# #     some papers, like 8805 don't have keyword data, but have data from WB, aggregate from mods ?
# #                     if 'keywords' in pubmed_data:
# #                         entry['keywords'] = pubmed_data['keywords']
# #     these probably need to be aggregated
# #                     if 'crossReferences' in pubmed_data:
# #                         entry['crossReferences'] = pubmed_data['crossReferences']
                except IOError:
                    fh_mod_report[mod].write("Warning: PMID %s does not have PubMed xml, from Mod %s primary_id %s\n" % (pmid, mod, orig_primary_id))
#                     logger.info("Warning: PMID %s does not have PubMed xml, from Mod %s primary_id %s", pmid, mod, orig_primary_id)

            sanitized_data.append(entry)
            if is_pubmod:
                sanitized_pubmod_data.append(entry)
            else:
                sanitized_pubmed_data.append(entry)

        entries_size = 100000
        sanitized_pubmod_list = list(chunks(sanitized_pubmod_data, entries_size))
        for i in range(len(sanitized_pubmod_list)):
            dict_to_output = sanitized_pubmod_list[i]
            json_filename = json_storage_path + 'REFERENCE_PUBMOD_' + mod + '_' + str(i+1) + '.json'
            write_json(json_filename, dict_to_output)

        sanitized_pubmed_list = list(chunks(sanitized_pubmed_data, entries_size))
        for i in range(len(sanitized_pubmed_list)):
            dict_to_output = sanitized_pubmed_list[i]
            json_filename = json_storage_path + 'REFERENCE_PUBMED_' + mod + '_' + str(i+1) + '.json'
            write_json(json_filename, dict_to_output)

# UNCOMMENT TO generate json
#         json_filename = json_storage_path + 'REFERENCE_' + mod + '.json'
#         with open(json_filename, "w") as json_file:
#             logger.info("Generating JSON")
#             json_data = json.dumps(sanitized_data, indent=4, sort_keys=True)
#             logger.info("Writing JSON")
#             json_file.write(json_data)
#             logger.info("Closing JSON file")
#             json_file.close()
#             logger.info("Done with JSON")

        for unexpected_mod_property in unexpected_mod_properties:
            logger.info("Warning: Unexpected Mod %s Property %s", mod, unexpected_mod_property)

    for mod in fh_mod_report:
        fh_mod_report[mod].close()

# file of pmids to modcount to mod list
#     output_pmid_mods_file = base_path + 'pmids_by_mods'
#     with open(output_pmid_mods_file, "w") as pmid_mods_file:

# hash sanitized entries per mod into %sanitized{pmid}{mod} = data
# go through those to aggregate data that should be aggregated
# check for single fields that have different values across mods

# allianceCategory - single value, check they aren't different for entries with same PMID
# MODReferenceTypes - array of hashes, aggregate the hashes
# tags - array of hashes, aggregate the hashes
# resourceAbbreviation - single value, keep for mod data, try to resolve to journal from PMID


if __name__ == "__main__":
    """ call main start function """
    logger.info("starting parse_dqm_json.py")

# pipenv run python parse_dqm_json.py -p
    if args['generate_pmid_data']:
        logger.info("Generating PMID files from DQM data")
        generate_pmid_data()

# pipenv run python parse_dqm_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/dqm_sample/ -m ZFIN
# pipenv run python parse_dqm_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/dqm_sample/ -m SGD
# pipenv run python parse_dqm_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/dqm_sample/ -m WB
# pipenv run python parse_dqm_json.py -f /home/azurebrd/git/agr_literature_service_demo/src/xml_processing/dqm_sample/ -m all
    elif args['file']:
        if args['mod']:
            aggregate_dqm_with_pubmed(args['file'], args['mod'])
        else:
            aggregate_dqm_with_pubmed(args['file'], 'all')

    else:
        logger.info("No flag passed in.  Use -h for help.")

    logger.info("ending parse_dqm_json.py")

