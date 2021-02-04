
import json

# import argparse
# import re

from os import path
import logging
import logging.config


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


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


def process_dqm_references():
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
       filename = 'dqm_data/REFERENCE_' + mod + '.json'
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
    output_pmid_file = 'inputs/alliance_pmids'
    with open(output_pmid_file, "w") as pmid_file:
        for pmid in sorted(pmid_stats.iterkeys(), key=int):
            pmid_file.write(pmid)
            pmid_file.write("\n")
        pmid_file.close()

# output pmids and the mods that have them
#     for identifier in pmid_stats:
#         ref_mods_list = pmid_stats[identifier]
#         count = len(ref_mods_list)
#         ref_mods_str = ", ".join(ref_mods_list)
#         print("pmid %s\t%s\t%s" % (identifier, count, ref_mods_str))
#         logger.info("pmid %s\t%s\t%s", identifier, count, ref_mods_str)
    
    
    # for primary_id in primary_ids:
    #     logger.info("primary_id %s", primary_id)

if __name__ == "__main__":
    """ call main start function """
    process_dqm_references()

