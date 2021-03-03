
# takes pmids_not_found from get_pubmed_xml.py, and pmids_by_mods from parse_dqm_json.py, and
# generates a set sorted by MODs of pmids that were not found in pubmed.
#
# python sort_mod_pmids_not_found.py


from os import path
import logging
import logging.config


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')


# todo: save this in an env variable
base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'


def sort_not_found_pmids_by_mod():
    mod_to_pmids = dict()

    pmids_by_mods_file = base_path + 'pmids_by_mods'
    pmid_to_mod = dict()
    with open(pmids_by_mods_file) as mods_file:
        mods_data = mods_file.read()
        mods_split = mods_data.split("\n")
        for line in mods_split:
            if line == '':
                continue
            tabs = line.split("\t")
            pmid = tabs[0]
            if len(tabs) < 2:
                print("line %s short" % (line))
            mods = tabs[2].split(", ")
            for mod in mods:
                try:
                    pmid_to_mod[pmid].append(mod)
                except KeyError:
                    pmid_to_mod[pmid] = [mod]
        mods_file.close()

    pmids_not_found_file = base_path + 'pmids_not_found'
    with open(pmids_not_found_file) as not_found_file:
        not_found_data = not_found_file.read()
        not_found_split = not_found_data.split("\n")
        for pmid in not_found_split:
            if pmid == '':
                continue
            for mod in pmid_to_mod[pmid]:
                # print("%s\t%s" % (mod, pmid))
                try:
                    mod_to_pmids[mod].append(pmid)
                except KeyError:
                    mod_to_pmids[mod] = [pmid]
        not_found_file.close()

    output_pmids_not_found_by_mod_file = base_path + 'pmids_not_found_by_mod'
    with open(output_pmids_not_found_by_mod_file, "w") as pmids_not_found_by_mod_file:
        for mod in mod_to_pmids:
            count = len(mod_to_pmids[mod])
            pmids = ", ".join(mod_to_pmids[mod])
            logger.info("mod %s has %s pmids not in PubMed %s" % (mod, count, pmids))
            pmids_not_found_by_mod_file.write("mod %s has %s pmids not in PubMed %s\n" % (mod, count, pmids))
        pmids_not_found_by_mod_file.close()


#     for pmid in pmid_to_mod:
#         for mod in pmid_to_mod[pmid]:
#             print("mod %s pmid %s" % (mod, pmid))


if __name__ == "__main__":
    """ call main start function """

    sort_not_found_pmids_by_mod()
