import glob
import datetime

# test using a list of pmids and removing from them.  it's much slower than using a set.
# this is not really useful anymore, but keeping it for later.

pmids_wanted = []

file_list = 'inputs/alliance_pmids'
print("Processing file input from %s" % (file_list))
with open(file_list, 'r') as fp:
    pmid = fp.readline()
    while pmid:
#         print("Read %s END" % (pmid.rstrip()))
        pmids_wanted.append(pmid.rstrip())
        pmid = fp.readline()

pmids_wanted_set = set(pmids_wanted)


# storage_path = 'pubmed_json/'
# storage_path = '/home2/postgres/work/pgpopulation/pap_papers/20210305_pubmed_time_changes/pubmed_json/'
# storage_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/pubmed_json/'
storage_path = '/home/core/git/azurebrd/agr_literature_service_demo/src/xml_processing/pubmed_json/'

start_time = datetime.datetime.now().timestamp()

full_path_pmid_json = glob.glob(storage_path + "*.json")
count = 0
slice_size = 10000
for elem in full_path_pmid_json:
#     print("ELEM %s END" % (elem))
    elem = elem.replace(storage_path, '')
    elem = elem.replace('.json', '')
#     print("Already had %s END" % (elem))
    if elem in pmids_wanted_set:
        count += 1
        # print("Remove %s from wanted set, count %s" % (elem, count))
        if count % slice_size == 0:
            ct = datetime.datetime.now()
            print("%s MOD %s Remove %s from wanted set, count %s" % (ct, slice_size, elem, count))
        pmids_wanted_set.remove(elem)

end_time = datetime.datetime.now().timestamp()
diff_time = end_time - start_time
print("took %s time" % (diff_time))
