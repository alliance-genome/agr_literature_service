from agr_literature_service.lit_processing.utils.sqlalchemy_utils import create_postgres_session


# This script queries the database for delete operations in the author_version table, that were created after 2024-01-01, connected to a
# reference that has a PMID.  This is just to get a sense of how much things are changing at pubmed.  At the time of the script, there
# were only two times that authors were updated with operation_type 1, when Pinglei replaced special characters on 2023-03-08, and when
# Shuai updated authors to have first_initial on 2023-05-19.  All other times that authors had any changes from pubmed, they were all
# delete with operation_type 2, and created with new author_id with operation_type 0.  In the future, authors will try to update if some
# information remains the same, so any information associated with flags of first_author or corresponding_author will be persistent, as
# well as any author-person connections, which are not yet allowed in the database.  At that point, this script will not work, as this
# ignores anything with an operation_type 1.
#
# This script groups data in three categories.  results_names_order for when names + order changes, and results_names_orcid_order for when
# names + orcid + order changes, by aggregating into an ordered list and joining into a long string.  results_names_orcid for when
# names + orcid changes, by grouping into a set.  In both cases strip leading and trailing spaces, and lowercase, to avoid changes from
# minor changes that will be ignored by future pubmed processing.
#
# In processing data, first get all the information from author_version, then collect all transactions, to find out the date of the
# transactions, assuming any one date will only have one set of changes, in case there were multiple transactions hashing to the same
# day, though it turned out all transactions happen at the same time, but deletes have a slightly different date from continuum, and we
# don't know why, but maybe so that there's a slight change to know which one happens first when there's a concurrent delete and create
# of data, as in this case.
#
# Verbose output with debug_output True will print a lot of information about what is getting queried and what authors and transactions
# exist as the data is processed.  This is useful when looking at one reference at a time to see what happened to it.  When there's a
# change it outputs whether the data is the same for that category, is the original creation of the data, or it has a meaningful change,
# adding to the counter.
#
# Looking at these three categories, we found that in just over five months, there were these many changes
# results_names_order total 331
# results_names_orcid_order total 448
# results_names_orcid total 436
#
# The full output also breaks down for each category, and for each reference, how many changes there were.  Most of these changes were
# meaningless.  An orcid added, then removed, then two more added, then removed.  An affiliation adding a comma.  Names changing from
# full names to initials.  Stuff that was often making things worse, so the recommendation was not to do pubmed updated, but ZFIN gets
# complaints from users if the names are not updated, so Ceri would like to change the pubmed update script to keep the author objects
# when possible, and trigger delete/create when an author doesn't have manually modified data.  The count of reference author changes
# in the time period was deemed too high to require review by a curator.
#
# There was going to be analysis on how often changes happened due to names + order + orcid + affiliation, but it's already too much to
# review, and this would have even higher counts.  An analysis of reordering could still be useful, when names + order change, but the
# set of names doesn't change.
#
# An output of this run is stored at
# https://dev.alliancegenome.org/azurebrd/agr-lit/out/author_analysis/20240521_author_analysis


# Set this value to True to see specifics for each PMID, better if only processing one PMID at a time, as there's a lot of output.
# debug_output = True
debug_output = False

results_names_order = dict()
results_names_orcid_order = dict()
results_names_orcid = dict()

results_names_order['total'] = 0
results_names_orcid_order['total'] = 0
results_names_orcid['total'] = 0


def analyze_author_changes(db_session):
    # has some delete
    # SELECT curie FROM cross_reference WHERE reference_id IN ( SELECT reference_id FROM author_version WHERE operation_type = 2 AND date_created > '2024-01-01' ) AND curie_prefix = 'PMID'
    # 'PMID:38503366'	# only affiliation changes
    # 'PMID:38465639'	# only affiliation changes
    # 'PMID:38493895'	# only affiliation changes
    # 'PMID:38493941'	# author initial changes
    # 'PMID:38508462'	# only affiliation changes

    # has some orcid
    # SELECT curie FROM cross_reference WHERE reference_id IN ( SELECT reference_id FROM author_version WHERE operation_type = 2 AND date_created > '2024-01-01' AND orcid IS NOT NULL) AND curie_prefix = 'PMID'
    # 'PMID:36747868'	# 20230208 1 orcid, 20231221 0 orcid, 20240203 2 orcid, 20240216 0 orcid
    # 'PMID:38400543'	# 20240225 1 orcid, 20240316 4 orcid
    # 'PMID:38182577'	# 20240130 3 orcid, 20240216 same orcid, name change
    # 'PMID:36865105'	# 20230304 0 orcid, 20231014 1 orcid, 20231221 0 orcid, 20240127 1 orcid (same), 20240309 1 orcid (aff)
    # 'PMID:38140980'	# 20231224 1 orcid, 20240113 1 orcid (aff), 20240120 1 orcid (aff)

    # 'PMID:37453426'	# 8 changes.

    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:15533247'")	# author names change
    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:31333191'")
    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:38503366'")	# only affiliation changes
    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:38493941'")	# author initial changes
    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:36747868'")	# 20230208 1 orcid, 20231221 0 orcid, 20240203 2 orcid, 20240216 0 orcid
    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:38400543'")	# 20240225 1 orcid, 20240316 4 orcid
    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:38182577'")	# 20240130 3 orcid, 20240216 same orcid, name change
    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:36865105'")	# 20230304 0 orcid, 20231014 1 orcid, 20231221 0 orcid, 20240127 1 orcid (same), 20240309 1 orcid (aff)
    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:38140980'")	# 20231224 1 orcid, 20240113 1 orcid (aff), 20240120 1 orcid (aff)

    # uncomment this block to see output for a specific pmid
    # rs = db_session.execute("SELECT curie, reference_id FROM cross_reference WHERE curie = 'PMID:37453426'")
    # rows = rs.fetchall()
    # for x in rows:
    #     reference_id = x[1]
    #     process_reference_id(db_session, x[1], x[0])
    #     print(x)

    # uncomment this block to process from a list of pmids
    # pmids = [ 'PMID:38503366', 'PMID:38465639', 'PMID:38493895', 'PMID:38493941', 'PMID:38508462', 'PMID:36747868', 'PMID:38400543', 'PMID:38182577', 'PMID:36865105', 'PMID:38140980' ]
    # for pmid in pmids:

    # uncomment this block to process from a db query to get pmids
    rs = db_session.execute("SELECT curie FROM cross_reference WHERE reference_id IN ( SELECT reference_id FROM author_version WHERE operation_type = 2 AND date_created > '2024-01-01' ) AND curie_prefix = 'PMID'")
    pmids = rs.fetchall()
    for something in pmids:
        pmid = something[0]

    # do not comment out this block, it works with either set of blocks above
        rs = db_session.execute(f"SELECT curie, reference_id FROM cross_reference WHERE curie = '{pmid}'")
        rows = rs.fetchall()
        for x in rows:
            process_reference_id(db_session, x[1], x[0])
            print(x)

    print()
    for key in results_names_order:
        print(f"results_names_order {key} {results_names_order[key]}")
    print()
    for key in results_names_orcid_order:
        print(f"results_names_orcid_order {key} {results_names_orcid_order[key]}")
    print()
    for key in results_names_orcid:
        print(f"results_names_orcid {key} {results_names_orcid[key]}")


def filter_string(str):
    if str is not None:
        str = str.strip().lower()
    else:
        str = ''
    return str


def process_reference_id(db_session, reference_id, agrkb):      # noqa: C901 pragma: no cover
    if debug_output:
        print(reference_id)

    results_names_order[agrkb] = 0
    results_names_orcid_order[agrkb] = 0
    results_names_orcid[agrkb] = 0

    aid_tid = {}
    tids_author_set = set()
    rs = db_session.execute(f"SELECT reference_id, author_id, transaction_id, date_updated, name, first_name, last_name, first_initial, orcid, \"order\", operation_type, affiliations FROM author_version WHERE reference_id = '{reference_id}'")
    rows = rs.fetchall()
    if debug_output:
        print(f"data for reference {reference_id} ref_id, aid, tid, date, name, fn, ln, fi, orcid, order, optype, affiliations")
    for x in rows:
        if x[10] == 1:
            continue
        if x[1] not in aid_tid:
            aid_tid[x[1]] = {}
        if x[2] not in aid_tid:
            aid_tid[x[1]][x[2]] = {}
        aid_tid[x[1]][x[2]]['name'] = filter_string(x[4])
        aid_tid[x[1]][x[2]]['first_name'] = filter_string(x[5])
        aid_tid[x[1]][x[2]]['last_name'] = filter_string(x[6])
        aid_tid[x[1]][x[2]]['first_initial'] = filter_string(x[7])
        aid_tid[x[1]][x[2]]['orcid'] = x[8]
        aid_tid[x[1]][x[2]]['order'] = x[9]
        aid_tid[x[1]][x[2]]['operation_type'] = x[10]
        aid_tid[x[1]][x[2]]['affiliations'] = x[11]
        tids_author_set.add(x[2])
        if debug_output:
            print(x)
    if debug_output:
        print()
        print("tids_authors_set")
        for tid in sorted(tids_author_set):
            print(tid)
        print()

    tid_date = {}
    tid_user = {}
    users_set = set()
    s = [str(i) for i in tids_author_set]
    tids = ", ".join(s)
    rs = db_session.execute(f"SELECT id, issued_at, user_id FROM transaction WHERE id IN ({tids})")
    rows = rs.fetchall()
    if debug_output:
        print("tids_transaction info: tid, issued_at, user_id")
    for x in rows:
        users_set.add(x[2])
        if debug_output:
            print(f"transaction {x}")
        date = x[1].strftime("%Y") + x[1].strftime("%m") + x[1].strftime("%d")
        tid_date[x[0]] = date
        tid_user[x[0]] = x[2]
        if debug_output:
            print(date)
    if debug_output:
        print()

    user_to_email = {}
    if None in users_set:
        users_set.remove(None)
    s = [str(i) for i in users_set]
    userids = "','".join(s)
    rs = db_session.execute(f"SELECT id, email FROM users WHERE id IN ('{userids}')")
    rows = rs.fetchall()
    if debug_output:
        print("users info: userid, email")
    for x in rows:
        if debug_output:
            print(f"users {x}")
        user_to_email[x[0]] = x[1]
    user_to_email[None] = 'None'

    date_tid_optype_order_aid = {}    # aggregate by names + order
    if debug_output:
        print("aid_tid: date, transaction_id, operation_type, author_id, user/email, name/first/last/first_initial")
    for aid in aid_tid:
        if debug_output:
            print()
        for tid in aid_tid[aid]:
            date = tid_date[tid]
            user = tid_user[tid]
            email = user_to_email[user]
            optype = aid_tid[aid][tid]['operation_type']
            order = aid_tid[aid][tid]['order']
            if date not in date_tid_optype_order_aid:
                date_tid_optype_order_aid[date] = {}
            if tid not in date_tid_optype_order_aid[date]:
                date_tid_optype_order_aid[date][tid] = {}
            if optype not in date_tid_optype_order_aid[date][tid]:
                date_tid_optype_order_aid[date][tid][optype] = {}
            if order not in date_tid_optype_order_aid[date][tid][optype]:
                date_tid_optype_order_aid[date][tid][optype][order] = aid
            if debug_output:
                print(f"{date}\t{tid}\t{aid_tid[aid][tid]['operation_type']}\t{aid}\t{user}/{email}\t{aid_tid[aid][tid]['name']}/{aid_tid[aid][tid]['first_name']}/{aid_tid[aid][tid]['last_name']}/{aid_tid[aid][tid]['first_initial']}")
    if debug_output:
        print()

    for date in date_tid_optype_order_aid:
        if debug_output:
            print(f"date {date}")
        for tid in date_tid_optype_order_aid[date]:
            if debug_output:
                print(f"transaction_id {tid}")
            delete_authors_order = ''
            create_authors_order = ''
            delete_authors_orcid_order = ''
            create_authors_orcid_order = ''
            delete_authors_orcid_set = set()
            create_authors_orcid_set = set()
            for optype in date_tid_optype_order_aid[date][tid]:
                highest = 0
                for order in date_tid_optype_order_aid[date][tid][optype]:
                    if order > highest:
                        highest = order
                authors_order = [None] * highest
                authors_orcid_order = [None] * highest
                for order in date_tid_optype_order_aid[date][tid][optype]:
                    if debug_output:
                        print(f"{optype} {order} {date_tid_optype_order_aid[date][tid][optype][order]}")
                    aid = date_tid_optype_order_aid[date][tid][optype][order]
                    author_order_string = f"{aid_tid[aid][tid]['name']}/{aid_tid[aid][tid]['first_name']}/{aid_tid[aid][tid]['last_name']}/{aid_tid[aid][tid]['first_initial']}"
                    author_orcid_order_string = f"{aid_tid[aid][tid]['name']}/{aid_tid[aid][tid]['first_name']}/{aid_tid[aid][tid]['last_name']}/{aid_tid[aid][tid]['first_initial']}/{aid_tid[aid][tid]['orcid']}"
                    if optype == 0:
                        create_authors_orcid_set.add(author_orcid_order_string)
                    elif optype == 2:
                        delete_authors_orcid_set.add(author_orcid_order_string)
                    if authors_order[order - 1] is not None:
                        if debug_output:
                            print("ERROR, two authors_order assigned same order by same transaction and operation type")
                    authors_order[order - 1] = author_order_string
                    if authors_orcid_order[order - 1] is not None:
                        if debug_output:
                            print("ERROR, two authors_orcid_order assigned same order by same transaction and operation type")
                    authors_orcid_order[order - 1] = author_orcid_order_string
                if debug_output:
                    print(authors_order)
                author_order_key = "|".join(authors_order)
                if debug_output:
                    print(author_order_key)
                if debug_output:
                    print(authors_orcid_order)
                author_orcid_order_key = "|".join(authors_orcid_order)
                if debug_output:
                    print(author_orcid_order_key)
                if optype == 0:
                    if create_authors_order != '':
                        if debug_output:
                            print("ERROR, two sets of authors_order created by same transaction")
                    create_authors_order = author_order_key
                    if debug_output:
                        print(f"AO {date} {tid} CREATE {create_authors_order}")
                    if create_authors_orcid_order != '':
                        if debug_output:
                            print("ERROR, two sets of authors_orcid_order created by same transaction")
                    create_authors_orcid_order = author_orcid_order_key
                    if debug_output:
                        print(f"AOO {date} {tid} CREATE {create_authors_orcid_order}")
                elif optype == 2:
                    if delete_authors_order != '':
                        if debug_output:
                            print("ERROR, two sets of authors_order deleted by same transaction")
                    delete_authors_order = author_order_key
                    if debug_output:
                        print(f"AO {date} {tid} DELETE {delete_authors_order}")
                    if delete_authors_orcid_order != '':
                        if debug_output:
                            print("ERROR, two sets of authors_orcid_order deleted by same transaction")
                    delete_authors_orcid_order = author_orcid_order_key
                    if debug_output:
                        print(f"AOO {date} {tid} DELETE {delete_authors_orcid_order}")
            if delete_authors_order != create_authors_order:
                if delete_authors_order == '':
                    if debug_output:
                        print(f"AO {date} {tid} ORIGINAL CREATE {create_authors_order}")
                else:
                    results_names_order[agrkb] = results_names_order[agrkb] + 1
                    results_names_order['total'] = results_names_order['total'] + 1
                    if debug_output:
                        print(f"AO {date} {tid} CHANGE\nDELETE {delete_authors_order}\nCREATE {create_authors_order}")
            else:
                if debug_output:
                    print(f"AO {date} {tid} SAME {create_authors_order}")
            if delete_authors_orcid_order != create_authors_orcid_order:
                if delete_authors_orcid_order == '':
                    if debug_output:
                        print(f"AOO {date} {tid} ORIGINAL CREATE {create_authors_orcid_order}")
                else:
                    results_names_orcid_order[agrkb] = results_names_orcid_order[agrkb] + 1
                    results_names_orcid_order['total'] = results_names_orcid_order['total'] + 1
                    if debug_output:
                        print(f"AOO {date} {tid} CHANGE\nDELETE {delete_authors_orcid_order}\nCREATE {create_authors_orcid_order}")
            else:
                if debug_output:
                    print(f"AOO {date} {tid} SAME {create_authors_orcid_order}")
            if (len(delete_authors_orcid_set.difference(create_authors_orcid_set)) > 0):
                if (len(delete_authors_orcid_set) < 1):
                    if debug_output:
                        print(f"AOS {date} {tid} ORIGINAL CREATE {create_authors_orcid_set}")
                else:
                    results_names_orcid[agrkb] = results_names_orcid[agrkb] + 1
                    results_names_orcid['total'] = results_names_orcid['total'] + 1
                    if debug_output:
                        print(f"AOS {date} {tid} CHANGE\nDELETE {delete_authors_orcid_set}\nCREATE {create_authors_orcid_set}")
            else:
                if debug_output:
                    print(f"AOS {date} {tid} SAME {create_authors_orcid_set}")
            if debug_output:
                print()
    if debug_output:
        print()


def process_reference_id_with_publication_status(db_session, reference_id):
    # use this to look at how things relate to pubmed pubstatus, but for sample pmid 15533247 / reference_id 620744, there were a lot of changes while pubstatus stayed the same, epublish across six transactions, so Ceri said to just drop that approach
    # run like:
    # process_reference_id_with_publication_status(db_session, 620744)
    #
    # but string for publication status stays the same across 5 transactions with little overlap with author transactions
    # tids_pubstatus_set				{1421383, 2858397, 662929, 941909, 1071069, 1179390}
    # tids_author_set                                   {2184425, 662929, 2874318, 3272593}
    # tids_pubstatus_set.difference(tids_author_set)    {1421383, 1179390, 941909, 1071069, 2858397}

    print(reference_id)

    tids_author_set = set()
    rs = db_session.execute(f"SELECT reference_id, author_id, transaction_id, date_updated FROM author_version WHERE reference_id = '{reference_id}'")
    rows = rs.fetchall()
    for x in rows:
        tids_author_set.add(x[2])
        print(x)
    for tid in sorted(tids_author_set):
        print(tid)

    tids_pubstatus_set = set()
    rs = db_session.execute(f"SELECT reference_id, pubmed_publication_status, transaction_id, date_updated FROM reference_version WHERE reference_id = '{reference_id}'")
    rows = rs.fetchall()
    for x in rows:
        tids_pubstatus_set.add(x[2])
        print(x)

    print(tids_pubstatus_set)
    print(tids_author_set)
    print(tids_pubstatus_set.difference(tids_author_set))
    print(len(tids_pubstatus_set.difference(tids_author_set)))


if __name__ == "__main__":

    db_session = create_postgres_session(False)
    analyze_author_changes(db_session)
