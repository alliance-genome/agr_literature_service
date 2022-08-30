from agr_literature_service.lit_processing.helper_sqlalchemy import create_postgres_engine
import logging

logging.basicConfig(format='%(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)


def cleanup_user_email_to_okta_uid():

    engine = create_postgres_engine(False)
    db_connection = engine.connect()

    # in rdsprod
    #
    # SELECT * FROM transaction WHERE user_id != '0oa1cs2ineBqEFiD85d7'
    # "2021-11-09 19:00:45.400051"	911163		"juancarlos@wormbase.org"
    # "2021-11-09 19:01:10.881227"	911164		"juancarlos@wormbase.org"
    # "2022-03-26 18:16:19.629189"	911167		"juancarlos@wormbase.org"
    # "2022-03-26 18:23:47.886362"	911169		"juancarlos@wormbase.org"
    # "2022-03-26 18:24:10.47261"	911170		"juancarlos@wormbase.org"
    # "2022-04-05 20:54:30.967617"	911172		"juancarlos@wormbase.org"
    # "2022-04-05 20:54:48.967382"	911173		"juancarlos@wormbase.org"
    # "2022-04-05 20:54:57.658239"	911174		"juancarlos@wormbase.org"
    # "2022-04-05 20:55:05.132912"	911175		"juancarlos@wormbase.org"
    # "2022-04-05 20:55:11.720868"	911176		"juancarlos@wormbase.org"
    # "2022-04-05 20:55:16.438315"	911177		"juancarlos@wormbase.org"
    # "2022-04-05 20:55:22.249015"	911178		"juancarlos@wormbase.org"
    # "2022-04-05 20:55:27.73944"	911179		"juancarlos@wormbase.org"
    # "2022-04-29 21:09:31.942943"	911180		"zfinfish@gmail.com"
    # "2022-05-09 21:49:34.685349"	911181		"zfinfish@gmail.com"
    # "2022-05-09 21:49:34.909366"	911182		"zfinfish@gmail.com"
    # "2022-05-09 23:59:46.43515"	911183		"zfinfish@gmail.com"
    # "2022-05-09 23:59:46.530329"	911184		"zfinfish@gmail.com"
    # "2022-07-11 16:25:01.475242"	946252		"zfinfish@gmail.com"
    # "2022-07-28 19:33:21.85102"	1021390		"zfinfish@gmail.com"
    # "2022-07-28 19:33:27.423134"	1021391		"zfinfish@gmail.com"
    # "2022-08-12 18:49:08.055158"	1025540		"zfinfish@gmail.com"
    # "2022-08-12 18:50:03.616006"	1025541		"zfinfish@gmail.com"

    # existing transaction_id to _version tables
    # {911167: ['author', 'cross_reference', 'reference'], 911169: ['author', 'cross_reference', 'reference'], 911170: ['author', 'cross_reference', 'reference'], 911181: ['cross_reference'], 911182: ['cross_reference'], 911183: ['cross_reference'], 1021390: ['cross_reference'], 1021391: ['cross_reference'], 911172: ['mod'], 911173: ['mod'], 911174: ['mod'], 911175: ['mod'], 911176: ['mod'], 911177: ['mod'], 911178: ['mod'], 911179: ['mod'], 911180: ['mod_corpus_association'], 911184: ['mod_corpus_association'], 946252: ['mod_corpus_association'], 1025540: ['mod_corpus_association'], 1025541: ['mod_corpus_association'], 911163: ['reference'], 911164: ['reference']}

    # transaction id to email and okta uid
    #
    # author
    # 911167 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911167 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911167 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911169 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911169 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911169 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911170 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911170 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911170 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911170 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    #
    # cross_reference
    # 911167 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911169 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911170 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911181 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    # 911182 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    # 911183 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    # 1021390 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    # 1021391 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    #
    # editor
    #
    # mod
    # 911172 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911173 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911174 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911175 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911176 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911177 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911178 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911179 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    #
    # mod_corpus_association
    # 911180 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    # 911184 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    # 946252 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    # 1025540 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    # 1025541 zfinfish@gmail.com 00u1mhf3mf28xjpPt5d7
    #
    # mod_reference_type
    #
    # mod_taxon
    #
    # obsolete_reference_curie
    #
    # reference
    # 911163 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911164 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911167 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911169 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    # 911170 juancarlos@wormbase.org 00u1ctzvjgMpk87Qm5d7
    #
    # resource
    #
    # topic_entity_tag
    #
    # topic_entity_tag_prop
    #
    # workflow_tag
    #
    # users

    email_to_uid = {'zfinfish@gmail.com': '00u1mhf3mf28xjpPt5d7', 'juancarlos@wormbase.org': '00u1ctzvjgMpk87Qm5d7'}
    # email_to_uid['blah'] = '0oa1cs2ineBqEFiD85d7'
    # email_to_uid['bleh'] = 'chris@wormbase.org'
    tid_to_email = {}
    tids_list = []
    tid_to_table = {}

    # create correct user entries with okta uid in id column
    for email in email_to_uid:
        uid = email_to_uid[email]
        rs = db_connection.execute(f"SELECT * FROM users WHERE id = '{uid}'")
        rows = rs.fetchall()
        # if len(rows) > 0:
        #     print(f"uid {uid} already in")
        if len(rows) == 0:
            rs = db_connection.execute(f"INSERT INTO users VALUES ('{uid}', '{email}')")
            print(f"creating uid {uid} to email {email} in users table")

    rs = db_connection.execute("SELECT * FROM transaction WHERE user_id != '0oa1cs2ineBqEFiD85d7' AND user_id != '00u1mhf3mf28xjpPt5d7' AND user_id != '00u1ctzvjgMpk87Qm5d7' ")
    rows = rs.fetchall()
    for x in rows:
        # when = x[0]
        tid = x[1]
        # junk = x[2]
        email = x[3]
        # print(x)
        tids_list.append(str(tid))
        tid_to_email[tid] = email

    tids = ", ".join(tids_list)
    print(f"{tids}\n")

    # tables that exist now and might have been modified with a transaction
    table_names = ['author', 'cross_reference', 'editor', 'mod', 'mod_corpus_association', 'mod_reference_type', 'mod_taxon', 'obsolete_reference_curie', 'reference', 'resource', 'topic_entity_tag', 'topic_entity_tag_prop', 'workflow_tag', 'users']

    if tids:
        for table_name in table_names:
            print(f"\ntids {table_name}")
            rs = db_connection.execute(f"SELECT transaction_id FROM {table_name}_version WHERE transaction_id IN ({tids})")
            rows = rs.fetchall()
            for x in rows:
                this_tid = x[0]
                email = tid_to_email[this_tid]
                uid = email_to_uid[email]
                print(f"{this_tid} {email} {uid}")
                if this_tid not in tid_to_table:
                    tid_to_table[this_tid] = []
                if table_name not in tid_to_table[this_tid]:
                    tid_to_table[this_tid].append(table_name)

    print('tid_to_table')
    print(tid_to_table)

    print('not in any table')
    for tid in tids_list:
        if int(tid) not in tid_to_table:
            print(f"tid {tid} from {tid_to_email[tid]} not in any table")

    # all of these have null in created_by and updated_by, nothing to fix.  If any were not null, would have to remap to correct user
    # tables_with_audited = ['author', 'mod', 'mod_corpus_association', 'reference']
    # for table_name in tables_with_audited:
    #     print(f"\naudited {table_name}")
    #     # rs = db_connection.execute(f"SELECT created_by FROM {table_name}_version WHERE created_by ~ '@'")
    #     rs = db_connection.execute(f"SELECT created_by FROM {table_name}_version WHERE created_by IS NOT NULL")
    #     rows = rs.fetchall()
    #     for x in rows:
    #         created_by = x[0]
    #         print(f"created_by {created_by} {table_name}")
    #     # rs = db_connection.execute(f"SELECT updated_by FROM {table_name}_version WHERE updated_by ~ '@'")
    #     rs = db_connection.execute(f"SELECT updated_by FROM {table_name}_version WHERE updated_by IS NOT NULL")
    #     rows = rs.fetchall()
    #     for x in rows:
    #         updated_by = x[0]
    #         print(f"updated_by {updated_by} {table_name}")

    # users have been created, reassign transactions to correct users
    for email in email_to_uid:
        uid = email_to_uid[email]
        print(f"UPDATE transaction SET user_id = '{uid}' WHERE user_id = '{email}'")
        rs = db_connection.execute(f"UPDATE transaction SET user_id = '{uid}' WHERE user_id = '{email}'")

    # this will happen in script to populate audited columns, not here.
    # need to update Audited in author, mod, mod_corpus_association, reference
    # no need to update Audited in cross_reference

    # for table_name in table_names:
    #     print(f"\n{table_name}")
    #     rs = db_connection.execute(f"SELECT * FROM {table_name}_version WHERE transaction_id IN ({tids})")
    #     rows = rs.fetchall()
    #     for x in rows:
    #         print(x)

    # clean up users that aren't correct
    rs = db_connection.execute("DELETE FROM users WHERE id = 'chris@wormbase.org'")
    rs = db_connection.execute("DELETE FROM users WHERE id = 'juancarlos@wormbase.org'")
    rs = db_connection.execute("DELETE FROM users WHERE id = 'mfisher103@gmail.com'")
    rs = db_connection.execute("DELETE FROM users WHERE id = 'valerio.arnaboldi@wormbase.org'")
    rs = db_connection.execute("DELETE FROM users WHERE id = 'zfinfish@gmail.com'")


if __name__ == "__main__":

    cleanup_user_email_to_okta_uid()
