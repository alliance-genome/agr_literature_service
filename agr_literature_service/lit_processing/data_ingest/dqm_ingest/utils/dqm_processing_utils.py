import bs4


def compare_authors_or_editors(db_entry, dqm_entry, datatype):   # noqa: C901
    """
    Authors and Editors are in a hash with order of 'order' from db and 'authorRank' from dqm.  They have to be sorted by the order, and for updates the db 'author_id' is used to patch.  Currently we don't want to remove any authors, so we patch None to the values.  We're only getting from dqms the 'name', 'first_name', 'last_name', and 'order', although the ingest schema allows other data.

    Sample db data, unchanged dqm entry, changed dqm entry
    db_entry = { "authors": [ { "date_created": "2021-10-08T17:03:27.036468", "date_updated": "2021-10-10T11:04:39.548530", "author_id": 4582613, "order": 1, "name": "Abdul Kader N", "first_name": None, "middle_names": None, "last_name": None, "first_author": False, "orcid": None, "affiliation": None, "corresponding_author": None }, { "date_created": "2021-10-08T17:03:27.036468", "date_updated": "2021-10-10T11:04:39.548947", "author_id": 4582614, "order": 2, "name": "Brun J", "first_name": None, "middle_names": None, "last_name": None, "first_author": False, "orcid": None, "affiliation": None, "corresponding_author": None } ] }
    dqm_entry_unchanged = { "authors": [ { "name": "Abdul Kader N", "referenceId": "WB:WBPaper00000003", "authorRank": 1 }, { "referenceId": "WB:WBPaper00000003", "name": "Brun J", "authorRank": 2 } ] }
    dqm_entry_changed = { "authors": [ { "name": "Abdul Kader AAN", "referenceId": "WB:WBPaper00000003", "authorRank": 1 }, { "referenceId": "WB:WBPaper00000003", "name": "Brunner J", "firstname": "bob", "authorRank": 2 }, { "referenceId": "WB:WBPaper00000003", "name": "NEW", "firstname": "bob", "authorRank": 3 }, { "referenceId": "WB:WBPaper00000003", "name": "AGAIN", "firstname": "bob", "authorRank": 4 } ] }

    :param db_entry:
    :param dqm_entry:
    :param datatype:
    :return:
    """

    # db_entry_text = json.dumps(db_entry, indent=4)
    # print('db entry ')
    # print(db_entry_text)
    # dqm_entry_text = json.dumps(dqm_entry, indent=4)
    # print('dqm entry ')
    # print(dqm_entry_text)
    db_authors = []
    dqm_authors = []
    if datatype in db_entry:
        if db_entry[datatype] is not None:
            db_authors = db_entry[datatype]

    dqm_key = datatype
    if datatype == 'editors':
        dqm_key = 'editorsOrAuthors'
    if dqm_key in dqm_entry:
        if dqm_entry[dqm_key] is not None:
            dqm_authors = dqm_entry[dqm_key]
    db_has_change = False
    db_ordered = dict()
    for author_dict in db_authors:
        if datatype == 'authors':
            if author_dict['corresponding_author']:
                db_has_change = True
            if author_dict['first_author']:
                db_has_change = True
        if 'order' not in author_dict:
            # print('no order ')
            db_has_change = True
        else:
            order = int(author_dict['order'])
            db_ordered[order] = author_dict
    if db_has_change:
        return False, None, None

    dqm_ordered = dict()
    for author_dict in dqm_authors:
        if 'authorRank' in author_dict:
            order = int(author_dict['authorRank'])
            sanitized_dict = dict()
            sanitized_dict['order'] = order
            if 'name' in author_dict:
                sanitized_dict['name'] = author_dict['name']
            if 'lastName' in author_dict:
                sanitized_dict['last_name'] = author_dict['lastName']
            elif 'lastname' in author_dict:
                sanitized_dict['last_name'] = author_dict['lastname']
            if 'firstName' in author_dict:
                sanitized_dict['first_name'] = author_dict['firstName']
            elif 'firstname' in author_dict:
                sanitized_dict['first_name'] = author_dict['firstname']
            dqm_ordered[order] = sanitized_dict

    to_patch = []
    to_create = []
    author_subfields = ['name', 'first_name', 'last_name']
    for order in sorted(db_ordered.keys()):
        author_changed = False
        patch_dict = {'order': order}
        dqm_dict = dict()
        if order in dqm_ordered:
            dqm_dict = dqm_ordered[order]
            # print("dqm %s %s" % (order, dqm_dict['name']))
        db_dict = db_ordered[order]
        # print("db %s %s" % (order, db_dict['name']))
        for field in author_subfields:
            dqm_value = None
            if field in dqm_dict:
                dqm_value = dqm_dict[field]
            db_value = None
            if field in db_dict:
                db_value = db_dict[field]
            if db_value != dqm_value:
                patch_dict[field] = dqm_value  # must assign None to fields if dqm did not set author at that order number
                # print("field changed %s %s %s" % (field, db_value, dqm_value))
                author_changed = True
        if author_changed:
            if datatype == 'authors':
                to_patch.append({'author_id': db_dict['author_id'], 'patch_dict': patch_dict})
            elif datatype == 'editors':
                to_patch.append({'editor_id': db_dict['editor_id'], 'patch_dict': patch_dict})

    for order in sorted(dqm_ordered.keys()):
        if order not in db_ordered:
            to_create.append(dqm_ordered[order])

    return True, to_patch, to_create


def clean_up_keywords(mod, entry):
    # e.g. 9882485 25544291 24201188 31188077
    if mod == 'ZFIN':
        if 'keywords' in entry:
            if entry['keywords'][0] == '':
                entry['keywords'] = []
            else:
                # zfin has all keywords in the first array element, they cannot fix it
                zfin_value = entry['keywords'][0]
                zfin_value = str(bs4.BeautifulSoup(zfin_value, "html.parser"))
                comma_count = 0
                semicolon_count = 0
                if ", " in zfin_value:
                    comma_count = zfin_value.count(',')
                if "; " in zfin_value:
                    semicolon_count = zfin_value.count(';')
                if (comma_count == 0) and (semicolon_count == 0):
                    entry['keywords'] = [zfin_value]
                elif comma_count >= semicolon_count:
                    entry['keywords'] = zfin_value.split(", ")
                else:
                    entry['keywords'] = zfin_value.split("; ")
    else:
        keywords = []
        for mod_keyword in entry['keywords']:
            mod_keyword = str(bs4.BeautifulSoup(mod_keyword, "html.parser"))
            keywords.append(mod_keyword)
        entry['keywords'] = keywords
