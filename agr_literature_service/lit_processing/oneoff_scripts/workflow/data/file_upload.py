def get_data(name_to_atp):
    """
    mod can only be 'ALL', the actual mod abbreviation or 'NOT_' + mod abbreviation
    i.e. ALL, WB, NOT_FB are three examples.
    """
    test_data = [
        {'mod': "ALL",
         'from': "file needed",
         'to': "file upload in progress",
         },
        {'mod': "NOT_WB",
         'from': "file upload in progress",
         'to': "files uploaded",
         'condition': 'on_success',
         'actions': [f"proceed_on_value::category::thesis::{name_to_atp['text conversion needed']}"]
         },
        {'mod': "WB",
         'from': "file upload in progress",
         'to': "files uploaded",
         'condition': 'on_success',
         'actions': [f"proceed_on_value::reference_type::experimental::{name_to_atp['text conversion needed']}"]
         },
        {'mod': "ALL",
         'from': "file upload in progress",
         'to': "file unavailable",
         'condition': 'on_failed'
         }
    ]
    return test_data