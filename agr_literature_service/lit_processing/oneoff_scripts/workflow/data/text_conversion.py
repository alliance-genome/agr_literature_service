

def get_data(name_to_atp):
    """
    mod can only be 'ALL', the actual mod abbreviation or 'NOT_' + mod abbreviation
    i.e. ALL, WB, NOT_FB are three examples.
    """
    ref_type = "proceed_on_value::reference_type"
    test_data = [
        {
            'mod': "ALL",
            'from': "files uploaded",
            'to': "text conversion needed",
            'condition': 'text_convert_job',
            'actions': []
        },
        {
            'mod': "ALL",
            'from': "text conversion needed",
            'to': "file to text conversion failed",
            'condition': 'on_failed',
            'actions': []
        },
        # then overwrite for WB and ZFIN with conditions
        # 'catalytic activity', 'disease', 'expression', 'physical interaction', 'RNAi'
        {
            'mod': "WB",
            'from': "text conversion needed",
            'to': "file converted to text",
            'condition': 'on_success',
            'actions': [f"{ref_type}::Experimental::{name_to_atp['catalytic activity classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['disease classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['expression classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['physical interaction classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['reference classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['RNAi classification needed']}"]
        },
        {
            'mod': "WB",
            'from': "text conversion in progress",
            'to': "file converted to text",
            'condition': 'on_success',
            'actions': [f"{ref_type}::Experimental::{name_to_atp['catalytic activity classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['disease classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['expression classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['physical interaction classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['reference classification needed']}",
                        f"{ref_type}::Experimental::{name_to_atp['RNAi classification needed']}"]
        },
        {
            'mod': "ZFIN",
            'from': "text conversion needed",
            'to': "file converted to text",
            'condition': 'on_success',
            'actions': []
        },
        {
            'mod': "ZFIN",
            'from': "text conversion in progress",
            'to': "file converted to text",
            'condition': 'on_success',
            'actions': []
        },
        {
            'mod': "FB",
            'from': "text conversion needed",
            'to': "file converted to text",
            'condition': 'on_success',
            'actions': []
        },
        {
            'mod': "FB",
            'from': "text conversion in progress",
            'to': "file converted to text",
            'condition': 'on_success',
            'actions': []
        },
        {
            'mod': "SGD",
            'from': "text conversion needed",
            'to': "file converted to text",
            'condition': 'on_success',
            'actions': []
        },
        {
            'mod': "SGD",
            'from': "text conversion in progress",
            'to': "file converted to text",
            'condition': 'on_success',
            'actions': []
        }
    ]
    return test_data
