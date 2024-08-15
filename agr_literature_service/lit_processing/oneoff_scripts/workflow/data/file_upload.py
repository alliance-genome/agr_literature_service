def get_data(name_to_atp):
    """
    mod can only be 'ALL', the actual mod abbreviation or 'NOT_' + mod abbreviation
    i.e. ALL, WB, NOT_FB are three examples.
    """
    test_data = [
        {
            'mod': "ALL",
            'from': "file needed",
            'to': "file upload in progress",
        },
        # Do not like this but it looks like some processes go from needed to uploaded so add transitions for this.
        {
            'mod': "NOT_WB",
            'from': "file needed",
            'to': "files uploaded",
            'condition': 'on_success, text_convert_job',
            'actions': [f"proceed_on_value::category::research_article::{name_to_atp['text conversion needed']}"]
        },
        {
            'mod': "WB",
            'from': "file needed",
            'to': "files uploaded",
            'condition': 'on_success, text_convert_job',
            'actions': [f"proceed_on_value::reference_type::experimental::{name_to_atp['text conversion needed']}"]
        },
        {
            'mod': "NOT_WB",
            'from': "file upload in progress",
            'to': "files uploaded",
            'condition': 'on_success',
            'actions': [f"proceed_on_value::category::research_article::{name_to_atp['text conversion needed']}"]
        },
        {
            'mod': "WB",
            'from': "file upload in progress",
            'to': "files uploaded",
            'condition': 'on_success',
            'actions': [f"proceed_on_value::reference_type::experimental::{name_to_atp['text conversion needed']}"]
        },
        {
            'mod': "ALL",
            'from': "files uploaded",
            'to': "text conversion needed",
            'condition': 'text_convert_job',
            'actions': [],
            'transition_type': 'action'
        },
        {
            'mod': "ALL",
            'from': "file upload in progress",
            'to': "file unavailable",
            'condition': 'on_failed'
        }
    ]
    return test_data
