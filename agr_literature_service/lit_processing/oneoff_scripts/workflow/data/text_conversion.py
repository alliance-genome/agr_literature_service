

def get_data(name_to_atp):
    """
    mod can only be 'ALL', the actual mod abbreviation or 'NOT_' + mod abbreviation
    i.e. ALL, WB, NOT_FB are three examples.
    """
    test_data = [
        {
            'mod': "ALL",
            'from': "file conversion needed",
            'to': "file converted to text",
            'condition': 'on_success',
            'actions': []
        },
        {
            'mod': "ALL",
            'from': "text conversion needed",
            'to': "file to text conversion failed",
            'condition': 'on_failed',
            'actions': []
        }
    ]
    return test_data
