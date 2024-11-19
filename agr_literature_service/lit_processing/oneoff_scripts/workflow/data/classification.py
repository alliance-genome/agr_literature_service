"""

reference classification (ATP:0000165)

    reference classification complete (ATP:0000169)
        catalytic activity classification complete (ATP:0000168)
        disease classification complete (ATP:0000167)
        expression classification complete (ATP:0000170)
        interaction classification complete (ATP:0000171)

    reference classification failed (ATP:0000189)
        catalytic activity classification failed (ATP:0000191)
        disease classification failed (ATP:0000192)
        expression classification failed (ATP:0000193)
        interaction classification failed (ATP:0000194)

    reference classification in progress (ATP:0000178)
        catalytic activity classification in progress (ATP:0000184)
        disease classification in progress (ATP:0000186)
        expression classification in progress (ATP:0000183)
        interaction classification in progress (ATP:0000185)

    reference classification needed (ATP:0000166)
        catalytic activity classification needed (ATP:0000180)
        disease classification needed (ATP:0000179)
        expression classification needed (ATP:0000181)
        interaction classification needed (ATP:0000182)
"""



def get_data(name_to_atp):
    """
    mod can only be 'ALL', the actual mod abbreviation or 'NOT_' + mod abbreviation
    i.e. ALL, WB, NOT_FB are three examples.
    """
    test_data = []
    # for each XXX activity add transitions needed for job control
    for entry in ('catalytic activity', 'disease', 'expression', 'interaction'):
        item = {
            'mod': 'ALL',
            'from': 'reference classification needed',
            'to': f'{entry} classification needed',
            'condition': f'{entry}_classification_job'}
        test_data.append(item)
        item = {
            'mod': 'ALL',
            'from': f'{entry} classification needed',
            'to': f'{entry} classification in progress',
            'condition': 'on_start',
            'actions': 'sub_task_in_progress:reference classification'}
        test_data.append(item)
        item = {
            'mod': 'ALL',
            'from': f'{entry} classification in progress',
            'to': f'{entry} classification failed',
            'condition': 'on_failed',
            'actions': 'sub_task_failed:reference classification'}
        test_data.append(item)
        item = {
            'mod': 'ALL',
            'from': f'{entry} classification in progress',
            'to': f'{entry} classification complete',
            'condition': 'on_success',
            'actions': 'sub_task_complete:reference classification'}
        test_data.append(item)
    return test_data
