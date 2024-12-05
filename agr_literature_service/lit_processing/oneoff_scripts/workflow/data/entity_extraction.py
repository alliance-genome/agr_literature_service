"""

reference classification complete (ATP:0000169)

entity extraction (ATP:0000172)

    entity extraction complete (ATP:0000174)
        allele extraction complete (ATP:0000215)
        antibody extraction complete (ATP:0000196)
        gene extraction complete (ATP:0000214)
        species extraction complete (ATP:0000203)
        strain extraction complete (ATP:0000250)
        transgenic allele extraction complete (ATP:0000251)

    entity extraction failed (ATP:0000187)
        allele extraction failed (ATP:0000217)
        antibody extraction failed (ATP:0000188)
        gene extraction failed (ATP:0000216)
        species extraction failed (ATP:0000204)
        strain extraction failed (ATP:0000270)
        transgenic allele extraction failed (ATP:0000267)

    entity extraction in progress (ATP:0000190)
        allele extraction in progress (ATP:0000219)
        antibody extraction in progress (ATP:0000195)
        gene extraction in progress (ATP:0000218)
        species extraction in progress (ATP:0000205)
        strain extraction in progress (ATP:0000271)
        transgenic allele extraction in progress (ATP:0000268)

    entity extraction needed (ATP:0000173)
        allele extraction needed (ATP:0000221)
        antibody extraction needed (ATP:0000175)
        gene extraction needed (ATP:0000220)
        species extraction needed (ATP:0000206)
        strain extraction needed (ATP:0000272)
        transgenic allele extraction needed (ATP:0000269)

TODO:

    Adding following actions to the transition rows with transition_to = 'ATP:000016'
    (reference classification complete) when everything is ready

    actions = ARRAY[
      'proceed_on_value::reference_type::paper::ATP:0000173',  # entity extraction needed
      'proceed_on_value::reference_type::paper::ATP:0000206',  # species extraction needed
      'proceed_on_value::reference_type::paper::ATP:0000221',  # allele extraction needed
      'proceed_on_value::reference_type::paper::ATP:0000220',  # gene extraction needed
      'proceed_on_value::reference_type::paper::ATP:0000269',  # transgenic allele extraction needed
      'proceed_on_value::reference_type::paper::ATP:0000175',  # antibody extraction needed
      'proceed_on_value::reference_type::paper::ATP:0000272'   # strain extraction needed
    ]

"""


def get_data(name_to_atp):
    """
    mod can only be 'ALL', the actual mod abbreviation or 'NOT_' + mod abbreviation
    i.e. ALL, WB, NOT_FB are three examples.
    """
    transition_data = []

    for entry in ('entity', 'allele', 'antibody', 'gene', 'species', 'strain', 'transgenic allele'):
        item = {
            'mod': 'WB',
            'from': 'reference classification complete',
            'to': f'{entry} extraction needed',
            'condition': f'{entry}_extraction_job',
            'actions': [],
            'transition_type': 'action'
        }
        transition_data.append(item)
        item = {
            'mod': 'WB',
            'from': f'{entry} extraction needed',
            'to': f'{entry} extraction in progress',
            'condition': 'on_start'
        }
        transition_data.append(item)
        item = {
            'mod': 'WB',
            'from': f'{entry} extraction in progress',
            'to': f'{entry} extraction failed',
            'condition': 'on_failed'
        }
        transition_data.append(item)
        item = {
            'mod': 'WB',
            'from': f'{entry} extraction in progress',
            'to': f'{entry} extraction complete',
            'condition': 'on_success'
        }
        transition_data.append(item)
    # we want to remove some too if they exist:
    for entry in ('entity', 'allele', 'antibody', 'gene',
                  'species', 'strain', 'transgenic allele'):
        for result in ("needed", "complete", "in progress", 'failed'):
            item = {
                'mod': 'NOT_WB',
                'from': f'{entry} extraction {result}',
                'to': 'ALL',
                'delete': True}
            transition_data.append(item)
    return transition_data
