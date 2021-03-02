
import json
import itertools

from os import path
import logging
import logging.config

# pipenv run python generate_dqm_json_test_set.py
# Take large dqm json data and generate a smaller subset to test with, with data from beginning, middle, and end of data array
# This takes about 90 seconds to run


log_file_path = path.join(path.dirname(path.abspath(__file__)), '../logging.conf')
logging.config.fileConfig(log_file_path)
logger = logging.getLogger('literature logger')

base_path = '/home/azurebrd/git/agr_literature_service_demo/src/xml_processing/'


def generate_dqm_json_test_set():
    sample_amount = 10
#     mods = ['SGD', 'RGD', 'FB', 'WB', 'MGI', 'ZFIN']
    mods = ['MGI']
    for mod in mods:
        logger.info("generating sample set for %s", mod)
        input_filename = base_path + 'dqm_data/REFERENCE_' + mod + '.json'
        print(input_filename)
        f = open(input_filename)
        dqm_data = json.load(f)

# generate half-as-big set
#        sample_amount = int(len(dqm_data['data']) / 2)
#        dqm_data['data'] = dqm_data['data'][:sample_amount]	# half one
#        dqm_data['data'] = dqm_data['data'][-sample_amount:]	# half two

        reference_amount = len(dqm_data['data'])
        if reference_amount > 3 * sample_amount:
            sample1 = dqm_data['data'][:sample_amount]
            start = int(reference_amount / 2) - 1
            sample2 = dqm_data['data'][start:start + sample_amount]
            sample3 = dqm_data['data'][-sample_amount:]
            dqm_data['data'] = list(itertools.chain(sample1, sample2, sample3))
        output_json_file = base_path + 'dqm_sample/REFERENCE_' + mod + '.json'
        with open(output_json_file, "w") as json_file:
            json_data = json.dumps(dqm_data, indent=4, sort_keys=True)
            json_file.write(json_data)
            json_file.close()


if __name__ == "__main__":
    """ call main start function """
    logger.info("starting generate_dqm_json_test_set.py")
    generate_dqm_json_test_set()
    logger.info("ending generate_dqm_json_test_set.py")
