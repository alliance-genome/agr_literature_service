import warnings
from os import environ
from agr_literature_service.lit_processing.utils.tmp_files_utils import init_tmp_dir

init_tmp_dir()

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

base_path = environ.get('XML_PATH')


def split_identifier(identifier, ignore_error=False):
    """

    Split Identifier.

    Does not throw exception anymore. Check return, if None returned, there was an error

    :param identifier:
    :param ignore_error:
    :return:
    """

    prefix = None
    identifier_processed = None
    separator = None

    if ':' in identifier:
        prefix, identifier_processed = identifier.split(':', 1)  # Split on the first occurrence
        separator = ':'
    elif '-' in identifier:
        prefix, identifier_processed = identifier.split('-', 1)  # Split on the first occurrence
        separator = '-'
    else:
        if not ignore_error:
            # not sure how to logger from imported function without breaking logger in main function
            # logger.critical('Identifier does not contain \':\' or \'-\' characters.')
            # logger.critical('Splitting identifier is not possible.')
            # logger.critical('Identifier: %s', identifier)
            print('Identifier does not contain \':\' or \'-\' characters.')
            print('Splitting identifier is not possible.')
            print('Identifier: %s' % (identifier))
        prefix = identifier_processed = separator = None

    return prefix, identifier_processed, separator
