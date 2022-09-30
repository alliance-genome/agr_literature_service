import warnings
from os import environ
import collections
import functools
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


class memoized(object):
    """Decorator. Caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned
   (not reevaluated).
   """

    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            # uncacheable. a list, for instance.
            # better to not cache than blow up.
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        else:
            value = self.func(*args)
            self.cache[args] = value
            return value

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)
