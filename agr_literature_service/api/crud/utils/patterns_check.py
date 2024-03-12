"""
patterns_check.py
=================
Code to read from yml files and create regular expressions to be used for validation.
To add new ones.
NOTE: yml file name must match the key for the dictionary.
      i.e. reference.yml is used to add a new key 'reference' to the patterns and patterns_prefixed.
"""
import logging
import yaml
import re
from os import path
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)

patterns = {}
patterns_prefixed = {}

logger = logging.getLogger(__name__)


def get_patterns(with_prefix=False):
    global patterns_prefixed
    global patterns
    filenames = ['reference', 'resource']
    if not patterns:
        try:
            for filename in filenames:
                yml_ret = yaml.load(open(f'{path.dirname(__file__)}/yml/{filename}.yml'), Loader=yaml.FullLoader)
                patterns[filename] = {}
                patterns_prefixed[filename] = {}
                for key in yml_ret:
                    patterns[filename][key] = yml_ret[key]['pattern']
                    patterns_prefixed[filename][key] = patterns[filename][key].replace('^', f'^{key}:')
                    patterns[filename][key] = yml_ret[key]['pattern']
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Error convert {filename} yml file: {e}")
    if with_prefix:
        return patterns_prefixed
    return patterns


def check_pattern(key: str, species: str, curie: str, prefix: bool = None):
    global patterns_prefixed
    global patterns
    if not patterns:
        get_patterns()
    # presume if species or key is in patterns it will also be in patterns_prefixed as they are built together
    if key not in patterns:
        logger.error(f"Unable to find {key} in pattern list")
        return None
    if species not in patterns[key]:
        logger.error(f"Unable to find species abbreviation {species} in pattern list")
        return None
    if prefix is None:  # prefix not defined so try both dictionarys
        if re.match(patterns[key][species], curie):
            return True
        if re.match(patterns_prefixed[key][species], curie):
            return True
        return False
    if prefix:
        if re.match(patterns_prefixed[key][species], curie):
            return True
    else:
        if re.match(patterns[key][species], curie):
            return True
    return False
