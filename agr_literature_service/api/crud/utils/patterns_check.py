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
from typing import Dict

logger = logging.getLogger(__name__)

patterns: Dict = {}

logger = logging.getLogger(__name__)


def get_patterns():
    global patterns
    filenames = ['reference', 'resource']
    if not patterns:
        try:
            for filename in filenames:
                yml_ret = yaml.load(open(f'{path.dirname(__file__)}/yml/{filename}.yml'), Loader=yaml.FullLoader)
                patterns[filename] = {}
                for key in yml_ret:
                    for pattern in yml_ret[key]['pattern']:
                        patterns[filename][pattern[1]] = pattern[0]
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Error convert {filename} yml file: {e}")
    return patterns


def check_pattern(key: str, curie: str):
    """
    key: type of pattern, currently 'reference' or 'resource'
    curie_prefix: type od cross reference i.e. 'DOI', 'MGI', 'ISBN'
    curie: the actual curie without the curie_prefix
    """
    global patterns
    if not patterns:
        get_patterns()
    if key not in patterns:
        logger.error(f"Unable to find {key} in pattern list")
        return None
    curie_prefix = curie.split(':')[0]
    if curie_prefix not in patterns[key]:
        logger.error(f"Unable to find curie prefix {curie_prefix} in pattern list for {key}")
        return None

    if re.match(patterns[key][curie_prefix], curie):
        return True
    return False
