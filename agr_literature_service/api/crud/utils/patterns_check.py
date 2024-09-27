"""
patterns_check.py
=================
Code to read from yml files and create regular expressions to be used for validation.
To add new ones.
NOTE: yml file name must match the key for the dictionary.
      i.e. reference.yml is used to add a new key 'reference' to the patterns and patterns_prefixed.
"""
import logging
import re
from os import path
from typing import Dict, Union

import yaml

logger = logging.getLogger(__name__)

patterns: Dict[str, Dict[str, str]] = {}


def get_patterns() -> Dict[str, Dict[str, str]]:
    global patterns
    filenames = ['reference', 'resource']
    if not patterns:
        for filename in filenames:
            file_path = f'{path.dirname(__file__)}/yml/{filename}.yml'
            try:
                with open(file_path, 'r') as f:
                    yml_ret = yaml.load(f, Loader=yaml.FullLoader)
                patterns[filename] = {}
                for key in yml_ret:
                    for pattern in yml_ret[key]['pattern']:
                        patterns[filename][pattern[1]] = pattern[0]
            except Exception as e:
                logger.error(f"Error converting {filename}.yml file: {e}")
                raise RuntimeError(f"Error converting {filename}.yml file: {e}")
    return patterns


def check_pattern(key: str, curie: str) -> Union[bool, None]:
    """
    Validates a CURIE against a predefined pattern.

    Args:
        key (str): Type of pattern, currently 'reference' or 'resource'.
        curie (str): The CURIE to validate.

    Returns:
        bool: True if the CURIE matches the pattern, False otherwise.
    """
    global patterns
    if not patterns:
        get_patterns()
    if key not in patterns:
        logger.error(f"Unable to find '{key}' in pattern list")
        return None
    curie_prefix = curie.split(':')[0]
    if curie_prefix not in patterns[key]:
        logger.error(f"Unable to find CURIE prefix '{curie_prefix}' in pattern list for '{key}'")
        return None

    pattern = patterns[key][curie_prefix]
    if re.match(pattern, curie):
        return True
    else:
        logger.error(f"CURIE '{curie}' does not match the pattern for '{curie_prefix}'")
        return False
