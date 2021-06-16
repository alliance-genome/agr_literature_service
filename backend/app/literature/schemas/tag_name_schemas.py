from enum import Enum

class TagName(str, Enum):
    can_show_images = "can_show_images"
    pmc_open_access = "pms_open_access"
    in_corpus = "in_corpus"
    not_relevant = "not_relevant"
