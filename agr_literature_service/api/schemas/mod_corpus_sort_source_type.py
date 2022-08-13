from enum import Enum


class ModCorpusSortSourceType(str, Enum):
    Mod_pubmed_search = "mod_pubmed_search"
    Dqm_files = "dqm_files"
    Manual_creation = "manual_creation"
    Automated_alliance = "automated_alliance"
    Assigned_for_review = "assigned_for_review"
