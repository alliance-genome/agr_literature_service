from enum import Enum

class TagName(str, Enum):
    canShowImages = "canShowImages"
    PMCOpenAccess = "PMCOpenAccess"
    inCorpus = "inCorpus"
    notRelevant = "notRelevant"
