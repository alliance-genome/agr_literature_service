from enum import Enum


class PubMedPublicationStatus(str, Enum):
    ppublish = "ppublish"
    epublish = "epublish"
    aheadofprint = "aheadofprint"
