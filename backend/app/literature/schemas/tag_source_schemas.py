from enum import Enum

class TagSource(str, Enum):
    SGD = "SGD"
    ZFIN = "ZFIN"
    RGD = "RGD"
    WB = "WB"
    MGI = "MGI"
    FB = "FB"
