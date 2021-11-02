from enum import Enum


class ReferenceCommentAndCorrectionType(str, Enum):
    RetractionOf = "RetractionOf"
    CommentOn = "CommentOn"
    ErratumFor = "ErratumFor"
    ReprintOf = "ReprintOf"
    UpdateOf = "UpdateOf"
    ExpressionOfConcernFor = "ExpressionOfConcernFor"
    RepublishedFrom = "RepublishedFrom"
