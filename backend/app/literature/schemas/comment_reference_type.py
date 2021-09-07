from enum import Enum

class CommentReferenceType(str, Enum):
    RetractionOf = "RetractionOf"
    CommentOn = "CommentOn"
    ReprintIn = "ReprintIn"
    ErratumFor = "ErratumFor"
    ReprintOf = "ReprintOf"
    ExpressionOfConcernIn = "ExpressionOfConcernIn"
    UpdateOf = "UpdateOf"
    CommentIn = "CommentIn"
    RetractionIn = "RetractionIn"
    RepublishedIn = "RepublishedIn"
    UpdateIn = "UpdateIn"
    AssociatedDataset = "AssociatedDataset"
    ErratumIn = "ErratumIn"
    ExpressionOfConcernFor = "ExpressionOfConcernFor"
    RepublishedFrom = "RepublishedFrom"
