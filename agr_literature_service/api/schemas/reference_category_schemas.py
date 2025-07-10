from enum import Enum


class ReferenceCategory(str, Enum):
    Research_Article = "research_article"
    Review_Article = "review_article"
    Thesis = "thesis"
    Book = "book"
    Other = "other"
    Preprint = "preprint"
    Conference_Publication = "conference_publication"
    Personal_Communication = "personal_communication"
    Direct_Data_Submission = "direct_data_submission"
    Internal_Process_Reference = "internal_process_reference"
    Unknown = "unknown"
    Comment = "comment"
    Retraction = "retraction"
    Obsolete = "obsolete"
    Correction = "correction"
