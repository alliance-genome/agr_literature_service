from enum import Enum

class ReferenceCategory(str, Enum):
    Research_Article = "ResearchArticle"
    Review_Article = "ReviewArticle"
    Thesis = "Thesis"
    Book = "Book"
    Other = "Other"
    Preprint = "Preprint"
    Conference_Publication = "ConferencePublication"
    Personal_Communication = "PersonalCommunication"
    Direct_Data_Submission = "DirectDataSubmission"
    Internal_Process_Reference = "InternalProcessReference"
    Unknown = "Unknown"
    Retraction = "Retraction"
