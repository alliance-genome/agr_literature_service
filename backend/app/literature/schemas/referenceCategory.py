from enum import Enum

class ReferenceCategory(str, Enum):
    Research_Article = "Research Article"
    Review_Article = "Review Article"
    Thesis = "Thesis"
    Book = "Book"
    Other = "Other"
    Preprint = "Preprint"
    Conference_Publication = "Conference Publication"
    Personal_Communication = "Personal Communication"
    Direct_Data_Submission = "Direct Data Submission"
    Internal_Process_Reference = "Internal Process Reference"
    Unknown = "Unknown"
    Retraction = "Retraction"
