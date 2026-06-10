from enum import Enum


class PersonPersonRole(str, Enum):
    phd = "phd"
    postdoc = "postdoc"
    masters = "masters"
    undergrad = "undergrad"
    highschool = "highschool"
    sabbatical = "sabbatical"
    lab_visitor = "lab_visitor"
    research_staff = "research_staff"
    assistant_professor = "assistant_professor"
    unknown = "unknown"
    collaborated = "collaborated"
